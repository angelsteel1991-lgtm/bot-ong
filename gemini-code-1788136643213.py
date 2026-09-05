import os
import time
import math
import hmac
import hashlib
import requests
import pandas as pd
import websocket
import json
import threading
import uuid

from urllib.parse import urlencode
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler


# ============================================================
# ONGUSDT BREAKOUT BOT
#
# Estrategia:
#   - Breakout estructural Donchian
#   - Lookback 20 velas
#   - ATR 14
#   - Filtro de volatilidad 0.60 ATR
#   - Stop = 3 ATR para ONG
#   - TP1 = 1.5R
#   - TP2 = 2.0R
#   - TP3 = 3.0R
#   - TP1 40%
#   - TP2 30%
#   - TP3 30%
#
# CONEXION:
#   Se mantiene la conexion del bot ONG que ya funciona:
#   Market WebSocket
#   WS API User Data
#   Private User Data Stream
# ============================================================


# ============================================================
# CONFIGURACION
# ============================================================

SYMBOL = "ONGUSDT"

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

USE_TESTNET = os.getenv(
    "USE_TESTNET",
    "true"
).lower() == "true"


if USE_TESTNET:

    BASE_URL = "https://testnet.binancefuture.com"

    MARKET_WS = (
        "wss://stream.binancefuture.com/"
        "ws/ongusdt@kline_1m"
    )

    WS_API_URL = (
        "wss://testnet.binancefuture.com/"
        "ws-fapi/v1"
    )

else:

    BASE_URL = "https://fapi.binance.com"

    MARKET_WS = (
        "wss://fstream.binance.com/"
        "market/ws/ongusdt@kline_1m"
    )

    WS_API_URL = (
        "wss://ws-fapi.binance.com/"
        "ws-fapi/v1"
    )


# ============================================================
# TRADING
# ============================================================

LIVE_TRADING = os.getenv(
    "LIVE_TRADING",
    "false"
).lower() == "true"

LEVERAGE = 6

MARGIN_TYPE = "ISOLATED"

MARGIN_PER_TRADE_USDT = 2.5

MARGIN_MIN_USDT = 1.0

MARGIN_MAX_USDT = 4.0


# ============================================================
# ESTRATEGIA BREAKOUT
# ============================================================

LOOKBACK = 20

ATR_PERIOD = 14

ATR_FILTER = 0.60

ATR_STOP_MULT = 3.0


# ============================================================
# TARGETS POR R
# ============================================================

TP1_R = 1.5

TP2_R = 2.0

TP3_R = 3.0


TP1_PERCENT = 0.40

TP2_PERCENT = 0.30

TP3_PERCENT = 0.30


# ============================================================
# CONTROL
# ============================================================

COOLDOWN_SECONDS = 60

RECONNECT_SECONDS = 60


# ============================================================
# VARIABLES
# ============================================================

candles = []

current_price = None

current_atr = None

channel_high = None

channel_low = None


position_side = None

position_qty = 0.0

entry_price = 0.0

initial_qty = 0.0

remaining_qty = 0.0

risk_distance = 0.0

stop_price = 0.0

tp1_price = 0.0

tp2_price = 0.0

tp3_price = 0.0


tp1_done = False

tp2_done = False

tp3_done = False


last_trade_time = 0


highest_price = 0.0

lowest_price = 0.0


rest_pause_until = 0


state_lock = threading.Lock()


user_stream_control = None

user_stream_control_lock = threading.Lock()

listen_key = None


QTY_STEP = 1.0

MIN_QTY = 1.0

MIN_NOTIONAL = 5.0


# ============================================================
# LOG
# ============================================================

def log(msg):

    now = datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    print(
        f"[{now}] {msg}",
        flush=True
    )


# ============================================================
# IP PUBLICA
# ============================================================

def log_public_ip():

    try:

        response = requests.get(
            "https://api.ipify.org?format=json",
            timeout=10
        )

        ip = response.json().get(
            "ip",
            "desconocida"
        )

        log(
            f"IP PUBLICA DE SALIDA: {ip}"
        )

    except Exception as e:

        log(
            f"No se pudo obtener IP publica: {e}"
        )


# ============================================================
# REST CONTROL
# ============================================================

def rest_allowed():

    return time.time() >= rest_pause_until


def pause_rest(seconds):

    global rest_pause_until

    until = time.time() + seconds

    if until > rest_pause_until:

        rest_pause_until = until

    log(
        f"REST pausado durante {seconds}s"
    )


# ============================================================
# REST FIRMADO
# ============================================================

def signed_request(
    method,
    endpoint,
    params=None
):

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan BINANCE_API_KEY o BINANCE_API_SECRET"
        )

    if not rest_allowed():

        raise Exception(
            "REST temporalmente pausado"
        )

    if params is None:

        params = {}

    params = dict(params)

    params["timestamp"] = int(
        time.time() * 1000
    )

    params["recvWindow"] = 5000

    query = urlencode(params)

    signature = hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()

    query += (
        "&signature="
        + signature
    )

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    url = BASE_URL + endpoint

    try:

        if method == "GET":

            response = requests.get(
                url,
                headers=headers,
                params=query,
                timeout=10
            )

        elif method == "POST":

            response = requests.post(
                url,
                headers=headers,
                params=query,
                timeout=10
            )

        elif method == "DELETE":

            response = requests.delete(
                url,
                headers=headers,
                params=query,
                timeout=10
            )

        else:

            raise Exception(
                "Metodo HTTP no soportado"
            )

    except requests.RequestException as e:

        raise Exception(
            f"Error conexion REST: {e}"
        )

    if response.status_code in (
        418,
        429
    ):

        retry_after = response.headers.get(
            "Retry-After"
        )

        try:

            wait = int(
                retry_after
            )

        except:

            wait = 120

        log(
            f"BINANCE {response.status_code}. "
            f"NO se vuelve a insistir. "
            f"Esperando {wait}s."
        )

        pause_rest(wait)

        raise Exception(
            f"Binance rate limit "
            f"{response.status_code}"
        )

    if response.status_code >= 400:

        raise Exception(
            f"Binance HTTP "
            f"{response.status_code}: "
            f"{response.text}"
        )

    return response.json()


# ============================================================
# REST PUBLICO
# ============================================================

def public_request(
    endpoint,
    params=None
):

    url = BASE_URL + endpoint

    response = requests.get(
        url,
        params=params,
        timeout=10
    )

    if response.status_code in (
        418,
        429
    ):

        raise Exception(
            f"Binance rate limit "
            f"{response.status_code}"
        )

    if response.status_code >= 400:

        raise Exception(
            f"Binance HTTP "
            f"{response.status_code}: "
            f"{response.text}"
        )

    return response.json()


# ============================================================
# REGLAS DEL SIMBOLO
# ============================================================

def load_symbol_rules():

    global QTY_STEP
    global MIN_QTY
    global MIN_NOTIONAL

    try:

        data = public_request(
            "/fapi/v1/exchangeInfo"
        )

        for s in data.get(
            "symbols",
            []
        ):

            if s.get(
                "symbol"
            ) != SYMBOL:

                continue

            for f in s.get(
                "filters",
                []
            ):

                if f.get(
                    "filterType"
                ) == "LOT_SIZE":

                    QTY_STEP = float(
                        f.get(
                            "stepSize",
                            QTY_STEP
                        )
                    )

                    MIN_QTY = float(
                        f.get(
                            "minQty",
                            MIN_QTY
                        )
                    )

                if f.get(
                    "filterType"
                ) == "MIN_NOTIONAL":

                    MIN_NOTIONAL = float(
                        f.get(
                            "notional",
                            MIN_NOTIONAL
                        )
                    )

            log(
                f"Reglas {SYMBOL} | "
                f"step={QTY_STEP} | "
                f"min_qty={MIN_QTY} | "
                f"min_notional={MIN_NOTIONAL}"
            )

            return True

        log(
            f"No se encontraron reglas para {SYMBOL}"
        )

        return False

    except Exception as e:

        log(
            f"Error exchangeInfo: {e}"
        )

        return False


# ============================================================
# REDONDEO CANTIDAD
# ============================================================

def round_quantity(qty):

    if QTY_STEP <= 0:

        return qty

    result = (
        math.floor(
            qty / QTY_STEP
        )
        * QTY_STEP
    )

    decimals = 8

    if QTY_STEP >= 1:

        decimals = 0

    elif QTY_STEP >= 0.1:

        decimals = 1

    elif QTY_STEP >= 0.01:

        decimals = 2

    elif QTY_STEP >= 0.001:

        decimals = 3

    elif QTY_STEP >= 0.0001:

        decimals = 4

    return round(
        result,
        decimals
    )


# ============================================================
# REDONDEO PRECIO
# ============================================================

def round_price(price):

    try:

        data = public_request(
            "/fapi/v1/exchangeInfo"
        )

        for s in data.get(
            "symbols",
            []
        ):

            if s.get(
                "symbol"
            ) != SYMBOL:

                continue

            for f in s.get(
                "filters",
                []
            ):

                if f.get(
                    "filterType"
                ) == "PRICE_FILTER":

                    tick = float(
                        f.get(
                            "tickSize",
                            0.0001
                        )
                    )

                    if tick > 0:

                        result = (
                            math.floor(
                                price / tick
                            )
                            * tick
                        )

                        return round(
                            result,
                            8
                        )

    except Exception:

        pass

    return round(
        price,
        8
    )


# ============================================================
# BALANCE
# ============================================================

def get_usdt_balance():

    data = signed_request(
        "GET",
        "/fapi/v2/balance"
    )

    for item in data:

        if item.get(
            "asset"
        ) == "USDT":

            return float(
                item.get(
                    "availableBalance",
                    0
                )
            )

    return 0.0


# ============================================================
# LEVERAGE
# ============================================================

def set_leverage():

    try:

        result = signed_request(
            "POST",
            "/fapi/v1/leverage",
            {
                "symbol": SYMBOL,
                "leverage": LEVERAGE
            }
        )

        log(
            f"Leverage configurado: "
            f"{result.get('leverage', LEVERAGE)}x"
        )

        return True

    except Exception as e:

        log(
            f"No se pudo configurar leverage: {e}"
        )

        return False


# ============================================================
# MARGIN TYPE
# ============================================================

def set_margin_type():

    try:

        signed_request(
            "POST",
            "/fapi/v1/marginType",
            {
                "symbol": SYMBOL,
                "marginType": MARGIN_TYPE
            }
        )

        log(
            f"Margin type configurado: "
            f"{MARGIN_TYPE}"
        )

    except Exception as e:

        log(
            f"Margin type "
            f"(puede que ya estuviera configurado): "
            f"{e}"
        )


# ============================================================
# ATR
# ============================================================

def calculate_atr(
    df,
    period=ATR_PERIOD
):

    if len(df) < period + 1:

        return None

    high = df["high"]

    low = df["low"]

    close = df["close"]

    prev_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs()
        ],
        axis=1
    ).max(
        axis=1
    )

    atr = tr.rolling(
        period
    ).mean()

    value = atr.iloc[-1]

    if pd.isna(value):

        return None

    return float(value)


# ============================================================
# BREAKOUT
# ============================================================

def calculate_breakout_signal(
    df
):

    global current_atr
    global channel_high
    global channel_low

    minimum = max(
        LOOKBACK + 2,
        ATR_PERIOD + 2
    )

    if len(df) < minimum:

        return None

    current_atr = calculate_atr(
        df
    )

    if current_atr is None:

        return None

    # Canal formado EXCLUSIVAMENTE
    # por las velas anteriores.
    #
    # La vela actual no entra en el canal.

    channel = df.iloc[
        -(LOOKBACK + 1):-1
    ]

    channel_high = float(
        channel["high"].max()
    )

    channel_low = float(
        channel["low"].min()
    )

    channel_width = (
        channel_high
        - channel_low
    )

    # Filtro de volatilidad
    if channel_width < (
        current_atr * ATR_FILTER
    ):

        return None

    close = float(
        df.iloc[-1]["close"]
    )

    if close > channel_high:

        log(
            f"BREAKOUT BUY | "
            f"close={close:.8f} | "
            f"resistencia={channel_high:.8f} | "
            f"ATR={current_atr:.8f} | "
            f"channel={channel_width:.8f}"
        )

        return "LONG"

    if close < channel_low:

        log(
            f"BREAKOUT SELL | "
            f"close={close:.8f} | "
            f"soporte={channel_low:.8f} | "
            f"ATR={current_atr:.8f} | "
            f"channel={channel_width:.8f}"
        )

        return "SHORT"

    return None


# ============================================================
# CANTIDAD
# ============================================================

def calculate_quantity(
    price
):

    balance = get_usdt_balance()

    if balance <= 0:

        raise Exception(
            "No hay balance USDT disponible"
        )

    margin = MARGIN_PER_TRADE_USDT

    if current_atr and price > 0:

        atr_pct = (
            current_atr / price
        )

        # Si hay mucha volatilidad,
        # reducimos margen.
        #
        # Si hay poca volatilidad,
        # permitimos hasta 4 USDT.

        if atr_pct > 0.015:

            margin = MARGIN_MIN_USDT

        elif atr_pct > 0.010:

            margin = 1.5

        elif atr_pct > 0.005:

            margin = 2.0

        else:

            margin = MARGIN_PER_TRADE_USDT

    margin = max(
        MARGIN_MIN_USDT,
        min(
            MARGIN_MAX_USDT,
            margin
        )
    )

    margin = min(
        margin,
        balance
    )

    notional = (
        margin * LEVERAGE
    )

    if notional < MIN_NOTIONAL:

        raise Exception(
            f"Notional {notional:.2f} USDT "
            f"< minimo {MIN_NOTIONAL}"
        )

    quantity = (
        notional / price
    )

    quantity = round_quantity(
        quantity
    )

    if quantity < MIN_QTY:

        quantity = MIN_QTY

    return float(
        f"{quantity:.8f}"
    )


# ============================================================
# ORDEN MARKET
# ============================================================

def market_order(
    side,
    quantity,
    reduce_only=False
):

    quantity = round_quantity(
        quantity
    )

    if quantity <= 0:

        raise Exception(
            "Cantidad <= 0"
        )

    params = {
        "symbol": SYMBOL,
        "side": side,
        "type": "MARKET",
        "quantity": quantity
    }

    if reduce_only:

        params[
            "reduceOnly"
        ] = "true"

    return signed_request(
        "POST",
        "/fapi/v1/order",
        params
    )


# ============================================================
# OBTENER POSICION REAL DE BINANCE
# ============================================================

def get_real_position():

    data = signed_request(
        "GET",
        "/fapi/v2/positionRisk",
        {
            "symbol": SYMBOL
        }
    )

    if not data:

        return None

    for p in data:

        if p.get(
            "symbol"
        ) != SYMBOL:

            continue

        amount = float(
            p.get(
                "positionAmt",
                0
            )
        )

        entry = float(
            p.get(
                "entryPrice",
                0
            )
        )

        if abs(amount) <= 0:

            continue

        if amount > 0:

            side = "LONG"

        else:

            side = "SHORT"

        return {
            "side": side,
            "qty": abs(amount),
            "entry": entry
        }

    return None


# ============================================================
# ABRIR POSICION
# ============================================================

def open_position(
    side
):

    global position_side
    global position_qty
    global entry_price
    global initial_qty
    global remaining_qty
    global risk_distance
    global stop_price
    global tp1_price
    global tp2_price
    global tp3_price
    global tp1_done
    global tp2_done
    global tp3_done
    global last_trade_time
    global highest_price
    global lowest_price

    now = time.time()

    if (
        now - last_trade_time
        < COOLDOWN_SECONDS
    ):

        log(
            "Cooldown activo"
        )

        return

    with state_lock:

        if position_side is not None:

            log(
                f"Ya existe posicion "
                f"{position_side}"
            )

            return

    if current_price is None:

        return

    if current_atr is None:

        log(
            "No hay ATR disponible"
        )

        return

    price = current_price

    if side == "LONG":

        order_side = "BUY"

        preliminary_stop = (
            price
            - current_atr * ATR_STOP_MULT
        )

    else:

        order_side = "SELL"

        preliminary_stop = (
            price
            + current_atr * ATR_STOP_MULT
        )

    preliminary_risk = abs(
        price
        - preliminary_stop
    )

    quantity = calculate_quantity(
        price
    )

    log(
        f"BREAKOUT {side} | "
        f"entry={price:.8f} | "
        f"ATR={current_atr:.8f} | "
        f"SL preliminar={preliminary_stop:.8f} | "
        f"R={preliminary_risk:.8f} | "
        f"qty={quantity}"
    )

    if not LIVE_TRADING:

        log(
            "LIVE_TRADING=False -> "
            "NO se envia orden"
        )

        return

    # ========================================================
    # ENVIAR ENTRADA
    # ========================================================

    try:

        order = market_order(
            order_side,
            quantity
        )

    except Exception as e:

        log(
            f"ERROR ABRIENDO POSICION: {e}"
        )

        return

    # ========================================================
    # MUY IMPORTANTE:
    #
    # NO confiamos solamente en executedQty.
    # Consultamos la posicion REAL.
    # ========================================================

    time.sleep(
        0.5
    )

    try:

        real_position = get_real_position()

    except Exception as e:

        log(
            f"ERROR verificando posicion real: {e}"
        )

        return

    if not real_position:

        log(
            "ATENCION: Binance no muestra "
            "ninguna posicion abierta."
        )

        log(
            "NO se crea estado local."
        )

        return

    real_qty = float(
        real_position["qty"]
    )

    real_entry = float(
        real_position["entry"]
    )

    real_side = real_position[
        "side"
    ]

    # ========================================================
    # SEGURIDAD CONTRA QTY 0
    # ========================================================

    if real_qty <= 0:

        log(
            "ERROR: Binance devolvio "
            "qty <= 0. No se crea posicion."
        )

        return

    if real_entry <= 0:

        real_entry = price

    # ========================================================
    # CALCULAR STOP REAL
    # ========================================================

    atr = current_atr

    if real_side == "LONG":

        real_stop = (
            real_entry
            - atr * ATR_STOP_MULT
        )

        real_risk = (
            real_entry
            - real_stop
        )

        real_tp1 = (
            real_entry
            + real_risk * TP1_R
        )

        real_tp2 = (
            real_entry
            + real_risk * TP2_R
        )

        real_tp3 = (
            real_entry
            + real_risk * TP3_R
        )

        highest_price = real_entry

        lowest_price = real_entry

    else:

        real_stop = (
            real_entry
            + atr * ATR_STOP_MULT
        )

        real_risk = (
            real_stop
            - real_entry
        )

        real_tp1 = (
            real_entry
            - real_risk * TP1_R
        )

        real_tp2 = (
            real_entry
            - real_risk * TP2_R
        )

        real_tp3 = (
            real_entry
            - real_risk * TP3_R
        )

        highest_price = real_entry

        lowest_price = real_entry

    # ========================================================
    # CREAR ESTADO
    # ========================================================

    with state_lock:

        position_side = real_side

        position_qty = real_qty

        initial_qty = real_qty

        remaining_qty = real_qty

        entry_price = real_entry

        risk_distance = real_risk

        stop_price = real_stop

        tp1_price = real_tp1

        tp2_price = real_tp2

        tp3_price = real_tp3

        tp1_done = False

        tp2_done = False

        tp3_done = False

    last_trade_time = time.time()

    log(
        f"POSICION REAL CONFIRMADA | "
        f"{real_side} | "
        f"qty={real_qty} | "
        f"entry={real_entry:.8f}"
    )

    log(
        f"NIVELES | "
        f"SL={real_stop:.8f} | "
        f"TP1={real_tp1:.8f} | "
        f"TP2={real_tp2:.8f} | "
        f"TP3={real_tp3:.8f}"
    )


# ============================================================
# CERRAR CANTIDAD PARCIAL
# ============================================================

def close_partial(
    percent,
    reason
):

    global remaining_qty
    global position_qty

    with state_lock:

        side = position_side

        qty_available = remaining_qty

    if side is None:

        return False

    if qty_available <= 0:

        return False

    quantity = (
        qty_available * percent
    )

    quantity = round_quantity(
        quantity
    )

    if quantity <= 0:

        log(
            f"{reason}: cantidad redondeada <= 0"
        )

        return False

    # Evitar intentar cerrar mas
    # de lo que queda.

    if quantity > qty_available:

        quantity = round_quantity(
            qty_available
        )

    close_side = (
        "SELL"
        if side == "LONG"
        else "BUY"
    )

    log(
        f"{reason} | "
        f"cerrando qty={quantity}"
    )

    if not LIVE_TRADING:

        return False

    try:

        result = market_order(
            close_side,
            quantity,
            reduce_only=True
        )

        time.sleep(
            0.3
        )

        real_position = get_real_position()

        if real_position:

            new_qty = float(
                real_position["qty"]
            )

            with state_lock:

                remaining_qty = new_qty

                position_qty = new_qty

        else:

            with state_lock:

                remaining_qty = 0.0

                position_qty = 0.0

        log(
            f"{reason} ejecutado | "
            f"restante={remaining_qty}"
        )

        return True

    except Exception as e:

        log(
            f"ERROR {reason}: {e}"
        )

        return False


# ============================================================
# CERRAR TODA LA POSICION
# ============================================================

def close_position(
    reason
):

    global position_side
    global position_qty
    global entry_price
    global initial_qty
    global remaining_qty
    global risk_distance
    global stop_price
    global tp1_price
    global tp2_price
    global tp3_price
    global tp1_done
    global tp2_done
    global tp3_done
    global highest_price
    global lowest_price
    global last_trade_time

    with state_lock:

        side = position_side

        qty = remaining_qty

    if side is None:

        return

    if qty <= 0:

        with state_lock:

            position_side = None

        return

    close_side = (
        "SELL"
        if side == "LONG"
        else "BUY"
    )

    log(
        f"CERRANDO POSICION | "
        f"{side} | "
        f"qty={qty} | "
        f"motivo={reason}"
    )

    if not LIVE_TRADING:

        return

    try:

        quantity = round_quantity(
            qty
        )

        if quantity <= 0:

            return

        market_order(
            close_side,
            quantity,
            reduce_only=True
        )

        time.sleep(
            0.5
        )

        real_position = get_real_position()

        if real_position:

            real_qty = float(
                real_position["qty"]
            )

            if real_qty > 0:

                log(
                    f"ATENCION: quedaron "
                    f"{real_qty} unidades abiertas"
                )

                with state_lock:

                    position_qty = real_qty

                    remaining_qty = real_qty

                return

        with state_lock:

            position_side = None

            position_qty = 0.0

            entry_price = 0.0

            initial_qty = 0.0

            remaining_qty = 0.0

            risk_distance = 0.0

            stop_price = 0.0

            tp1_price = 0.0

            tp2_price = 0.0

            tp3_price = 0.0

            tp1_done = False

            tp2_done = False

            tp3_done = False

            highest_price = 0.0

            lowest_price = 0.0

        last_trade_time = time.time()

        log(
            f"POSICION CERRADA | {reason}"
        )

    except Exception as e:

        log(
            f"ERROR CERRANDO POSICION: {e}"
        )


# ============================================================
# PROCESAR TP
# ============================================================

def manage_profit_targets():

    global tp1_done
    global tp2_done
    global tp3_done
    global stop_price

    with state_lock:

        side = position_side

        entry = entry_price

        tp1 = tp1_price

        tp2 = tp2_price

        tp3 = tp3_price

        current_stop = stop_price

    if side is None:

        return

    price = current_price

    if price is None:

        return

    # ========================================================
    # LONG
    # ========================================================

    if side == "LONG":

        # ----------------------------------------------------
        # TP1
        # ----------------------------------------------------

        if (
            not tp1_done
            and price >= tp1
        ):

            log(
                f"TP1 ALCANZADO | "
                f"price={price:.8f} | "
                f"TP1={tp1:.8f}"
            )

            success = close_partial(
                TP1_PERCENT,
                "TP1"
            )

            if success:

                tp1_done = True

                # Stop a BE
                stop_price = entry

                log(
                    f"TP1 -> "
                    f"STOP BREAK EVEN "
                    f"{entry:.8f}"
                )

            # MUY IMPORTANTE:
            # no procesamos TP2 en el mismo tick.

            return

        # ----------------------------------------------------
        # TP2
        # ----------------------------------------------------

        if (
            tp1_done
            and not tp2_done
            and price >= tp2
        ):

            log(
                f"TP2 ALCANZADO | "
                f"price={price:.8f} | "
                f"TP2={tp2:.8f}"
            )

            success = close_partial(
                TP2_PERCENT,
                "TP2"
            )

            if success:

                tp2_done = True

                # Proteccion en TP1
                stop_price = tp1

                log(
                    f"TP2 -> "
                    f"STOP PROTEGIDO EN TP1 "
                    f"{tp1:.8f}"
                )

            return

        # ----------------------------------------------------
        # TP3
        # ----------------------------------------------------

        if (
            tp2_done
            and not tp3_done
            and price >= tp3
        ):

            log(
                f"TP3 ALCANZADO | "
                f"price={price:.8f} | "
                f"TP3={tp3:.8f}"
            )

            tp3_done = True

            close_position(
                "TP3 - 3R"
            )

            return

    # ========================================================
    # SHORT
    # ========================================================

    else:

        # ----------------------------------------------------
        # TP1
        # ----------------------------------------------------

        if (
            not tp1_done
            and price <= tp1
        ):

            log(
                f"TP1 ALCANZADO | "
                f"price={price:.8f} | "
                f"TP1={tp1:.8f}"
            )

            success = close_partial(
                TP1_PERCENT,
                "TP1"
            )

            if success:

                tp1_done = True

                stop_price = entry

                log(
                    f"TP1 -> "
                    f"STOP BREAK EVEN "
                    f"{entry:.8f}"
                )

            return

        # ----------------------------------------------------
        # TP2
        # ----------------------------------------------------

        if (
            tp1_done
            and not tp2_done
            and price <= tp2
        ):

            log(
                f"TP2 ALCANZADO | "
                f"price={price:.8f} | "
                f"TP2={tp2:.8f}"
            )

            success = close_partial(
                TP2_PERCENT,
                "TP2"
            )

            if success:

                tp2_done = True

                stop_price = tp1

                log(
                    f"TP2 -> "
                    f"STOP PROTEGIDO EN TP1 "
                    f"{tp1:.8f}"
                )

            return

        # ----------------------------------------------------
        # TP3
        # ----------------------------------------------------

        if (
            tp2_done
            and not tp3_done
            and price <= tp3
        ):

            log(
                f"TP3 ALCANZADO | "
                f"price={price:.8f} | "
                f"TP3={tp3:.8f}"
            )

            tp3_done = True

            close_position(
                "TP3 - 3R"
            )

            return


# ============================================================
# STOP LOGICO
#
# Este control queda activo por WebSocket.
# ============================================================

def manage_stop():

    with state_lock:

        side = position_side

        stop = stop_price

        entry = entry_price

    if side is None:

        return

    price = current_price

    if price is None:

        return

    if stop <= 0:

        return

    if side == "LONG":

        if price <= stop:

            close_position(
                "STOP LOSS"
            )

    else:

        if price >= stop:

            close_position(
                "STOP LOSS"
            )


# ============================================================
# GESTION COMPLETA DE POSICION
# ============================================================

def manage_position():

    with state_lock:

        side = position_side

    if side is None:

        return

    # Primero stop
    manage_stop()

    with state_lock:

        if position_side is None:

            return

    # Después targets
    manage_profit_targets()


# ============================================================
# PROCESAR VELA CERRADA
# ============================================================

def process_candle():

    if len(candles) < (
        LOOKBACK + ATR_PERIOD + 2
    ):

        return

    df = pd.DataFrame(
        candles,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume"
        ]
    )

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]:

        df[col] = pd.to_numeric(
            df[col]
        )

    signal = calculate_breakout_signal(
        df
    )

    if signal is None:

        return

    with state_lock:

        existing = position_side

    if existing is None:

        open_position(
            signal
        )

        return

    # No damos vuelta inmediatamente
    # a una posicion existente.
    #
    # El bot mantiene la operacion
    # hasta que llegue su gestion de R.

    if existing != signal:

        log(
            f"BREAKOUT CONTRARIO {signal} "
            f"detectado mientras hay "
            f"{existing}. Se mantiene posicion."
        )


# ============================================================
# MARKET WEBSOCKET
# ============================================================

def on_market_message(
    ws,
    message
):

    global current_price

    try:

        data = json.loads(
            message
        )

        # Soporta raw y combined
        data = data.get(
            "data",
            data
        )

        kline = data.get(
            "k"
        )

        if not kline:

            return

        current_price = float(
            kline["c"]
        )

        # ====================================================
        # SOLO VELA CERRADA
        # ====================================================

        if kline["x"]:

            candle = [
                int(kline["t"]),
                float(kline["o"]),
                float(kline["h"]),
                float(kline["l"]),
                float(kline["c"]),
                float(kline["v"])
            ]

            if candles:

                if (
                    candles[-1][0]
                    == candle[0]
                ):

                    candles[-1] = candle

                else:

                    candles.append(
                        candle
                    )

            else:

                candles.append(
                    candle
                )

            if len(candles) > 300:

                del candles[:-300]

            log(
                f"VELA 1m CERRADA | "
                f"close={candle[4]:.8f}"
            )

            process_candle()

    except Exception as e:

        log(
            f"Error Market WS: {e}"
        )


def on_market_error(
    ws,
    error
):

    log(
        f"Market WS error: {error}"
    )


def on_market_close(
    ws,
    code,
    msg
):

    log(
        f"Market WS cerrado: "
        f"{code} {msg}"
    )


def on_market_open(
    ws
):

    log(
        "MARKET WEBSOCKET CONECTADO"
    )


def market_websocket_loop():

    while True:

        try:

            log(
                "Conectando Market WebSocket..."
            )

            ws = websocket.WebSocketApp(
                MARKET_WS,
                on_open=on_market_open,
                on_message=on_market_message,
                on_error=on_market_error,
                on_close=on_market_close
            )

            ws.run_forever(
                ping_interval=60,
                ping_timeout=20
            )

        except Exception as e:

            log(
                f"Market WS exception: {e}"
            )

        log(
            "Reconexión Market WS "
            "en 60 segundos..."
        )

        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# USER DATA STREAM
# ============================================================

def start_user_data_stream():

    global user_stream_control
    global listen_key

    log(
        "Abriendo conexión WS API "
        "para User Data Stream..."
    )

    ws = websocket.create_connection(
        WS_API_URL,
        timeout=15
    )

    request_id = str(
        uuid.uuid4()
    )

    request = {
        "id": request_id,
        "method": "userDataStream.start",
        "params": {
            "apiKey": API_KEY
        }
    }

    ws.send(
        json.dumps(
            request
        )
    )

    response = json.loads(
        ws.recv()
    )

    if response.get(
        "status"
    ) != 200:

        ws.close()

        raise Exception(
            f"UserDataStream.start "
            f"rechazado: {response}"
        )

    key = (
        response
        .get("result", {})
        .get("listenKey")
    )

    if not key:

        ws.close()

        raise Exception(
            "Binance no devolvió listenKey"
        )

    with user_stream_control_lock:

        user_stream_control = ws

    listen_key = key

    log(
        "USER DATA STREAM CREADO "
        "POR WS API"
    )

    log(
        "ListenKey recibido correctamente"
    )

    return listen_key


# ============================================================
# KEEPALIVE
# ============================================================

def user_stream_keepalive_loop():

    global user_stream_control
    global listen_key

    while True:

        time.sleep(
            45 * 60
        )

        try:

            with user_stream_control_lock:

                ws = user_stream_control

            if ws is None:

                log(
                    "Keepalive: "
                    "no hay conexión WS API"
                )

                continue

            request_id = str(
                uuid.uuid4()
            )

            request = {
                "id": request_id,
                "method": "userDataStream.ping",
                "params": {
                    "apiKey": API_KEY
                }
            }

            with user_stream_control_lock:

                ws.send(
                    json.dumps(
                        request
                    )
                )

                ws.settimeout(
                    15
                )

                response = json.loads(
                    ws.recv()
                )

            if response.get(
                "status"
            ) == 200:

                new_key = (
                    response
                    .get("result", {})
                    .get("listenKey")
                )

                if new_key:

                    listen_key = new_key

                log(
                    "USER DATA STREAM "
                    "KEEPALIVE OK"
                )

            else:

                log(
                    f"USER DATA KEEPALIVE: "
                    f"{response}"
                )

        except Exception as e:

            log(
                f"User Data keepalive error: {e}"
            )

            with user_stream_control_lock:

                try:

                    if user_stream_control:

                        user_stream_control.close()

                except:

                    pass

                user_stream_control = None


# ============================================================
# ACCOUNT UPDATE
# ============================================================

def process_account_update(
    data
):

    global position_side
    global position_qty
    global remaining_qty
    global entry_price
    global highest_price
    global lowest_price

    account = data.get(
        "a",
        {}
    )

    positions = account.get(
        "P",
        []
    )

    for p in positions:

        if p.get(
            "s"
        ) != SYMBOL:

            continue

        amount = float(
            p.get(
                "pa",
                0
            )
        )

        entry = float(
            p.get(
                "ep",
                0
            )
        )

        with state_lock:

            if amount > 0:

                position_side = "LONG"

                position_qty = amount

                remaining_qty = amount

                entry_price = entry

                if highest_price == 0:

                    highest_price = entry

                if lowest_price == 0:

                    lowest_price = entry

                log(
                    f"ACCOUNT UPDATE -> "
                    f"LONG qty={amount} "
                    f"entry={entry}"
                )

            elif amount < 0:

                position_side = "SHORT"

                position_qty = abs(
                    amount
                )

                remaining_qty = abs(
                    amount
                )

                entry_price = entry

                if highest_price == 0:

                    highest_price = entry

                if lowest_price == 0:

                    lowest_price = entry

                log(
                    f"ACCOUNT UPDATE -> "
                    f"SHORT qty={abs(amount)} "
                    f"entry={entry}"
                )

            else:

                position_side = None

                position_qty = 0.0

                remaining_qty = 0.0

                entry_price = 0.0

                highest_price = 0.0

                lowest_price = 0.0

                log(
                    "ACCOUNT UPDATE -> "
                    "posición cerrada"
                )


# ============================================================
# ORDER UPDATE
# ============================================================

def process_order_update(
    data
):

    order = data.get(
        "o",
        {}
    )

    if order.get(
        "s"
    ) != SYMBOL:

        return

    status = order.get(
        "X"
    )

    side = order.get(
        "S"
    )

    executed_qty = order.get(
        "z",
        "0"
    )

    avg_price = order.get(
        "ap",
        "0"
    )

    order_type = order.get(
        "o"
    )

    if status == "FILLED":

        log(
            f"ORDER FILLED | "
            f"type={order_type} | "
            f"side={side} | "
            f"qty={executed_qty} | "
            f"avg={avg_price}"
        )


# ============================================================
# USER WS MESSAGE
# ============================================================

def on_user_message(
    ws,
    message
):

    try:

        data = json.loads(
            message
        )

        event_type = data.get(
            "e"
        )

        if event_type == (
            "ACCOUNT_UPDATE"
        ):

            process_account_update(
                data
            )

        elif event_type == (
            "ORDER_TRADE_UPDATE"
        ):

            process_order_update(
                data
            )

        elif event_type == (
            "listenKeyExpired"
        ):

            log(
                "ListenKey expirado"
            )

    except Exception as e:

        log(
            f"Error User WS: {e}"
        )


def on_user_error(
    ws,
    error
):

    log(
        f"User WS error: {error}"
    )


def on_user_close(
    ws,
    code,
    msg
):

    log(
        f"User WS cerrado: "
        f"{code} {msg}"
    )


def on_user_open(
    ws
):

    log(
        "USER DATA WEBSOCKET CONECTADO"
    )


# ============================================================
# USER WEBSOCKET LOOP
# ============================================================

def user_websocket_loop():

    global listen_key
    global user_stream_control

    while True:

        stream_ws = None

        try:

            key = start_user_data_stream()

            ws_url = (
                "wss://fstream.binance.com/"
                "private/ws?listenKey="
                + key
                + "&events="
                "ORDER_TRADE_UPDATE/"
                "ACCOUNT_UPDATE"
            )

            if USE_TESTNET:

                ws_url = (
                    "wss://stream.binancefuture.com/"
                    "ws/"
                    + key
                )

            log(
                "Conectando User Data "
                "WebSocket..."
            )

            stream_ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_user_open,
                on_message=on_user_message,
                on_error=on_user_error,
                on_close=on_user_close
            )

            stream_ws.run_forever(
                ping_interval=60,
                ping_timeout=20
            )

        except Exception as e:

            log(
                f"User WS exception: {e}"
            )

        finally:

            try:

                if stream_ws:

                    stream_ws.close()

            except:

                pass

        with user_stream_control_lock:

            try:

                if user_stream_control:

                    user_stream_control.close()

            except:

                pass

            user_stream_control = None

        log(
            "Reconexión User Data "
            "en 60 segundos..."
        )

        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# SINCRONIZAR POSICION AL ARRANCAR
# ============================================================

def sync_existing_position():

    global position_side
    global position_qty
    global remaining_qty
    global entry_price
    global highest_price
    global lowest_price

    try:

        real = get_real_position()

    except Exception as e:

        log(
            f"No se pudo sincronizar "
            f"posicion: {e}"
        )

        return

    if not real:

        log(
            "ARRANQUE | "
            "No hay posicion abierta"
        )

        return

    with state_lock:

        position_side = real["side"]

        position_qty = real["qty"]

        remaining_qty = real["qty"]

        entry_price = real["entry"]

        highest_price = real["entry"]

        lowest_price = real["entry"]

    log(
        f"ARRANQUE | POSICION EXISTENTE | "
        f"{real['side']} | "
        f"qty={real['qty']} | "
        f"entry={real['entry']}"
    )

    log(
        "IMPORTANTE: una posicion "
        "recuperada al arrancar no tiene "
        "los TP locales reconstruidos "
        "hasta disponer de ATR nuevo."
    )


# ============================================================
# POSITION MANAGER
# ============================================================

def position_manager_loop():

    while True:

        try:

            manage_position()

        except Exception as e:

            log(
                f"Position manager error: {e}"
            )

        time.sleep(
            1
        )


# ============================================================
# STATUS
# ============================================================

def status_loop():

    while True:

        try:

            with state_lock:

                side = position_side

                qty = remaining_qty

                entry = entry_price

                stop = stop_price

                tp1 = tp1_price

                tp2 = tp2_price

                tp3 = tp3_price

            if side:

                log(
                    f"STATUS | "
                    f"price={current_price} | "
                    f"position={side} | "
                    f"qty={qty} | "
                    f"entry={entry:.8f} | "
                    f"SL={stop:.8f} | "
                    f"TP1={tp1:.8f} | "
                    f"TP2={tp2:.8f} | "
                    f"TP3={tp3:.8f}"
                )

            else:

                log(
                    f"STATUS | "
                    f"price={current_price} | "
                    f"position=None | "
                    f"qty=0"
                )

        except Exception as e:

            log(
                f"STATUS error: {e}"
            )

        time.sleep(
            60
        )


# ============================================================
# HEALTH SERVER
# ============================================================

class HealthHandler(
    BaseHTTPRequestHandler
):

    def do_GET(
        self
    ):

        self.send_response(
            200
        )

        self.send_header(
            "Content-type",
            "text/plain"
        )

        self.end_headers()

        self.wfile.write(
            b"BOT ONLINE"
        )

    def log_message(
        self,
        format,
        *args
    ):

        return


def health_server():

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    server = HTTPServer(
        (
            "0.0.0.0",
            port
        ),
        HealthHandler
    )

    log(
        f"Health server escuchando "
        f"en puerto {port}"
    )

    server.serve_forever()


# ============================================================
# MAIN
# ============================================================

def main():

    log(
        "=================================================="
    )

    log(
        "       ONGUSDT BREAKOUT FUTURES BOT"
    )

    log(
        "=================================================="
    )

    log_public_ip()

    log(
        f"USE_TESTNET = {USE_TESTNET}"
    )

    log(
        f"LIVE_TRADING = {LIVE_TRADING}"
    )

    log(
        f"SYMBOL = {SYMBOL}"
    )

    log(
        f"LEVERAGE = {LEVERAGE}x"
    )

    log(
        f"LOOKBACK = {LOOKBACK}"
    )

    log(
        f"ATR_PERIOD = {ATR_PERIOD}"
    )

    log(
        f"ATR_FILTER = {ATR_FILTER}"
    )

    log(
        f"ATR_STOP_MULT = {ATR_STOP_MULT}"
    )

    log(
        f"TP1 = {TP1_R}R"
    )

    log(
        f"TP2 = {TP2_R}R"
    )

    log(
        f"TP3 = {TP3_R}R"
    )

    log(
        "=================================================="
    )

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan las variables "
            "BINANCE_API_KEY y "
            "BINANCE_API_SECRET"
        )

    # --------------------------------------------------------
    # REGLAS
    # --------------------------------------------------------

    log(
        "Cargando reglas del simbolo..."
    )

    load_symbol_rules()

    # --------------------------------------------------------
    # CUENTA
    # --------------------------------------------------------

    if LIVE_TRADING:

        log(
            "Configurando leverage..."
        )

        set_leverage()

        log(
            "Configurando margin type..."
        )

        set_margin_type()

        log(
            "Sincronizando posicion..."
        )

        sync_existing_position()

    # --------------------------------------------------------
    # SERVIDOR HEALTH
    # --------------------------------------------------------

    threading.Thread(
        target=health_server,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # MARKET WS
    # --------------------------------------------------------

    threading.Thread(
        target=market_websocket_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # USER DATA WS
    # --------------------------------------------------------

    threading.Thread(
        target=user_websocket_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # KEEPALIVE
    # --------------------------------------------------------

    threading.Thread(
        target=user_stream_keepalive_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # POSITION MANAGER
    # --------------------------------------------------------

    threading.Thread(
        target=position_manager_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    threading.Thread(
        target=status_loop,
        daemon=True
    ).start()

    log(
        "BOT INICIADO CORRECTAMENTE"
    )

    log(
        "Esperando velas 1m..."
    )

    # --------------------------------------------------------
    # LOOP PRINCIPAL
    # --------------------------------------------------------

    while True:

        time.sleep(
            60
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
