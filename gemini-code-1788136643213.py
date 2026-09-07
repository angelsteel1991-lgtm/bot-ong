import os
import time
import math
import json
import uuid
import hmac
import hashlib
import threading
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import requests
import pandas as pd
import websocket

from datetime import datetime, timezone


# ============================================================
# UNIVERSAL BINANCE FUTURES BOT V3 - CORREGIDO
#
# CAMBIOS PRINCIPALES:
# - Riesgo real por trade
# - Apalancamiento configurable
# - Reglas reales por símbolo
# - stepSize / minQty / maxQty / minNotional / tickSize
# - Margen máximo
# - Carga inicial de velas
# - Stop real obligatorio
# - Sin duplicación de entradas
# - Sincronización de posiciones
# - WS para trading
# - REST únicamente para metadata / klines / leverage
# ============================================================


# ============================================================
# CONFIGURACION PRINCIPAL
# ============================================================

# IMPORTANTE:
# Arranca en FALSE.
# Cuando hayas comprobado que todo funciona correctamente:
#
# LIVE_TRADING = True
#
LIVE_TRADING = False

USE_TESTNET = False

INTERVAL = "5m"

MAX_POSITIONS = 4

START_BALANCE = 100.0

RISK_PER_TRADE = 0.01

LEVERAGE = 6

# Por seguridad no utilizamos todo el margen disponible.
MAX_MARGIN_USAGE = 0.80

# Nunca permitir que el riesgo teórico de la posición
# supere este multiplicador del riesgo calculado.
MAX_RISK_MULTIPLIER = 1.00

ATR_PERIOD = 14

ATR_STOP_MULT = 1.7

MIN_CANDLES = 40

INITIAL_CANDLES = 200

RECONNECT_SECONDS = 10

REST_TIMEOUT = 15


# ============================================================
# PROTECCION DE GANANCIA
# ============================================================

PROTECTION_LEVELS = [
    (1.0, 0.15),
    (1.5, 0.35),
    (2.0, 0.70),
    (2.5, 1.15),
    (3.0, 1.75),
    (4.0, 2.50),
    (5.0, 3.25),
    (6.0, 4.25),
    (8.0, 5.50),
    (10.0, 7.00),
]


TRAILING_LEVELS = [
    (4.0, 2.0),
    (6.0, 2.5),
    (8.0, 3.0),
    (10.0, 3.5),
]


TIME_STOP_CANDLES = 36

TIME_STOP_MIN_MFE_R = 0.50


# ============================================================
# API
# ============================================================

API_KEY = os.getenv("BINANCE_API_KEY")

API_SECRET = os.getenv("BINANCE_API_SECRET")


if USE_TESTNET:

    REST_BASE = "https://testnet.binancefuture.com"

    MARKET_WS_BASE = (
        "wss://stream.binancefuture.com/ws/"
    )

    WS_API_URL = (
        "wss://testnet.binancefuture.com/"
        "ws-fapi/v1"
    )

else:

    REST_BASE = "https://fapi.binance.com"

    MARKET_WS_BASE = (
        "wss://fstream.binance.com/market/ws/"
    )

    WS_API_URL = (
        "wss://ws-fapi.binance.com/"
        "ws-fapi/v1"
    )


# ============================================================
# SIMBOLOS
# ============================================================

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "SUIUSDT",
    "LTCUSDT",
    "BCHUSDT",
    "ETCUSDT",
    "FILUSDT",
    "APTUSDT",
    "ARBUSDT",
    "OPUSDT",
    "INJUSDT",
    "NEARUSDT",
    "ATOMUSDT",
    "DOTUSDT",
    "TRXUSDT",
    "UNIUSDT",
    "AAVEUSDT",
    "HBARUSDT",
    "TIAUSDT",
    "SEIUSDT",
    "WIFUSDT",
    "PEPEUSDT",
    "TONUSDT",
]


# ============================================================
# ESTADO
# ============================================================

market_data = {}

symbol_rules = {}

positions = {}

trade_history = []

paper_balance = START_BALANCE

state_lock = threading.RLock()

order_lock = threading.Lock()

ws_request_lock = threading.Lock()

user_stream_control = None

listen_key = None


# ============================================================
# CREAR ESTADO
# ============================================================

def create_symbol_state():

    return {
        "candles": [],
        "price": None,
        "atr": None,
        "last_signal": None,
        "last_candle_time": None,
    }


for symbol in SYMBOLS:

    market_data[symbol] = create_symbol_state()


# ============================================================
# LOG
# ============================================================

def log(message):

    now = datetime.now(
        timezone.utc
    ).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    print(
        f"[{now}] {message}",
        flush=True
    )


# ============================================================
# REST SIGNATURE
# ============================================================

def rest_signature(params):

    query = urlencode(
        params,
        doseq=True
    )

    return hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()


# ============================================================
# REST REQUEST
# ============================================================

def rest_request(
    method,
    path,
    params=None,
    signed=False
):

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan BINANCE_API_KEY o BINANCE_API_SECRET"
        )

    params = dict(
        params or {}
    )

    if signed:

        params["timestamp"] = int(
            time.time() * 1000
        )

        params["recvWindow"] = 5000

        params["signature"] = rest_signature(
            params
        )

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    url = REST_BASE + path

    if method == "GET":

        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=REST_TIMEOUT
        )

    elif method == "POST":

        response = requests.post(
            url,
            params=params,
            headers=headers,
            timeout=REST_TIMEOUT
        )

    elif method == "DELETE":

        response = requests.delete(
            url,
            params=params,
            headers=headers,
            timeout=REST_TIMEOUT
        )

    else:

        raise Exception(
            f"Metodo REST no soportado: {method}"
        )

    if response.status_code >= 400:

        raise Exception(
            f"REST {method} {path} "
            f"{response.status_code}: "
            f"{response.text}"
        )

    return response.json()


# ============================================================
# CARGAR REGLAS REALES
# ============================================================

def load_symbol_rules():

    global symbol_rules

    log(
        "Cargando reglas reales de Binance..."
    )

    data = rest_request(
        "GET",
        "/fapi/v1/exchangeInfo"
    )

    rules = {}

    for item in data.get(
        "symbols",
        []
    ):

        symbol = item.get(
            "symbol"
        )

        if symbol not in SYMBOLS:

            continue

        filters = {
            x.get("filterType"): x
            for x in item.get(
                "filters",
                []
            )
        }

        lot = filters.get(
            "LOT_SIZE",
            {}
        )

        market_lot = filters.get(
            "MARKET_LOT_SIZE",
            {}
        )

        price_filter = filters.get(
            "PRICE_FILTER",
            {}
        )

        min_notional_filter = filters.get(
            "MIN_NOTIONAL",
            filters.get(
                "NOTIONAL",
                {}
            )
        )

        step = float(
            market_lot.get(
                "stepSize",
                lot.get(
                    "stepSize",
                    0
                )
            )
        )

        min_qty = float(
            market_lot.get(
                "minQty",
                lot.get(
                    "minQty",
                    0
                )
            )
        )

        max_qty = float(
            market_lot.get(
                "maxQty",
                lot.get(
                    "maxQty",
                    0
                )
            )

        min_notional = float(
            min_notional_filter.get(
                "notional",
                min_notional_filter.get(
                    "minNotional",
                    0
                )
            )
        )

        tick_size = float(
            price_filter.get(
                "tickSize",
                0
            )
        )

        if step <= 0:

            log(
                f"{symbol} | "
                f"ERROR stepSize invalido"
            )

            continue

        rules[symbol] = {
            "step": step,
            "min_qty": min_qty,
            "max_qty": max_qty,
            "min_notional": min_notional,
            "tick_size": tick_size,
        }

        log(
            f"RULES | {symbol} | "
            f"step={step} | "
            f"minQty={min_qty} | "
            f"maxQty={max_qty} | "
            f"minNotional={min_notional} | "
            f"tickSize={tick_size}"
        )

    symbol_rules = rules

    missing = [
        x for x in SYMBOLS
        if x not in symbol_rules
    ]

    if missing:

        log(
            "ADVERTENCIA: símbolos sin reglas: "
            + ", ".join(missing)
        )

    log(
        f"REGLAS CARGADAS: "
        f"{len(symbol_rules)}/{len(SYMBOLS)}"
    )


# ============================================================
# PRECISION
# ============================================================

def decimals_from_step(step):

    if step >= 1:

        return 0

    text = f"{step:.12f}".rstrip("0")

    if "." not in text:

        return 0

    return len(
        text.split(".")[1]
    )


# ============================================================
# REDONDEAR CANTIDAD HACIA ABAJO
# ============================================================

def floor_to_step(
    value,
    step
):

    if step <= 0:

        return 0.0

    value_dec = Decimal(
        str(value)
    )

    step_dec = Decimal(
        str(step)
    )

    units = (
        value_dec / step_dec
    ).to_integral_value(
        rounding=ROUND_DOWN
    )

    result = (
        units * step_dec
    )

    return float(result)


# ============================================================
# REDONDEAR PRECIO
# ============================================================

def floor_price(
    price,
    tick_size
):

    if tick_size <= 0:

        return float(price)

    value_dec = Decimal(
        str(price)
    )

    tick_dec = Decimal(
        str(tick_size)
    )

    units = (
        value_dec / tick_dec
    ).to_integral_value(
        rounding=ROUND_DOWN
    )

    return float(
        units * tick_dec
    )


# ============================================================
# NORMALIZAR CANTIDAD
#
# IMPORTANTE:
# NO fuerza minQty.
#
# Si minQty hace que el riesgo supere el 1%,
# la operación se rechaza.
# ============================================================

def normalize_quantity(
    symbol,
    quantity,
    price
):

    rule = symbol_rules.get(
        symbol
    )

    if not rule:

        return 0.0

    step = rule["step"]

    min_qty = rule["min_qty"]

    max_qty = rule["max_qty"]

    min_notional = rule["min_notional"]

    quantity = floor_to_step(
        quantity,
        step
    )

    if quantity <= 0:

        return 0.0

    if max_qty > 0:

        quantity = min(
            quantity,
            max_qty
        )

        quantity = floor_to_step(
            quantity,
            step
        )

    if quantity < min_qty:

        return 0.0

    if min_notional > 0:

        if quantity * price < min_notional:

            return 0.0

    return quantity


# ============================================================
# CONFIGURAR LEVERAGE
# ============================================================

def set_symbol_leverage(
    symbol
):

    if not LIVE_TRADING:

        log(
            f"{symbol} | "
            f"TEST MODE | leverage pretendido = x{LEVERAGE}"
        )

        return True

    try:

        result = rest_request(
            "POST",
            "/fapi/v1/leverage",
            {
                "symbol": symbol,
                "leverage": LEVERAGE,
            },
            signed=True
        )

        actual = result.get(
            "leverage"
        )

        log(
            f"{symbol} | "
            f"LEVERAGE CONFIGURADO = x{actual}"
        )

        return True

    except Exception as e:

        log(
            f"{symbol} | "
            f"ERROR CONFIGURANDO LEVERAGE: {e}"
        )

        return False


def configure_all_leverage():

    log(
        f"Configurando leverage x{LEVERAGE}..."
    )

    for symbol in SYMBOLS:

        if symbol not in symbol_rules:

            continue

        set_symbol_leverage(
            symbol
        )

        time.sleep(
            0.08
        )


# ============================================================
# ATR
# ============================================================

def calculate_atr(
    df,
    period=ATR_PERIOD
):

    high = df["high"]

    low = df["low"]

    close = df["close"]

    previous_close = close.shift(
        1
    )

    tr = pd.concat(
        [
            high - low,
            (
                high - previous_close
            ).abs(),
            (
                low - previous_close
            ).abs(),
        ],
        axis=1,
    ).max(
        axis=1
    )

    atr = tr.rolling(
        period
    ).mean()

    if atr.empty:

        return None

    value = atr.iloc[-1]

    if pd.isna(value):

        return None

    return float(value)


# ============================================================
# INDICADORES
# ============================================================

def calculate_indicators(df):

    df = df.copy()

    df["ema9"] = df["close"].ewm(
        span=9,
        adjust=False
    ).mean()

    df["ema21"] = df["close"].ewm(
        span=21,
        adjust=False
    ).mean()

    df["ema50"] = df["close"].ewm(
        span=50,
        adjust=False
    ).mean()

    delta = df["close"].diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

    avg_gain = gain.rolling(
        14
    ).mean()

    avg_loss = loss.rolling(
        14
    ).mean()

    rs = (
        avg_gain
        / avg_loss.replace(
            0,
            1e-10
        )
    )

    df["rsi"] = (
        100
        - (
            100
            / (
                1 + rs
            )
        )
    )

    df["volume_ma"] = df[
        "volume"
    ].rolling(
        20
    ).mean()

    df["atr"] = calculate_atr(
        df
    )

    return df


# ============================================================
# SEÑAL
# ============================================================

def calculate_signal(
    symbol
):

    with state_lock:

        candles = list(
            market_data[symbol][
                "candles"
            ]
        )

    if len(candles) < MIN_CANDLES:

        return None

    df = pd.DataFrame(
        candles,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ],
    )

    df = calculate_indicators(
        df
    )

    last = df.iloc[-1]

    previous = df.iloc[-2]

    values = [
        last["ema9"],
        last["ema21"],
        last["ema50"],
        last["rsi"],
        last["volume_ma"],
        last["atr"],
    ]

    if any(
        pd.isna(x)
        for x in values
    ):

        return None

    close = float(
        last["close"]
    )

    ema9 = float(
        last["ema9"]
    )

    ema21 = float(
        last["ema21"]
    )

    ema50 = float(
        last["ema50"]
    )

    rsi = float(
        last["rsi"]
    )

    volume = float(
        last["volume"]
    )

    volume_ma = float(
        last["volume_ma"]
    )

    previous_close = float(
        previous["close"]
    )

    long_score = 0

    if ema9 > ema21:

        long_score += 1

    if ema21 > ema50:

        long_score += 1

    if close > ema9:

        long_score += 1

    if 50 < rsi < 72:

        long_score += 1

    if volume > volume_ma:

        long_score += 1

    if close > previous_close:

        long_score += 1

    short_score = 0

    if ema9 < ema21:

        short_score += 1

    if ema21 < ema50:

        short_score += 1

    if close < ema9:

        short_score += 1

    if 28 < rsi < 50:

        short_score += 1

    if volume > volume_ma:

        short_score += 1

    if close < previous_close:

        short_score += 1

    signal = None

    if (
        long_score >= 5
        and long_score - short_score >= 2
    ):

        signal = "LONG"

    elif (
        short_score >= 5
        and short_score - long_score >= 2
    ):

        signal = "SHORT"

    log(
        f"{symbol} | "
        f"price={close:.8f} | "
        f"RSI={rsi:.1f} | "
        f"LONG={long_score} | "
        f"SHORT={short_score} | "
        f"signal={signal}"
    )

    return signal


# ============================================================
# WS API
# ============================================================

def _ws_signature(params):

    query = urlencode(
        sorted(
            params.items()
        ),
        doseq=True
    )

    return hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()


def ws_api_request(
    method,
    params=None,
    signed=True,
    timeout=15
):

    global user_stream_control

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan API KEY / SECRET"
        )

    with ws_request_lock:

        with user_stream_control_lock:

            ws = user_stream_control

        if ws is None:

            raise Exception(
                "WS API no conectada"
            )

        request_params = dict(
            params or {}
        )

        if signed:

            request_params[
                "apiKey"
            ] = API_KEY

            request_params[
                "timestamp"
            ] = int(
                time.time() * 1000
            )

            request_params[
                "recvWindow"
            ] = 5000

            request_params[
                "signature"
            ] = _ws_signature(
                request_params
            )

        request_id = str(
            uuid.uuid4()
        )

        request = {
            "id": request_id,
            "method": method,
            "params": request_params,
        }

        ws.settimeout(
            timeout
        )

        ws.send(
            json.dumps(
                request
            )
        )

        while True:

            raw = ws.recv()

            response = json.loads(
                raw
            )

            if response.get(
                "id"
            ) == request_id:

                break

    if response.get(
        "status"
    ) != 200:

        raise Exception(
            f"{method} rechazado: "
            f"{response}"
        )

    return response.get(
        "result"
    )


# ============================================================
# BALANCE
# ============================================================

def get_usdt_balance():

    if not LIVE_TRADING:

        return START_BALANCE

    result = ws_api_request(
        "account.balance",
        {},
        signed=True
    )

    for item in (
        result or []
    ):

        if item.get(
            "asset"
        ) == "USDT":

            return float(
                item.get(
                    "availableBalance",
                    item.get(
                        "balance",
                        0
                    )
                )
            )

    return 0.0


# ============================================================
# MARGEN DISPONIBLE
# ============================================================

def calculate_max_quantity_by_margin(
    symbol,
    price,
    available_balance
):

    if price <= 0:

        return 0.0

    if available_balance <= 0:

        return 0.0

    max_margin = (
        available_balance
        * MAX_MARGIN_USAGE
    )

    max_notional = (
        max_margin
        * LEVERAGE
    )

    return (
        max_notional
        / price
    )


# ============================================================
# CALCULO DE POSICION
#
# RIESGO = balance * 1%
#
# quantity = riesgo / distancia_stop
#
# El leverage NO multiplica el riesgo.
#
# Luego limitamos por margen.
# ============================================================

def calculate_position_size(
    symbol,
    entry,
    stop_distance
):

    if (
        entry <= 0
        or stop_distance <= 0
    ):

        return 0.0

    balance = get_usdt_balance()

    if balance <= 0:

        return 0.0

    risk_money = (
        balance
        * RISK_PER_TRADE
    )

    risk_quantity = (
        risk_money
        / stop_distance
    )

    margin_quantity = (
        calculate_max_quantity_by_margin(
            symbol,
            entry,
            balance
        )
    )

    raw_quantity = min(
        risk_quantity,
        margin_quantity
    )

    quantity = normalize_quantity(
        symbol,
        raw_quantity,
        entry
    )

    if quantity <= 0:

        rule = symbol_rules.get(
            symbol,
            {}
        )

        min_qty = rule.get(
            "min_qty",
            0
        )

        min_notional = rule.get(
            "min_notional",
            0
        )

        log(
            f"{symbol} | "
            f"NO TRADE: capital demasiado pequeño "
            f"para cumplir reglas sin superar "
            f"el riesgo del {RISK_PER_TRADE * 100:.2f}% | "
            f"balance={balance:.4f} | "
            f"riesgo=${risk_money:.4f} | "
            f"minQty={min_qty} | "
            f"minNotional={min_notional}"
        )

        return 0.0

    theoretical_risk = (
        quantity
        * stop_distance
    )

    max_allowed_risk = (
        risk_money
        * MAX_RISK_MULTIPLIER
    )

    if theoretical_risk > (
        max_allowed_risk
        + 1e-12
    ):

        log(
            f"{symbol} | "
            f"NO TRADE: cantidad mínima "
            f"superaría riesgo permitido | "
            f"risk={theoretical_risk:.8f} | "
            f"max={max_allowed_risk:.8f}"
        )

        return 0.0

    notional = (
        quantity
        * entry
    )

    margin_required = (
        notional
        / LEVERAGE
    )

    if margin_required > (
        balance
        * MAX_MARGIN_USAGE
    ):

        log(
            f"{symbol} | "
            f"NO TRADE: margen insuficiente"
        )

        return 0.0

    log(
        f"{symbol} | "
        f"SIZE | "
        f"balance={balance:.4f} | "
        f"risk=${risk_money:.4f} | "
        f"qty={quantity:.12f} | "
        f"notional=${notional:.4f} | "
        f"margin=${margin_required:.4f} | "
        f"leverage=x{LEVERAGE} | "
        f"risk_real=${theoretical_risk:.4f}"
    )

    return quantity


# ============================================================
# STOP PRICE
# ============================================================

def normalize_stop_price(
    symbol,
    price
):

    rule = symbol_rules.get(
        symbol
    )

    if not rule:

        return price

    tick = rule.get(
        "tick_size",
        0
    )

    if tick <= 0:

        return price

    return floor_price(
        price,
        tick
    )


# ============================================================
# STOP REAL
# ============================================================

def place_exchange_stop(
    symbol,
    side,
    stop_price
):

    close_side = (
        "SELL"
        if side == "LONG"
        else "BUY"
    )

    stop_price = normalize_stop_price(
        symbol,
        stop_price
    )

    params = {
        "algoType": "CONDITIONAL",
        "symbol": symbol,
        "side": close_side,
        "type": "STOP_MARKET",
        "triggerPrice": str(
            stop_price
        ),
        "closePosition": "true",
        "workingType": "MARK_PRICE",
        "priceProtect": "FALSE",
    }

    return ws_api_request(
        "algoOrder.place",
        params,
        signed=True
    )


# ============================================================
# CANCELAR STOP
# ============================================================

def cancel_order(
    symbol,
    order_id
):

    if not order_id:

        return

    try:

        ws_api_request(
            "algoOrder.cancel",
            {
                "symbol": symbol,
                "algoId": int(
                    order_id
                ),
            },
            signed=True
        )

    except Exception as e:

        log(
            f"{symbol} | "
            f"Error cancelando STOP "
            f"{order_id}: {e}"
        )


# ============================================================
# ACTUALIZAR STOP
# ============================================================

def update_exchange_stop(
    symbol,
    position
):

    stop = float(
        position["stop"]
    )

    stop = normalize_stop_price(
        symbol,
        stop
    )

    old_id = position.get(
        "stop_order_id"
    )

    old_stop = position.get(
        "exchange_stop_price"
    )

    if (
        old_id
        and old_stop is not None
        and abs(
            old_stop - stop
        )
        <= max(
            abs(stop) * 0.000001,
            1e-12
        )
    ):

        return True

    if old_id:

        cancel_order(
            symbol,
            old_id
        )

    try:

        result = place_exchange_stop(
            symbol,
            position["side"],
            stop
        )

        result = result or {}

        algo_id = result.get(
            "algoId"
        )

        if not algo_id:

            raise Exception(
                f"Binance no devolvio algoId: "
                f"{result}"
            )

        position[
            "stop_order_id"
        ] = algo_id

        position[
            "exchange_stop_price"
        ] = stop

        log(
            f"{symbol} | "
            f"STOP REAL OK | "
            f"stop={stop:.8f} | "
            f"algoId={algo_id}"
        )

        return True

    except Exception as e:

        position[
            "stop_order_id"
        ] = None

        position[
            "exchange_stop_price"
        ] = None

        log(
            f"{symbol} | "
            f"ERROR STOP REAL: {e}"
        )

        return False


# ============================================================
# ABRIR POSICION
# ============================================================

def open_position(
    symbol,
    side
):

    with order_lock:

        with state_lock:

            if symbol in positions:

                log(
                    f"{symbol} | "
                    f"Entrada cancelada: "
                    f"ya existe posicion"
                )

                return

            if len(positions) >= MAX_POSITIONS:

                return

            price = market_data[
                symbol
            ]["price"]

            atr = market_data[
                symbol
            ]["atr"]

        if (
            price is None
            or atr is None
            or atr <= 0
        ):

            return

        stop_distance = (
            atr
            * ATR_STOP_MULT
        )

        quantity = calculate_position_size(
            symbol,
            price,
            stop_distance
        )

        if quantity <= 0:

            return

        order_side = (
            "BUY"
            if side == "LONG"
            else "SELL"
        )

        # ----------------------------------------------------
        # TEST MODE
        # ----------------------------------------------------

        if not LIVE_TRADING:

            avg_price = price

            executed_qty = quantity

            log(
                f"TEST OPEN | "
                f"{symbol} | "
                f"{side} | "
                f"entry={avg_price:.8f} | "
                f"qty={executed_qty:.12f}"
            )

        # ----------------------------------------------------
        # LIVE
        # ----------------------------------------------------

        else:

            try:

                result = ws_api_request(
                    "order.place",
                    {
                        "symbol": symbol,
                        "side": order_side,
                        "type": "MARKET",
                        "quantity": quantity,
                        "newOrderRespType": "RESULT",
                    },
                    signed=True
                )

                result = result or {}

                executed_qty = float(
                    result.get(
                        "executedQty",
                        0
                    )
                )

                avg_price = float(
                    result.get(
                        "avgPrice",
                        0
                    )
                    or 0
                )

                if executed_qty <= 0:

                    raise Exception(
                        f"Orden sin ejecucion: "
                        f"{result}"
                    )

                if avg_price <= 0:

                    avg_price = price

                log(
                    f"LIVE ENTRY EXECUTED | "
                    f"{symbol} | "
                    f"{side} | "
                    f"qty={executed_qty:.12f} | "
                    f"entry={avg_price:.8f}"
                )

            except Exception as e:

                log(
                    f"{symbol} | "
                    f"ERROR ENTRADA: {e}"
                )

                return

        # ----------------------------------------------------
        # CREAR POSICION INTERNA
        # ----------------------------------------------------

        initial_stop = (
            avg_price - stop_distance
            if side == "LONG"
            else avg_price + stop_distance
        )

        position = {
            "symbol": symbol,
            "side": side,
            "entry": avg_price,
            "quantity": executed_qty,
            "initial_risk": stop_distance,
            "stop": initial_stop,
            "highest": avg_price,
            "lowest": avg_price,
            "mfe_r": 0.0,
            "bars": 0,
            "opened_at": time.time(),
            "stop_order_id": None,
            "exchange_stop_price": None,
        }

        with state_lock:

            # Segunda comprobacion antes de insertar.
            if symbol in positions:

                log(
                    f"{symbol} | "
                    f"Posicion ya existente. "
                    f"No se registra segunda posicion."
                )

                return

            positions[
                symbol
            ] = position

        # ----------------------------------------------------
        # STOP REAL
        # ----------------------------------------------------

        if LIVE_TRADING:

            stop_ok = update_exchange_stop(
                symbol,
                position
            )

            if not stop_ok:

                log(
                    f"{symbol} | "
                    f"ALERTA CRITICA: "
                    f"entrada ejecutada pero STOP "
                    f"no pudo colocarse."
                )

                try:

                    close_position(
                        symbol,
                        avg_price,
                        "STOP NO COLOCADO"
                    )

                except Exception as close_error:

                    log(
                        f"{symbol} | "
                        f"FALLO CIERRE EMERGENCIA: "
                        f"{close_error}"
                    )

                return

        log(
            f"POSITION ACTIVE | "
            f"{symbol} | "
            f"{side} | "
            f"entry={avg_price:.8f} | "
            f"qty={executed_qty:.12f} | "
            f"stop={initial_stop:.8f}"
        )


# ============================================================
# CERRAR POSICION
# ============================================================

def close_position(
    symbol,
    price,
    reason
):

    with order_lock:

        with state_lock:

            position = positions.get(
                symbol
            )

            if position is None:

                return

            side = position[
                "side"
            ]

            entry = position[
                "entry"
            ]

            quantity = position[
                "quantity"
            ]

            stop_order_id = position.get(
                "stop_order_id"
            )

        if stop_order_id:

            cancel_order(
                symbol,
                stop_order_id
            )

        close_side = (
            "SELL"
            if side == "LONG"
            else "BUY"
        )

        # ----------------------------------------------------
        # TEST
        # ----------------------------------------------------

        if not LIVE_TRADING:

            exit_price = price

            executed_qty = quantity

        # ----------------------------------------------------
        # LIVE
        # ----------------------------------------------------

        else:

            try:

                result = ws_api_request(
                    "order.place",
                    {
                        "symbol": symbol,
                        "side": close_side,
                        "type": "MARKET",
                        "quantity": quantity,
                        "reduceOnly": "true",
                        "newOrderRespType": "RESULT",
                    },
                    signed=True
                )

                result = result or {}

                executed_qty = float(
                    result.get(
                        "executedQty",
                        0
                    )
                )

                exit_price = float(
                    result.get(
                        "avgPrice",
                        0
                    )
                    or 0
                )

                if executed_qty <= 0:

                    raise Exception(
                        f"Cierre sin ejecucion: "
                        f"{result}"
                    )

                if exit_price <= 0:

                    exit_price = price

            except Exception as e:

                log(
                    f"{symbol} | "
                    f"ERROR CERRANDO POSICION: {e}"
                )

                return

        # ----------------------------------------------------
        # PNL
        # ----------------------------------------------------

        pnl = (
            (
                exit_price
                - entry
            )
            * executed_qty
            if side == "LONG"
            else
            (
                entry
                - exit_price
            )
            * executed_qty
        )

        initial_risk = position[
            "initial_risk"
        ]

        if initial_risk > 0:

            if side == "LONG":

                r_multiple = (
                    exit_price
                    - entry
                ) / initial_risk

            else:

                r_multiple = (
                    entry
                    - exit_price
                ) / initial_risk

        else:

            r_multiple = 0.0

        with state_lock:

            positions.pop(
                symbol,
                None
            )

        log(
            f"CLOSE | "
            f"{symbol} | "
            f"{side} | "
            f"entry={entry:.8f} | "
            f"exit={exit_price:.8f} | "
            f"qty={executed_qty:.12f} | "
            f"PnL={pnl:+.6f} USDT | "
            f"R={r_multiple:+.2f} | "
            f"reason={reason}"
        )


# ============================================================
# GESTION DE POSICION
# ============================================================

def manage_position(
    symbol
):

    with state_lock:

        position = positions.get(
            symbol
        )

        if position is None:

            return

        price = market_data[
            symbol
        ]["price"]

    if price is None:

        return

    side = position[
        "side"
    ]

    entry = position[
        "entry"
    ]

    initial_risk = position[
        "initial_risk"
    ]

    if initial_risk <= 0:

        return

    with state_lock:

        if side == "LONG":

            position[
                "highest"
            ] = max(
                position["highest"],
                price
            )

            mfe = (
                position["highest"]
                - entry
            ) / initial_risk

        else:

            position[
                "lowest"
            ] = min(
                position["lowest"],
                price
            )

            mfe = (
                entry
                - position["lowest"]
            ) / initial_risk

        position[
            "mfe_r"
        ] = max(
            position["mfe_r"],
            mfe
        )

    new_stop = position[
        "stop"
    ]

    for trigger_r, protect_r in PROTECTION_LEVELS:

        if mfe >= trigger_r:

            if side == "LONG":

                candidate = (
                    entry
                    + protect_r
                    * initial_risk
                )

                if candidate > new_stop:

                    new_stop = candidate

            else:

                candidate = (
                    entry
                    - protect_r
                    * initial_risk
                )

                if candidate < new_stop:

                    new_stop = candidate

    for trigger_r, gap_r in TRAILING_LEVELS:

        if mfe >= trigger_r:

            if side == "LONG":

                candidate = (
                    position["highest"]
                    - gap_r
                    * initial_risk
                )

                if candidate > new_stop:

                    new_stop = candidate

            else:

                candidate = (
                    position["lowest"]
                    + gap_r
                    * initial_risk
                )

                if candidate < new_stop:

                    new_stop = candidate

    old_stop = position[
        "stop"
    ]

    with state_lock:

        if side == "LONG":

            position[
                "stop"
            ] = max(
                position["stop"],
                new_stop
            )

        else:

            position[
                "stop"
            ] = min(
                position["stop"],
                new_stop
            )

        stop = position[
            "stop"
        ]

        position[
            "bars"
        ] += 1

        bars = position[
            "bars"
        ]

        mfe_r = position[
            "mfe_r"
        ]

    if abs(
        stop - old_stop
    ) > max(
        abs(stop) * 0.000001,
        1e-12
    ):

        if LIVE_TRADING:

            update_exchange_stop(
                symbol,
                position
            )

    # --------------------------------------------------------
    # STOP LOCAL DE SEGURIDAD
    # --------------------------------------------------------

    if (
        side == "LONG"
        and price <= stop
    ):

        close_position(
            symbol,
            price,
            f"PROTECTED STOP | "
            f"MFE={mfe_r:.2f}R"
        )

        return

    if (
        side == "SHORT"
        and price >= stop
    ):

        close_position(
            symbol,
            price,
            f"PROTECTED STOP | "
            f"MFE={mfe_r:.2f}R"
        )

        return

    # --------------------------------------------------------
    # TIME STOP
    # --------------------------------------------------------

    if (
        bars >= TIME_STOP_CANDLES
        and mfe_r < TIME_STOP_MIN_MFE_R
    ):

        close_position(
            symbol,
            price,
            f"TIME STOP | "
            f"MFE={mfe_r:.2f}R"
        )


# ============================================================
# PROCESAR VELA
# ============================================================

def process_candle(
    symbol
):

    with state_lock:

        candles = list(
            market_data[
                symbol
            ]["candles"]
        )

    if len(candles) < MIN_CANDLES:

        return

    df = pd.DataFrame(
        candles,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ],
    )

    df = calculate_indicators(
        df
    )

    atr = df[
        "atr"
    ].iloc[-1]

    if pd.isna(atr):

        return

    with state_lock:

        market_data[
            symbol
        ]["atr"] = float(
            atr
        )

    signal = calculate_signal(
        symbol
    )

    if signal is None:

        return

    with state_lock:

        existing = positions.get(
            symbol
        )

        total_positions = len(
            positions
        )

        price = market_data[
            symbol
        ]["price"]

    if existing is not None:

        if existing[
            "side"
        ] != signal:

            if price is not None:

                close_position(
                    symbol,
                    price,
                    "SEÑAL CONTRARIA"
                )

        return

    if total_positions >= MAX_POSITIONS:

        return

    open_position(
        symbol,
        signal
    )


# ============================================================
# CARGAR VELAS INICIALES
# ============================================================

def load_initial_candles():

    log(
        "Cargando historial inicial de velas..."
    )

    for symbol in SYMBOLS:

        try:

            data = rest_request(
                "GET",
                "/fapi/v1/klines",
                {
                    "symbol": symbol,
                    "interval": INTERVAL,
                    "limit": INITIAL_CANDLES,
                }
            )

            candles = []

            for row in data:

                candles.append(
                    [
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5]),
                    ]
                )

            if candles:

                # La última vela puede estar abierta.
                # La dejamos afuera para señales.
                current_open = candles[-1][0]

                with state_lock:

                    market_data[
                        symbol
                    ]["candles"] = candles[:-1]

                    market_data[
                        symbol
                    ]["price"] = candles[-1][4]

                log(
                    f"{symbol} | "
                    f"Velas iniciales cargadas: "
                    f"{len(candles[:-1])}"
                )

        except Exception as e:

            log(
                f"{symbol} | "
                f"ERROR historial inicial: {e}"
            )

        time.sleep(
            0.08
        )


# ============================================================
# MARKET WS MESSAGE
# ============================================================

def on_market_message(
    symbol,
    ws,
    message
):

    try:

        data = json.loads(
            message
        )

        data = data.get(
            "data",
            data
        )

        kline = data.get(
            "k"
        )

        if not kline:

            return

        price = float(
            kline["c"]
        )

        with state_lock:

            market_data[
                symbol
            ]["price"] = price

        # Vela todavía abierta:
        # actualizamos precio pero NO generamos señal.
        if not kline["x"]:

            return

        candle = [
            int(kline["t"]),
            float(kline["o"]),
            float(kline["h"]),
            float(kline["l"]),
            float(kline["c"]),
            float(kline["v"]),
        ]

        with state_lock:

            candles = market_data[
                symbol
            ]["candles"]

            if candles:

                if candles[-1][0] == candle[0]:

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

            market_data[
                symbol
            ]["last_candle_time"] = candle[0]

        process_candle(
            symbol
        )

    except Exception as e:

        log(
            f"{symbol} | "
            f"Market WS error: {e}"
        )


# ============================================================
# MARKET WS CALLBACKS
# ============================================================

def make_market_open(
    symbol
):

    def callback(ws):

        log(
            f"{symbol} | "
            f"MARKET WS CONECTADO"
        )

    return callback


def make_market_error(
    symbol
):

    def callback(
        ws,
        error
    ):

        log(
            f"{symbol} | "
            f"Market WS error: {error}"
        )

    return callback


def make_market_close(
    symbol
):

    def callback(
        ws,
        code,
        msg
    ):

        log(
            f"{symbol} | "
            f"Market WS cerrado: "
            f"{code} {msg}"
        )

    return callback


# ============================================================
# MARKET WS
# ============================================================

def market_websocket_loop(
    symbol
):

    market_ws = (
        MARKET_WS_BASE
        + symbol.lower()
        + "@kline_"
        + INTERVAL
    )

    while True:

        try:

            ws = websocket.WebSocketApp(
                market_ws,

                on_open=make_market_open(
                    symbol
                ),

                on_message=lambda ws, message:
                    on_market_message(
                        symbol,
                        ws,
                        message
                    ),

                on_error=make_market_error(
                    symbol
                ),

                on_close=make_market_close(
                    symbol
                ),
            )

            ws.run_forever(
                ping_interval=30,
                ping_timeout=10
            )

        except Exception as e:

            log(
                f"{symbol} | "
                f"Market exception: {e}"
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
        "Abriendo WS API..."
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
        },
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
            f"userDataStream.start "
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
            "Binance no devolvio listenKey"
        )

    with user_stream_control_lock:

        user_stream_control = ws

    listen_key = key

    log(
        "WS API CONECTADA"
    )

    return listen_key


# ============================================================
# USER WS CALLBACKS
# ============================================================

def on_user_open(ws):

    log(
        "USER DATA WS CONECTADO"
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


# ============================================================
# SINCRONIZAR POSICIONES
# ============================================================

def sync_account_positions(
    data
):

    account = data.get(
        "a",
        {}
    )

    for item in account.get(
        "P",
        []
    ):

        symbol = item.get(
            "s"
        )

        if symbol not in SYMBOLS:

            continue

        try:

            amount = float(
                item.get(
                    "pa",
                    0
                )
            )

            entry = float(
                item.get(
                    "ep",
                    0
                )
            )

        except Exception:

            continue

        if amount == 0:

            with state_lock:

                positions.pop(
                    symbol,
                    None
                )

            continue

        side = (
            "LONG"
            if amount > 0
            else "SHORT"
        )

        qty = abs(
            amount
        )

        with state_lock:

            old = positions.get(
                symbol
            )

            atr = market_data[
                symbol
            ].get(
                "atr"
            )

            if (
                atr is None
                or atr <= 0
            ):

                risk = (
                    entry
                    * 0.01
                )

            else:

                risk = (
                    atr
                    * ATR_STOP_MULT
                )

            if (
                old
                and old.get(
                    "side"
                ) == side
            ):

                stop = old.get(
                    "stop",
                    (
                        entry - risk
                        if side == "LONG"
                        else entry + risk
                    )
                )

                mfe_r = old.get(
                    "mfe_r",
                    0.0
                )

                bars = old.get(
                    "bars",
                    0
                )

                opened_at = old.get(
                    "opened_at",
                    time.time()
                )

                stop_id = old.get(
                    "stop_order_id"
                )

                exchange_stop = old.get(
                    "exchange_stop_price"
                )

            else:

                stop = (
                    entry - risk
                    if side == "LONG"
                    else entry + risk
                )

                mfe_r = 0.0

                bars = 0

                opened_at = time.time()

                stop_id = None

                exchange_stop = None

            current_price = (
                market_data[
                    symbol
                ].get(
                    "price"
                )
                or entry
            )

            positions[
                symbol
            ] = {
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "quantity": qty,
                "initial_risk": risk,
                "stop": stop,
                "highest": max(
                    entry,
                    current_price
                ),
                "lowest": min(
                    entry,
                    current_price
                ),
                "mfe_r": mfe_r,
                "bars": bars,
                "opened_at": opened_at,
                "stop_order_id": stop_id,
                "exchange_stop_price": exchange_stop,
            }

        log(
            f"ACCOUNT SYNC | "
            f"{symbol} | "
            f"{side} | "
            f"qty={qty:.12f} | "
            f"entry={entry:.8f}"
        )


# ============================================================
# USER DATA MESSAGE
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

        if event_type == "listenKeyExpired":

            log(
                "LISTEN KEY EXPIRADO"
            )

        elif event_type == "ACCOUNT_UPDATE":

            sync_account_positions(
                data
            )

            log(
                "ACCOUNT_UPDATE recibido"
            )

        elif event_type == "ORDER_TRADE_UPDATE":

            order = data.get(
                "o",
                {}
            )

            log(
                f"ORDER UPDATE | "
                f"symbol={order.get('s')} | "
                f"side={order.get('S')} | "
                f"status={order.get('X')} | "
                f"exec={order.get('x')}"
            )

    except Exception as e:

        log(
            f"Error User WS: {e}"
        )


# ============================================================
# USER WS LOOP
# ============================================================

def user_websocket_loop():

    global user_stream_control
    global listen_key

    while True:

        stream_ws = None

        try:

            key = start_user_data_stream()

            if USE_TESTNET:

                ws_url = (
                    "wss://stream.binancefuture.com/ws/"
                    + key
                )

            else:

                ws_url = (
                    "wss://fstream.binance.com/private/ws/"
                    + key
                )

            log(
                "Conectando User Data Stream..."
            )

            stream_ws = websocket.WebSocketApp(
                ws_url,

                on_open=on_user_open,

                on_message=on_user_message,

                on_error=on_user_error,

                on_close=on_user_close,
            )

            stream_ws.run_forever(
                ping_interval=30,
                ping_timeout=10
            )

        except Exception as e:

            log(
                f"User WS exception: {e}"
            )

        finally:

            try:

                if stream_ws:

                    stream_ws.close()

            except Exception:

                pass

            with user_stream_control_lock:

                try:

                    if user_stream_control:

                        user_stream_control.close()

                except Exception:

                    pass

                user_stream_control = None

        log(
            "Reconectando User Data en "
            f"{RECONNECT_SECONDS}s..."
        )

        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# KEEPALIVE
# ============================================================

def user_stream_keepalive_loop():

    global user_stream_control
    global listen_key

    while True:

        time.sleep(
            30 * 60
        )

        try:

            with user_stream_control_lock:

                ws = user_stream_control

            if ws is None:

                continue

            request_id = str(
                uuid.uuid4()
            )

            request = {
                "id": request_id,
                "method": "userDataStream.ping",
                "params": {
                    "apiKey": API_KEY
                },
            }

            with ws_request_lock:

                with user_stream_control_lock:

                    ws = user_stream_control

                    if ws is None:

                        continue

                    ws.send(
                        json.dumps(
                            request
                        )
                    )

                    ws.settimeout(
                        15
                    )

                    while True:

                        response = json.loads(
                            ws.recv()
                        )

                        if response.get(
                            "id"
                        ) == request_id:

                            break

            if response.get(
                "status"
            ) == 200:

                new_key = (
                    response
                    .get(
                        "result",
                        {}
                    )
                    .get(
                        "listenKey"
                    )
                )

                if new_key:

                    listen_key = new_key

                log(
                    "USER DATA KEEPALIVE OK"
                )

            else:

                log(
                    f"KEEPALIVE ERROR: "
                    f"{response}"
                )

        except Exception as e:

            log(
                f"Keepalive error: {e}"
            )

            with user_stream_control_lock:

                try:

                    if user_stream_control:

                        user_stream_control.close()

                except Exception:

                    pass

                user_stream_control = None


# ============================================================
# POSITION MANAGER
# ============================================================

def position_manager_loop():

    while True:

        try:

            with state_lock:

                symbols = list(
                    positions.keys()
                )

            for symbol in symbols:

                manage_position(
                    symbol
                )

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

                active = len(
                    positions
                )

            try:

                balance = get_usdt_balance()

            except Exception as e:

                balance = None

                log(
                    f"STATUS balance error: {e}"
                )

            log(
                f"STATUS | "
                f"balance="
                f"{balance if balance is not None else 'N/A'} | "
                f"positions="
                f"{active}/{MAX_POSITIONS} | "
                f"leverage=x{LEVERAGE} | "
                f"risk={RISK_PER_TRADE * 100:.2f}%"
            )

        except Exception as e:

            log(
                f"Status error: {e}"
            )

        time.sleep(
            60
        )


# ============================================================
# HEALTH SERVER
# ============================================================

def health_server():

    try:

        from http.server import (
            HTTPServer,
            BaseHTTPRequestHandler
        )

        class HealthHandler(
            BaseHTTPRequestHandler
        ):

            def do_GET(self):

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
            f"Health server puerto {port}"
        )

        server.serve_forever()

    except Exception as e:

        log(
            f"Health server error: {e}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    log(
        "=============================================="
    )

    log(
        "     UNIVERSAL BINANCE FUTURES BOT V3"
    )

    log(
        "     RISK / LEVERAGE CORRECTED"
    )

    log(
        "=============================================="
    )

    log(
        f"LIVE_TRADING      = {LIVE_TRADING}"
    )

    log(
        f"USE_TESTNET       = {USE_TESTNET}"
    )

    log(
        f"LEVERAGE          = x{LEVERAGE}"
    )

    log(
        f"RISK_PER_TRADE    = "
        f"{RISK_PER_TRADE * 100:.2f}%"
    )

    log(
        f"MAX_MARGIN_USAGE  = "
        f"{MAX_MARGIN_USAGE * 100:.1f}%"
    )

    log(
        f"MAX_POSITIONS     = {MAX_POSITIONS}"
    )

    log(
        f"INTERVAL          = {INTERVAL}"
    )

    log(
        f"SYMBOLS           = {len(SYMBOLS)}"
    )

    log(
        "=============================================="
    )

    if not API_KEY or not API_SECRET:

        log(
            "FATAL: faltan "
            "BINANCE_API_KEY / BINANCE_API_SECRET"
        )

        return

    # --------------------------------------------------------
    # HEALTH
    # --------------------------------------------------------

    threading.Thread(
        target=health_server,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # REGLAS REALES
    # --------------------------------------------------------

    try:

        load_symbol_rules()

    except Exception as e:

        log(
            f"FATAL: no se pudieron cargar "
            f"las reglas Binance: {e}"
        )

        return

    # --------------------------------------------------------
    # VELAS INICIALES
    # --------------------------------------------------------

    try:

        load_initial_candles()

    except Exception as e:

        log(
            f"ERROR cargando historial: {e}"
        )

    # --------------------------------------------------------
    # LEVERAGE
    # --------------------------------------------------------

    configure_all_leverage()

    # --------------------------------------------------------
    # MARKET WS
    # --------------------------------------------------------

    for symbol in SYMBOLS:

        if symbol not in symbol_rules:

            continue

        threading.Thread(
            target=market_websocket_loop,
            args=(symbol,),
            daemon=True
        ).start()

        time.sleep(
            0.10
        )

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
        "=============================================="
    )

    log(
        "BOT INICIADO"
    )

    log(
        f"TRADING = "
        f"{'REAL' if LIVE_TRADING else 'TEST'}"
    )

    log(
        "Esperando velas cerradas..."
    )

    log(
        "=============================================="
    )

    while True:

        time.sleep(
            60
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        log(
            "Bot detenido manualmente"
        )

    except Exception as e:

        log(
            f"FATAL ERROR: {e}"
        )

        while True:

            time.sleep(
                60
            )
