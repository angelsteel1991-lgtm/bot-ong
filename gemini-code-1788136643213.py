import os
import time
import math
import json
import uuid
import hmac
import hashlib
import requests
from urllib.parse import urlencode
import threading
from datetime import datetime, timezone

import pandas as pd
import websocket


# ============================================================
# UNIVERSAL BINANCE FUTURES BOT V3
#
# CONEXION BASADA DIRECTAMENTE EN ONGUSDT BOT FUNCIONANDO
# ============================================================

LIVE_TRADING = True
USE_TESTNET = False

INTERVAL = "5m"

MAX_POSITIONS = 4
START_BALANCE = 100.0

RISK_PER_TRADE = 0.01

ATR_PERIOD = 14
ATR_STOP_MULT = 1.7

MIN_CANDLES = 40

RECONNECT_SECONDS = 60


# ------------------------------------------------------------
# PROTECCION DE GANANCIA
# ------------------------------------------------------------

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
# BINANCE
# ============================================================

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")


if USE_TESTNET:

    MARKET_WS_BASE = (
        "wss://stream.binancefuture.com/ws/"
    )

    WS_API_URL = (
        "wss://testnet.binancefuture.com/"
        "ws-fapi/v1"
    )

else:

    # EXACTAMENTE LA BASE USADA POR ONG

    MARKET_WS_BASE = (
        "wss://fstream.binance.com/market/ws/"
    )

    WS_API_URL = (
        "wss://ws-fapi.binance.com/"
        "ws-fapi/v1"
    )


# Reglas reales por simbolo

symbol_rules = {}


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
# ESTADO DE CADA SIMBOLO
# ============================================================

market_data = {}

state_lock = threading.Lock()


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
# POSICIONES PAPER
# ============================================================

positions = {}

paper_balance = START_BALANCE

trade_history = []


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
# IP PUBLICA
# ============================================================

def log_public_ip():

    try:

        import requests

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
# ATR
# ============================================================

def calculate_atr(
    df,
    period=ATR_PERIOD
):

    high = df["high"]

    low = df["low"]

    close = df["close"]

    previous_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - previous_close).abs(),
            (low - previous_close).abs(),
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

    rs = avg_gain / avg_loss.replace(
        0,
        1e-10
    )

    df["rsi"] = (
        100
        - (
            100 / (1 + rs)
        )
    )

    df["volume_ma"] = df["volume"].rolling(
        20
    ).mean()

    df["atr"] = calculate_atr(
        df
    )

    return df


# ============================================================
# SEÑAL
# ============================================================

def calculate_signal(symbol):

    with state_lock:

        candles = list(
            market_data[symbol]["candles"]
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

    if any(
        pd.isna(x)
        for x in [
            ema9,
            ema21,
            ema50,
            rsi,
            volume_ma,
        ]
    ):

        return None

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
        f"price={close:.6f} | "
        f"RSI={rsi:.1f} | "
        f"LONG={long_score} | "
        f"SHORT={short_score} | "
        f"signal={signal}"
    )

    return signal


# ============================================================
# RIESGO / BINANCE WS API
# ============================================================

#
# IMPORTANTE:
#
# - NO USA REST DE BINANCE.
#
# - USA LA MISMA WS API ORIGINAL:
#
# wss://ws-fapi.binance.com/ws-fapi/v1
#
# - Las órdenes reales se envían por esa misma conexión WS API.
#
# ============================================================

symbol_rules = {}


def _ws_signature(params):

    """
    Firma Binance WebSocket API:
    orden alfabético de parámetros,
    query string y HMAC SHA256.
    """

    if not API_SECRET:

        raise Exception(
            "Falta BINANCE_API_SECRET"
        )

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

    """
    Envía una petición por la MISMA conexión WS API usada
    para crear el User Data Stream.

    El lock evita que keepalive y órdenes hagan send/recv
    simultáneamente sobre el mismo socket.
    """

    global user_stream_control

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan BINANCE_API_KEY o BINANCE_API_SECRET"
        )

    with user_stream_control_lock:

        ws = user_stream_control

        if ws is None:

            raise Exception(
                "WS API todavía no está conectada"
            )

        request_params = dict(
            params or {}
        )

        if signed:

            request_params["apiKey"] = API_KEY

            request_params["timestamp"] = int(
                time.time() * 1000
            )

            request_params["recvWindow"] = 5000

            request_params["signature"] = _ws_signature(
                request_params
            )

        request = {
            "id": str(
                uuid.uuid4()
            ),
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

            # La WS API devuelve una respuesta por petición.
            # Si llega algo que no corresponde, seguimos esperando.

            if response.get(
                "id"
            ) == request["id"]:

                break

    if response.get(
        "status"
    ) != 200:

        raise Exception(
            f"{method} rechazado: {response}"
        )

    return response.get(
        "result"
    )


def load_symbol_rules_ws():

    """
    Carga reglas de símbolos.

    WS API Futures no expone exchangeInfo.
    Usa valores compatibles por defecto.
    """

    global symbol_rules

    rules = {}

    for symbol in SYMBOLS:

        rules[symbol] = {
            "step": 0.001,
            "min_qty": 0.001,
            "min_notional": 5.0,
        }

    symbol_rules = rules

    log(
        f"REGLAS BINANCE CARGADAS | "
        f"simbolos={len(symbol_rules)}"
    )


def get_usdt_balance():

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

    min_notional = rule["min_notional"]

    if step <= 0:

        return 0.0

    quantity = (
        math.floor(
            quantity / step
        )
        * step
    )

    if quantity < min_qty:

        quantity = min_qty

    if quantity * price < min_notional:

        quantity = (
            math.ceil(
                (min_notional / price) / step
            )
            * step
        )

    # Evita errores de representación flotante.

    decimals = max(
        0,
        int(
            round(
                -math.log10(step)
            )
        )
    ) if step < 1 else 0

    return float(
        f"{quantity:.{min(decimals, 12)}f}"
    )


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
        balance * RISK_PER_TRADE
    )

    raw_quantity = (
        risk_money / stop_distance
    )

    return normalize_quantity(
        symbol,
        raw_quantity,
        entry
    )


def place_exchange_stop(
    symbol,
    side,
    stop_price
):

    """
    STOP_MARKET real por la misma WS API.

    En USDⓈ-M actual, los conditional orders se envían
    mediante algoOrder.place.
    """

    close_side = (
        "SELL"
        if side == "LONG"
        else "BUY"
    )

    params = {
        "algoType": "CONDITIONAL",
        "symbol": symbol,
        "side": close_side,
        "type": "STOP_MARKET",
        "triggerPrice": f"{stop_price:.12f}",
        "closePosition": "true",
        "workingType": "MARK_PRICE",
        "priceProtect": "FALSE",
    }

    return ws_api_request(
        "algoOrder.place",
        params,
        signed=True
    )


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
            f"No se pudo cancelar stop "
            f"{order_id}: {e}"
        )


def update_exchange_stop(
    symbol,
    position
):

    stop = float(
        position["stop"]
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

        return

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

        position["stop_order_id"] = (
            result or {}
        ).get(
            "algoId"
        )

        position["exchange_stop_price"] = stop

        log(
            f"{symbol} | STOP REAL colocado | "
            f"stop={stop:.8f} | "
            f"algoId={position['stop_order_id']}"
        )

    except Exception as e:

        position["stop_order_id"] = None

        log(
            f"{symbol} | "
            f"ERROR colocando STOP REAL: {e}"
        )


def open_paper_position(
    symbol,
    side
):

    """
    Nombre conservado para no tocar la estructura original.
    En LIVE_TRADING=True ejecuta una orden REAL.
    """

    with state_lock:

        if (
            symbol in positions
            or len(positions) >= MAX_POSITIONS
        ):

            return

        price = market_data[symbol]["price"]

        atr = market_data[symbol]["atr"]

    if (
        price is None
        or atr is None
        or atr <= 0
    ):

        return

    stop_distance = (
        atr * ATR_STOP_MULT
    )

    try:

        quantity = calculate_position_size(
            symbol,
            price,
            stop_distance
        )

    except Exception as e:

        log(
            f"{symbol} | "
            f"No se pudo calcular cantidad real: {e}"
        )

        return

    if quantity <= 0:

        log(
            f"{symbol} | "
            f"No se abre: cantidad real no disponible"
        )

        return

    order_side = (
        "BUY"
        if side == "LONG"
        else "SELL"
    )

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
                quantity
            )
        )

        avg_price = float(
            result.get(
                "avgPrice",
                0
            ) or 0
        )

        if avg_price <= 0:

            avg_price = price

        if executed_qty <= 0:

            raise Exception(
                f"Orden sin cantidad ejecutada: {result}"
            )

        position = {
            "symbol": symbol,
            "side": side,
            "entry": avg_price,
            "quantity": executed_qty,
            "initial_risk": stop_distance,
            "stop": (
                avg_price - stop_distance
                if side == "LONG"
                else avg_price + stop_distance
            ),
            "highest": avg_price,
            "lowest": avg_price,
            "mfe_r": 0.0,
            "bars": 0,
            "opened_at": time.time(),
            "stop_order_id": None,
            "exchange_stop_price": None,
        }

        with state_lock:

            positions[symbol] = position

        update_exchange_stop(
            symbol,
            position
        )

        log(
            f"LIVE OPEN | {symbol} | {side} | "
            f"entry={avg_price:.6f} | "
            f"qty={executed_qty:.6f} | "
            f"risk={stop_distance:.6f}"
        )

    except Exception as e:

        log(
            f"ERROR ORDEN REAL ABRIENDO "
            f"{symbol}: {e}"
        )


def close_paper_position(
    symbol,
    price,
    reason
):

    """
    Nombre conservado para no tocar la estructura original.
    En LIVE_TRADING=True cierra la posición REAL.
    """

    with state_lock:

        position = positions.get(
            symbol
        )

        if position is None:

            return

        side = position["side"]

        entry = position["entry"]

        quantity = position["quantity"]

        stop_order_id = position.get(
            "stop_order_id"
        )

    close_side = (
        "SELL"
        if side == "LONG"
        else "BUY"
    )

    try:

        if stop_order_id:

            cancel_order(
                symbol,
                stop_order_id
            )

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
                quantity
            )
        )

        exit_price = float(
            result.get(
                "avgPrice",
                0
            ) or 0
        )

        if exit_price <= 0:

            exit_price = price

        pnl = (
            (
                exit_price - entry
            )
            * executed_qty
            if side == "LONG"
            else
            (
                entry - exit_price
            )
            * executed_qty
        )

        initial_risk = position[
            "initial_risk"
        ]

        r_multiple = (
            (
                (
                    exit_price - entry
                )
                / initial_risk
            )
            if side == "LONG"
            else
            (
                (
                    entry - exit_price
                )
                / initial_risk
            )
        ) if initial_risk > 0 else 0.0

        with state_lock:

            positions.pop(
                symbol,
                None
            )

        log(
            f"LIVE CLOSE | {symbol} | {side} | "
            f"entry={entry:.6f} | "
            f"exit={exit_price:.6f} | "
            f"PnL={pnl:+.4f} USDT | "
            f"R={r_multiple:+.2f} | "
            f"reason={reason}"
        )

    except Exception as e:

        log(
            f"ERROR ORDEN REAL CERRANDO "
            f"{symbol}: {e}"
        )


# ============================================================
# GESTION DE GANANCIA
# ============================================================

def manage_position(symbol):

    with state_lock:

        position = positions.get(
            symbol
        )

        if position is None:

            return

        price = market_data[symbol]["price"]

        atr = market_data[symbol]["atr"]

    if (
        price is None
        or atr is None
    ):

        return

    side = position["side"]

    entry = position["entry"]

    initial_risk = position[
        "initial_risk"
    ]

    if initial_risk <= 0:

        return

    # --------------------------------------------------------
    # ACTUALIZAR MAXIMO / MINIMO
    # --------------------------------------------------------

    with state_lock:

        position["bars"] += 1

        if side == "LONG":

            position["highest"] = max(
                position["highest"],
                price
            )

            mfe = (
                position["highest"]
                - entry
            ) / initial_risk

        else:

            position["lowest"] = min(
                position["lowest"],
                price
            )

            mfe = (
                entry
                - position["lowest"]
            ) / initial_risk

        position["mfe_r"] = max(
            position["mfe_r"],
            mfe
        )

    # --------------------------------------------------------
    # PROTECCION PROGRESIVA
    # --------------------------------------------------------

    new_stop = position["stop"]

    for trigger_r, protect_r in PROTECTION_LEVELS:

        if mfe >= trigger_r:

            if side == "LONG":

                candidate = (
                    entry
                    + protect_r * initial_risk
                )

                if candidate > new_stop:

                    new_stop = candidate

            else:

                candidate = (
                    entry
                    - protect_r * initial_risk
                )

                if candidate < new_stop:

                    new_stop = candidate

    # --------------------------------------------------------
    # TRAILING
    # --------------------------------------------------------

    for trigger_r, gap_r in TRAILING_LEVELS:

        if mfe >= trigger_r:

            if side == "LONG":

                candidate = (
                    position["highest"]
                    - gap_r * initial_risk
                )

                if candidate > new_stop:

                    new_stop = candidate

            else:

                candidate = (
                    position["lowest"]
                    + gap_r * initial_risk
                )

                if candidate < new_stop:

                    new_stop = candidate

    # --------------------------------------------------------
    # EL STOP JAMAS SE AFLOJA
    # --------------------------------------------------------

    old_stop = position["stop"]

    with state_lock:

        if side == "LONG":

            position["stop"] = max(
                position["stop"],
                new_stop
            )

        else:

            position["stop"] = min(
                position["stop"],
                new_stop
            )

        stop = position["stop"]

        bars = position["bars"]

        mfe_r = position["mfe_r"]

    # Si el stop protegido avanzó,
    # mover también el STOP REAL.

    if abs(
        stop - old_stop
    ) > max(
        abs(stop) * 0.000001,
        1e-12
    ):

        update_exchange_stop(
            symbol,
            position
        )

    # --------------------------------------------------------
    # STOP
    # --------------------------------------------------------

    if (
        side == "LONG"
        and price <= stop
    ):

        close_paper_position(
            symbol,
            price,
            f"PROTECTED STOP | MFE={mfe_r:.2f}R"
        )

        return

    if (
        side == "SHORT"
        and price >= stop
    ):

        close_paper_position(
            symbol,
            price,
            f"PROTECTED STOP | MFE={mfe_r:.2f}R"
        )

        return

    # --------------------------------------------------------
    # TIME STOP
    # --------------------------------------------------------

    if (
        bars >= TIME_STOP_CANDLES
        and mfe_r < TIME_STOP_MIN_MFE_R
    ):

        close_paper_position(
            symbol,
            price,
            f"TIME STOP | MFE={mfe_r:.2f}R"
        )


# ============================================================
# PROCESAR VELA
# ============================================================

def process_candle(symbol):

    with state_lock:

        candles = list(
            market_data[symbol]["candles"]
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

    atr = df["atr"].iloc[-1]

    if pd.isna(atr):

        return

    with state_lock:

        market_data[symbol]["atr"] = float(
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

    # --------------------------------------------------------
    # SI YA ESTA ABIERTA
    # --------------------------------------------------------

    if existing is not None:

        if existing["side"] != signal:

            price = market_data[symbol]["price"]

            if price is not None:

                close_paper_position(
                    symbol,
                    price,
                    "SEÑAL CONTRARIA"
                )

        return

    # --------------------------------------------------------
    # MAX 4
    # --------------------------------------------------------

    if total_positions >= MAX_POSITIONS:

        return

    open_paper_position(
        symbol,
        signal
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

        # Igual que ONG:
        # admite mensaje directo y combined.

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

            market_data[symbol]["price"] = price

        # ----------------------------------------------------
        # SOLO VELA CERRADA
        # ----------------------------------------------------

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

            candles = market_data[symbol]["candles"]

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

        process_candle(
            symbol
        )

    except Exception as e:

        log(
            f"{symbol} | Error market WS: {e}"
        )


# ============================================================
# MARKET WS CALLBACKS
# ============================================================

def make_market_open(symbol):

    def callback(ws):

        log(
            f"{symbol} | "
            f"MARKET WEBSOCKET CONECTADO"
        )

    return callback


def make_market_error(symbol):

    def callback(
        ws,
        error
    ):

        log(
            f"{symbol} | "
            f"Market WS error: {error}"
        )

    return callback


def make_market_close(symbol):

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
# MARKET WEBSOCKET
# ============================================================

#
# ESTA PARTE SIGUE EL PATRON DEL ONG:
#
# while True
#
# WebSocketApp
#
# run_forever(ping_interval=60, ping_timeout=20)
#
# log reconexion
#
# sleep(60)
#
# ============================================================

def market_websocket_loop(symbol):

    market_ws = (
        MARKET_WS_BASE
        + symbol.lower()
        + "@kline_"
        + INTERVAL
    )

    while True:

        try:

            log(
                f"{symbol} | "
                f"Conectando Market WebSocket..."
            )

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

            # EXACTAMENTE EL PATRON DEL ONG

            ws.run_forever(
                ping_interval=60,
                ping_timeout=20
            )

        except Exception as e:

            log(
                f"{symbol} | "
                f"Market WS exception: {e}"
            )

        # ----------------------------------------------------
        # RECONEXION CADA 60 SEGUNDOS
        # ----------------------------------------------------

        log(
            f"{symbol} | "
            f"Reconexión Market WS en 60 segundos..."
        )

        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# USER DATA STREAM
# ============================================================

user_stream_control = None

user_stream_control_lock = threading.Lock()

listen_key = None


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
            f"UserDataStream.start rechazado: "
            f"{response}"
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
        "USER DATA STREAM CREADO POR WS API"
    )

    log(
        "ListenKey recibido correctamente"
    )

    return listen_key


# ============================================================
# USER DATA CALLBACKS
# ============================================================

def on_user_open(ws):

    log(
        "USER DATA WEBSOCKET CONECTADO"
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
        f"User WS cerrado: {code} {msg}"
    )


def sync_account_positions(data):

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

        with state_lock:

            if amount == 0:

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

            atr = market_data[symbol].get(
                "atr"
            )

            if (
                atr is None
                or atr <= 0
            ):

                continue

            risk = (
                atr * ATR_STOP_MULT
            )

            old = positions.get(
                symbol
            )

            stop = (
                old.get("stop")
                if old
                and old.get("side") == side
                else
                (
                    entry - risk
                    if side == "LONG"
                    else entry + risk
                )
            )

            positions[symbol] = {
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "quantity": qty,
                "initial_risk": risk,
                "stop": stop,
                "highest": max(
                    entry,
                    market_data[symbol].get(
                        "price"
                    ) or entry
                ),
                "lowest": min(
                    entry,
                    market_data[symbol].get(
                        "price"
                    ) or entry
                ),
                "mfe_r": (
                    old.get(
                        "mfe_r",
                        0.0
                    )
                    if old
                    else 0.0
                ),
                "bars": (
                    old.get(
                        "bars",
                        0
                    )
                    if old
                    else 0
                ),
                "opened_at": (
                    old.get(
                        "opened_at",
                        time.time()
                    )
                    if old
                    else time.time()
                ),
                "stop_order_id": (
                    old.get(
                        "stop_order_id"
                    )
                    if old
                    else None
                ),
                "exchange_stop_price": (
                    old.get(
                        "exchange_stop_price"
                    )
                    if old
                    else None
                ),
            }

        log(
            f"ACCOUNT SYNC | "
            f"{symbol} | "
            f"{side} | "
            f"qty={qty} | "
            f"entry={entry}"
        )


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
                "ListenKey expirado"
            )

        elif event_type == "ACCOUNT_UPDATE":

            sync_account_positions(
                data
            )

            log(
                "ACCOUNT UPDATE recibido"
            )

        elif event_type == "ORDER_TRADE_UPDATE":

            log(
                "ORDER TRADE UPDATE recibido"
            )

    except Exception as e:

        log(
            f"Error User WS: {e}"
        )


# ============================================================
# USER DATA WEBSOCKET
# ============================================================

#
# MISMO PATRON DEL ONG
#
# REINTENTO CADA 60 SEGUNDOS
#
# ============================================================

def user_websocket_loop():

    global listen_key
    global user_stream_control

    while True:

        stream_ws = None

        try:

            key = start_user_data_stream()

            try:

                load_symbol_rules_ws()

            except Exception as e:

                log(
                    f"REGLAS BINANCE WS: {e}"
                )

            ws_url = (
                "wss://fstream.binance.com/private/ws?"
                "listenKey="
                + key
                + "&events="
                "ORDER_TRADE_UPDATE/"
                "ACCOUNT_UPDATE"
            )

            if USE_TESTNET:

                ws_url = (
                    "wss://stream.binancefuture.com/ws/"
                    + key
                )

            log(
                "Conectando User Data WebSocket..."
            )

            stream_ws = websocket.WebSocketApp(
                ws_url,

                on_open=on_user_open,

                on_message=on_user_message,

                on_error=on_user_error,

                on_close=on_user_close,
            )

            # EXACTAMENTE COMO ONG

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

        # ----------------------------------------------------
        # RECONEXION CADA 60 SEGUNDOS
        # ----------------------------------------------------

        log(
            "Reconexión User Data en 60 segundos..."
        )

        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# USER DATA KEEPALIVE
# ============================================================

def user_stream_keepalive_loop():

    global user_stream_control
    global listen_key

    while True:

        # Igual que ONG:
        # mantener vivo el User Data Stream.

        time.sleep(
            45 * 60
        )

        try:

            with user_stream_control_lock:

                ws = user_stream_control

            if ws is None:

                log(
                    "Keepalive: no hay conexión WS API"
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
                },
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
                    "USER DATA STREAM KEEPALIVE OK"
                )

            else:

                log(
                    "USER DATA KEEPALIVE "
                    f"RESPUESTA: {response}"
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
# POSITION MANAGER
# ============================================================

def position_manager_loop():

    while True:

        try:

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
                    f"STATUS | "
                    f"Error balance real: {e}"
                )

            log(
                f"STATUS | "
                f"USDT_available="
                f"{balance if balance is not None else 'N/A'} | "
                f"positions={active}/{MAX_POSITIONS}"
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
            f"Health server escuchando "
            f"en puerto {port}"
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
        "=========================================="
    )

    log(
        "       UNIVERSAL FUTURES BOT V3"
    )

    log(
        "       LIVE TRADING REAL"
    )

    log(
        "=========================================="
    )

    log(
        f"LIVE_TRADING = {LIVE_TRADING}"
    )

    log(
        f"USE_TESTNET = {USE_TESTNET}"
    )

    log(
        f"SYMBOLS = {len(SYMBOLS)}"
    )

    log(
        f"MAX_POSITIONS = {MAX_POSITIONS}"
    )

    log(
        f"INTERVAL = {INTERVAL}"
    )

    log(
        "CONEXION WS BASADA EN ONG FUNCIONANDO"
    )

    log(
        "SIN REST DE BINANCE | ORDENES REALES POR WS API"
    )

    log(
        "RECONEXION WS = 60 SEGUNDOS"
    )

    log(
        "BINANCE REAL MARKET"
    )

    log(
        "=========================================="
    )

    log_public_ip()

    # --------------------------------------------------------
    # CREDENCIALES
    # --------------------------------------------------------

    if not API_KEY or not API_SECRET:

        log(
            "FATAL ERROR: "
            "Faltan BINANCE_API_KEY "
            "o BINANCE_API_SECRET"
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
    # MARKET WEBSOCKETS
    # --------------------------------------------------------
    #
    # Un WS por símbolo.
    #
    # Cada uno:
    # ping_interval=60
    # ping_timeout=20
    # reconexion=60 segundos
    #
    # --------------------------------------------------------

    for symbol in SYMBOLS:

        threading.Thread(
            target=market_websocket_loop,
            args=(symbol,),
            daemon=True
        ).start()

        time.sleep(
            0.10
        )

    # --------------------------------------------------------
    # USER DATA
    # --------------------------------------------------------

    threading.Thread(
        target=user_websocket_loop,
        daemon=True
    ).start()

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
        "Esperando datos de mercado..."
    )

    # --------------------------------------------------------
    # MAIN LOOP
    # --------------------------------------------------------

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

        # No cerrar inmediatamente.
        # Deja visible el error en Railway.

        while True:

            time.sleep(
                60
            )
