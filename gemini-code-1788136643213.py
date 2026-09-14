import os
import time
import math
import json
import uuid
import hmac
import hashlib
import urllib.request
from urllib.parse import urlencode
import threading
from datetime import datetime, timezone

import pandas as pd
import websocket


# ============================================================
# BOT LINKUSDT - BINANCE USD-M FUTURES
#
# ESTRATEGIA:
#
#   EMA 9 / EMA 21
#   DONCHIAN 20
#   ATR14
#   TAKER PRESSURE
#   MOMENTUM
#   VOLUMEN COMO CONFIRMACION SUAVE
#   ANTI-ENTRADA TARDIA
#
# FILOSOFIA:
#
#   ENTRADA PERMISIVA
#   SALIDA ESTRICTA
#   DEJAR CORRER LAS GANADORAS
#
# ============================================================
#
# RIESGO:
#
#   MARGEN MAXIMO = 25% DEL BALANCE
#   LEVERAGE = x6
#   STOP INICIAL = 1.8 x ATR14
#
#   El tamaño se calcula principalmente desde
#   el margen disponible.
#
# ============================================================
#
# GESTION:
#
#   STOP LOCAL ATR
#   TRAILING ATR
#   EMERGENCY LOSS CAP
#   SIN STOP ALGO EN BINANCE
#   SALIDA MARKET reduceOnly
#
# ============================================================
#
# BINANCE:
#   - CERO REST PARA TRADING
#   - Market WebSocket
#   - WebSocket API para balance
#   - WebSocket API para ordenes
#   - User Data Stream por WebSocket
#
# KUCOIN:
#   - SOLO historial inicial
#   - NO ejecuta operaciones
#
# ============================================================


LIVE_TRADING = True
USE_TESTNET = False

INTERVAL = "5m"

MAX_POSITIONS = 1

LEVERAGE = 6


# ============================================================
# MARGEN
# ============================================================

MAX_MARGIN_PERCENT = 0.25

BUDGET_SPLIT = MAX_POSITIONS


# ============================================================
# HISTORIAL
# ============================================================

MIN_CANDLES = 80

ATR_PERIOD = 14

DONCHIAN_PERIOD = 20

EMA_FAST = 9
EMA_SLOW = 21

VOLUME_PERIOD = 20

TAKER_PRESSURE_LOOKBACK = 3

RECONNECT_SECONDS = 60


# ============================================================
# TAKER PRESSURE
#
# No necesitamos 0.55 / 0.45 tan rígido.
#
# La entrada es permisiva.
#
# ============================================================

TAKER_LONG_THRESHOLD = 0.52
TAKER_SHORT_THRESHOLD = 0.48


# ============================================================
# DONCHIAN / ENTRADA ANTICIPADA
#
# Permitimos entrar cerca del breakout.
#
# Ejemplo LONG:
#
# Donchian High = 12.00
# ATR = 0.10
#
# zona anticipada:
#
# 12.00 - 0.20 ATR
# = 11.98
#
# ============================================================

DONCHIAN_PREBREAK_ATR = 0.20

MAX_ENTRY_DISTANCE_ATR = 0.45


# ============================================================
# MOMENTUM
# ============================================================

MOMENTUM_LOOKBACK = 2

MIN_MOMENTUM_ATR = 0.05


# ============================================================
# ATR STOP
# ============================================================

ATR_STOP_MULTIPLIER = 1.80


# ============================================================
# TRAILING
#
# No TP fijo.
#
# Primero dejamos que la operación demuestre
# que realmente funciona.
#
# ============================================================

TRAILING_START_R = 1.00

TRAILING_ATR_MULTIPLIER = 1.35


# ============================================================
# EMERGENCY LOSS
#
# El stop ATR es la protección principal.
#
# Este límite solamente evita una discrepancia extrema.
# ============================================================

EMERGENCY_LOSS_R_MULTIPLIER = 2.20


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
        "wss://testnet.binancefuture.com/ws-fapi/v1"
    )

    USER_STREAM_BASE = (
        "wss://stream.binancefuture.com/ws/"
    )

else:

    MARKET_WS_BASE = (
        "wss://fstream.binance.com/market/ws/"
    )

    WS_API_URL = (
        "wss://ws-fapi.binance.com/ws-fapi/v1"
    )

    USER_STREAM_BASE = (
        "wss://fstream.binance.com/private/ws/"
    )


# ============================================================
# KUCOIN
# ============================================================

KUCOIN_BASE = "https://api.kucoin.com"

KUCOIN_KLINE_URL = (
    f"{KUCOIN_BASE}/api/v1/market/candles"
)

KUCOIN_SYMBOL = "LINK-USDT"

KUCOIN_INTERVAL = "5min"


# ============================================================
# SIMBOLO
# ============================================================

SYMBOLS = [
    "LINKUSDT",
]


# ============================================================
# REGLAS LINK
#
# LINKUSDT:
#   tick = 0.001
#   minQty = 0.1
#   minNotional = 20 USDT
#
# Se mantienen locales para conservar CERO REST.
# ============================================================

SYMBOL_RULES = {

    "LINKUSDT": {

        "step": 0.1,

        "min_qty": 0.1,

        "max_qty": None,

        "min_notional": 20.0,

        "tick": 0.001,

    }

}


# ============================================================
# ESTADO
# ============================================================

market_data = {}

state_lock = threading.Lock()


def create_symbol_state():

    return {

        "candles": [],

        "price": None,

        "last_signal": None,

        "last_candle_time": None,

    }


for symbol in SYMBOLS:

    market_data[symbol] = create_symbol_state()


# ============================================================
# POSICIONES
# ============================================================

positions = {}

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

        with urllib.request.urlopen(
            "https://api.ipify.org?format=json",
            timeout=10
        ) as response:

            data = json.loads(
                response.read().decode()
            )

        ip = data.get(
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
# KUCOIN HISTORICO
# ============================================================

def load_initial_history_from_kucoin(symbol):

    if symbol != "LINKUSDT":

        raise Exception(
            f"No existe mapeo KuCoin para {symbol}"
        )

    log(
        f"{symbol} | Cargando historial inicial "
        f"desde KuCoin SPOT: {KUCOIN_SYMBOL}"
    )

    params = {

        "symbol": KUCOIN_SYMBOL,

        "type": KUCOIN_INTERVAL,

    }

    query = urlencode(params)

    url = (
        f"{KUCOIN_KLINE_URL}?{query}"
    )

    request = urllib.request.Request(

        url,

        headers={
            "User-Agent": "Mozilla/5.0"
        }

    )

    with urllib.request.urlopen(
        request,
        timeout=20
    ) as response:

        raw = response.read().decode()

    data = json.loads(raw)

    if not isinstance(data, dict):

        raise Exception(
            "Respuesta KuCoin invalida"
        )

    rows = data.get("data")

    if not isinstance(rows, list) or not rows:

        raise Exception(
            f"KuCoin no devolvio velas Spot: {data}"
        )

    candles = []

    for row in rows:

        try:

            if not isinstance(
                row,
                (list, tuple)
            ):

                continue

            if len(row) < 6:

                continue

            timestamp = int(
                float(row[0])
            )

            if timestamp < 10_000_000_000:

                timestamp *= 1000

            open_price = float(row[1])

            close_price = float(row[2])

            high_price = float(row[3])

            low_price = float(row[4])

            volume = float(row[5])

            if (

                open_price <= 0
                or high_price <= 0
                or low_price <= 0
                or close_price <= 0
                or volume < 0

            ):

                continue

            candles.append(

                [

                    timestamp,

                    open_price,

                    high_price,

                    low_price,

                    close_price,

                    volume,

                    None,

                ]

            )

        except Exception:

            continue

    if not candles:

        raise Exception(
            "No se pudieron convertir velas "
            "de KuCoin Spot"
        )

    unique = {}

    for candle in candles:

        unique[candle[0]] = candle

    candles = list(
        unique.values()
    )

    candles.sort(
        key=lambda x: x[0]
    )

    now_ms = int(
        time.time() * 1000
    )

    interval_ms = (
        5 * 60 * 1000
    )

    closed_candles = []

    for candle in candles:

        candle_open = candle[0]

        candle_close = (
            candle_open
            + interval_ms
        )

        if candle_close <= now_ms:

            closed_candles.append(
                candle
            )

    if len(closed_candles) < MIN_CANDLES:

        raise Exception(
            f"KuCoin Spot devolvio solo "
            f"{len(closed_candles)} velas cerradas; "
            f"se necesitan {MIN_CANDLES}"
        )

    closed_candles = closed_candles[
        -MIN_CANDLES:
    ]

    with state_lock:

        market_data[symbol]["candles"] = (
            closed_candles
        )

        market_data[symbol][
            "last_candle_time"
        ] = closed_candles[-1][0]

        market_data[symbol]["price"] = (
            closed_candles[-1][4]
        )

    log(
        f"{symbol} | HISTORIAL KUCOIN SPOT OK | "
        f"symbol={KUCOIN_SYMBOL} | "
        f"interval={KUCOIN_INTERVAL} | "
        f"velas={len(closed_candles)}"
    )

    log(
        f"{symbol} | "
        f"Las nuevas velas vienen "
        f"EXCLUSIVAMENTE de Binance Market WebSocket"
    )

    log(
        f"{symbol} | "
        f"Esperando TAKER PRESSURE real "
        f"de Binance"
    )


# ============================================================
# INDICADORES
# ============================================================

def calculate_indicators(df):

    df = df.copy()

    # --------------------------------------------------------
    # TRUE RANGE
    # --------------------------------------------------------

    previous_close = (
        df["close"].shift(1)
    )

    tr1 = (
        df["high"]
        - df["low"]
    )

    tr2 = (
        df["high"]
        - previous_close
    ).abs()

    tr3 = (
        df["low"]
        - previous_close
    ).abs()

    df["tr"] = pd.concat(

        [
            tr1,
            tr2,
            tr3
        ],

        axis=1

    ).max(axis=1)

    # --------------------------------------------------------
    # ATR14
    # --------------------------------------------------------

    df["atr14"] = (

        df["tr"]
        .rolling(
            ATR_PERIOD
        )
        .mean()

    )

    # --------------------------------------------------------
    # EMA
    # --------------------------------------------------------

    df["ema9"] = (

        df["close"]
        .ewm(
            span=EMA_FAST,
            adjust=False
        )
        .mean()

    )

    df["ema21"] = (

        df["close"]
        .ewm(
            span=EMA_SLOW,
            adjust=False
        )
        .mean()

    )

    # --------------------------------------------------------
    # DONCHIAN
    #
    # Se excluye la vela actual.
    # --------------------------------------------------------

    df["donchian_high"] = (

        df["high"]
        .shift(1)
        .rolling(
            DONCHIAN_PERIOD
        )
        .max()

    )

    df["donchian_low"] = (

        df["low"]
        .shift(1)
        .rolling(
            DONCHIAN_PERIOD
        )
        .min()

    )

    # --------------------------------------------------------
    # VOLUMEN
    # --------------------------------------------------------

    df["volume_ma"] = (

        df["volume"]
        .rolling(
            VOLUME_PERIOD
        )
        .mean()

    )

    # --------------------------------------------------------
    # TAKER BUY RATIO
    # --------------------------------------------------------

    df["taker_ratio"] = (

        df["taker_buy_volume"]
        /
        df["volume"].replace(
            0,
            float("nan")
        )

    )

    return df


# ============================================================
# TAKER PRESSURE
# ============================================================

def get_taker_pressure(df):

    recent = (

        df["taker_ratio"]
        .iloc[
            -TAKER_PRESSURE_LOOKBACK:
        ]

    )

    recent = recent.dropna()

    if len(recent) < TAKER_PRESSURE_LOOKBACK:

        return None

    return float(
        recent.mean()
    )


# ============================================================
# SEÑAL
#
# ENTRADA PERMISIVA
#
# LONG:
#
#   EMA9 > EMA21
#   precio cerca del Donchian High
#   momentum positivo
#   taker comprador
#
# No necesitamos:
#
#   ruptura completa
#   volumen gigante
#   RSI perfecto
#   6/6
#
# ============================================================

def calculate_signal(symbol):

    with state_lock:

        candles = list(
            market_data[symbol]["candles"]
        )

    minimum_needed = max(

        DONCHIAN_PERIOD + 5,

        EMA_SLOW + 5,

        ATR_PERIOD + 5,

        VOLUME_PERIOD + 5

    )

    if len(candles) < minimum_needed:

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
            "taker_buy_volume",

        ]

    )

    df = calculate_indicators(df)

    last = df.iloc[-1]

    previous = df.iloc[-2]

    close = float(
        last["close"]
    )

    previous_close = float(
        previous["close"]
    )

    atr = float(
        last["atr14"]
    )

    ema9 = float(
        last["ema9"]
    )

    ema21 = float(
        last["ema21"]
    )

    donchian_high = float(
        last["donchian_high"]
    )

    donchian_low = float(
        last["donchian_low"]
    )

    volume = float(
        last["volume"]
    )

    volume_ma = float(
        last["volume_ma"]
    )

    taker_pressure = (
        get_taker_pressure(df)
    )

    if any(

        pd.isna(x)

        for x in [

            atr,
            ema9,
            ema21,
            donchian_high,
            donchian_low,
            volume_ma,

        ]

    ):

        return None

    if atr <= 0:

        return None

    if taker_pressure is None:

        log(
            f"{symbol} | "
            f"ESPERANDO TAKER PRESSURE"
        )

        return None

    # ========================================================
    # MOMENTUM
    # ========================================================

    momentum_price = (

        close
        - float(
            df["close"].iloc[
                -1 - MOMENTUM_LOOKBACK
            ]
        )

    )

    momentum_atr = (
        momentum_price
        / atr
    )

    bullish_momentum = (

        momentum_price > 0

        and

        momentum_atr >= MIN_MOMENTUM_ATR

    )

    bearish_momentum = (

        momentum_price < 0

        and

        abs(momentum_atr)
        >= MIN_MOMENTUM_ATR

    )

    # ========================================================
    # DIRECCION EMA
    # ========================================================

    bullish_trend = (
        ema9 > ema21
    )

    bearish_trend = (
        ema9 < ema21
    )

    # ========================================================
    # DISTANCIA AL DONCHIAN
    # ========================================================

    long_distance = (

        (
            donchian_high
            - close
        )
        / atr

    )

    short_distance = (

        (
            close
            - donchian_low
        )
        / atr

    )

    # ========================================================
    # ZONA DE ENTRADA
    #
    # Podemos entrar:
    #
    # 1. antes de romper
    # 2. justo rompiendo
    # 3. poco despues
    #
    # Pero no perseguimos el movimiento.
    # ========================================================

    long_near_breakout = (

        close
        >=
        (
            donchian_high
            -
            atr
            * DONCHIAN_PREBREAK_ATR
        )

    )

    short_near_breakout = (

        close
        <=
        (
            donchian_low
            +
            atr
            * DONCHIAN_PREBREAK_ATR
        )

    )

    long_not_late = (

        close
        <=
        (
            donchian_high
            +
            atr
            * MAX_ENTRY_DISTANCE_ATR
        )

    )

    short_not_late = (

        close
        >=
        (
            donchian_low
            -
            atr
            * MAX_ENTRY_DISTANCE_ATR
        )

    )

    # ========================================================
    # TAKER
    # ========================================================

    long_pressure = (

        taker_pressure
        >=
        TAKER_LONG_THRESHOLD

    )

    short_pressure = (

        taker_pressure
        <=
        TAKER_SHORT_THRESHOLD

    )

    # ========================================================
    # VOLUMEN SUAVE
    #
    # No exigimos volumen > media.
    #
    # Solamente bloqueamos volumen extremadamente muerto.
    # ========================================================

    volume_alive = (

        volume
        >=
        volume_ma * 0.65

    )

    # ========================================================
    # LONG
    # ========================================================

    long_valid = (

        bullish_trend

        and

        bullish_momentum

        and

        long_pressure

        and

        long_near_breakout

        and

        long_not_late

        and

        volume_alive

    )

    # ========================================================
    # SHORT
    # ========================================================

    short_valid = (

        bearish_trend

        and

        bearish_momentum

        and

        short_pressure

        and

        short_near_breakout

        and

        short_not_late

        and

        volume_alive

    )

    # ========================================================
    # LOG
    # ========================================================

    log(

        f"{symbol} | "
        f"SETUP | "
        f"price={close:.4f} | "
        f"EMA9={ema9:.4f} | "
        f"EMA21={ema21:.4f} | "
        f"DC_H={donchian_high:.4f} | "
        f"DC_L={donchian_low:.4f} | "
        f"ATR={atr:.4f} | "
        f"TAKER={taker_pressure:.3f} | "
        f"MOM={momentum_atr:+.2f}ATR | "
        f"VOL={volume/volume_ma:.2f}x | "
        f"LONG_NEAR={long_near_breakout} | "
        f"SHORT_NEAR={short_near_breakout}"
    )

    # ========================================================
    # LONG
    # ========================================================

    if long_valid:

        log(

            f"{symbol} | "
            f"SEÑAL LONG | "
            f"EMA9>EMA21 | "
            f"MOM={momentum_atr:+.2f}ATR | "
            f"TAKER={taker_pressure:.3f} | "
            f"DC_DIST={long_distance:+.2f}ATR"

        )

        return {

            "side": "LONG",

            "atr": atr,

            "breakout": donchian_high,

            "taker_pressure": taker_pressure,

            "momentum_atr": momentum_atr,

        }

    # ========================================================
    # SHORT
    # ========================================================

    if short_valid:

        log(

            f"{symbol} | "
            f"SEÑAL SHORT | "
            f"EMA9<EMA21 | "
            f"MOM={momentum_atr:+.2f}ATR | "
            f"TAKER={taker_pressure:.3f} | "
            f"DC_DIST={short_distance:+.2f}ATR"

        )

        return {

            "side": "SHORT",

            "atr": atr,

            "breakout": donchian_low,

            "taker_pressure": taker_pressure,

            "momentum_atr": momentum_atr,

        }

    return None


# ============================================================
# BINANCE WS API
# ============================================================

user_stream_control = None

user_stream_control_lock = (
    threading.Lock()
)

listen_key = None

ws_api_ready = threading.Event()


# ============================================================
# FIRMA WS API
# ============================================================

def _ws_signature(params):

    if not API_SECRET:

        raise Exception(
            "Falta BINANCE_API_SECRET"
        )

    items = []

    for key in sorted(params):

        if key == "signature":

            continue

        value = params[key]

        if isinstance(value, bool):

            value = (
                "true"
                if value
                else "false"
            )

        else:

            value = str(value)

        items.append(
            f"{key}={value}"
        )

    payload = "&".join(items)

    return hmac.new(

        API_SECRET.encode("utf-8"),

        payload.encode("utf-8"),

        hashlib.sha256

    ).hexdigest()


# ============================================================
# WS API REQUEST
# ============================================================

def ws_api_request(

    method,
    params=None,
    signed=True,
    timeout=15

):

    global user_stream_control

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan BINANCE_API_KEY "
            "o BINANCE_API_SECRET"
        )

    with user_stream_control_lock:

        ws = user_stream_control

        if ws is None:

            raise Exception(
                "WS API todavia no esta conectada"
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

            request_params["signature"] = (
                _ws_signature(
                    request_params
                )
            )

        request = {

            "id": str(
                uuid.uuid4()
            ),

            "method": method,

            "params": request_params,

        }

        ws.settimeout(timeout)

        ws.send(
            json.dumps(request)
        )

        while True:

            raw = ws.recv()

            response = json.loads(raw)

            if (
                response.get("id")
                == request["id"]
            ):

                break

    if response.get("status") != 200:

        raise Exception(
            f"{method} rechazado: "
            f"{response}"
        )

    return response.get("result")


# ============================================================
# BALANCE
# ============================================================

def get_usdt_balance():

    result = ws_api_request(

        "v2/account.balance",
        {},
        signed=True

    )

    for item in (
        result or []
    ):

        if item.get("asset") == "USDT":

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
# NORMALIZAR CANTIDAD
# ============================================================

def normalize_quantity(

    symbol,
    quantity,
    price

):

    rule = SYMBOL_RULES.get(symbol)

    if (
        not rule
        or price <= 0
        or quantity <= 0
    ):

        return 0.0

    step = rule["step"]

    min_qty = rule["min_qty"]

    max_qty = rule["max_qty"]

    min_notional = rule["min_notional"]

    quantity = (

        math.floor(
            (
                quantity
                + 1e-15
            )
            / step
        )
        * step

    )

    if quantity < min_qty:

        return 0.0

    if (
        max_qty is not None
        and quantity > max_qty
    ):

        quantity = max_qty

    notional = (
        quantity * price
    )

    if (
        min_notional > 0
        and notional < min_notional
    ):

        return 0.0

    decimals = max(

        0,

        int(
            round(
                -math.log10(step)
            )
        )

    )

    decimals = min(
        decimals,
        12
    )

    return float(
        f"{quantity:.{decimals}f}"
    )


# ============================================================
# NORMALIZAR PRECIO
# ============================================================

def normalize_price(

    symbol,
    price

):

    rule = SYMBOL_RULES.get(symbol)

    if not rule:

        return price

    tick = rule.get(
        "tick",
        0.001
    )

    if tick <= 0:

        return price

    price = (

        math.floor(
            (
                price
                + tick * 1e-9
            )
            / tick
        )
        * tick

    )

    decimals = max(

        0,

        int(
            round(
                -math.log10(tick)
            )
        )

    )

    decimals = min(
        decimals,
        12
    )

    return float(
        f"{price:.{decimals}f}"
    )


# ============================================================
# TAMAÑO
#
# 25% DEL BALANCE COMO MARGEN MAXIMO.
#
# No usamos riesgo fijo de US$0.15.
#
# La volatilidad controla el STOP.
#
# ============================================================

def calculate_position_size(

    symbol,
    entry,
    atr

):

    if entry <= 0:

        return 0, 0.0, 0.0, 0.0

    if atr <= 0:

        log(
            f"{symbol} | "
            f"NO SE ABRE | ATR invalido"
        )

        return 0, 0.0, 0.0, 0.0

    balance = get_usdt_balance()

    if balance <= 0:

        return 0, 0.0, 0.0, 0.0

    # --------------------------------------------------------
    # MARGEN MAXIMO
    # --------------------------------------------------------

    max_margin = (

        balance
        * MAX_MARGIN_PERCENT
        / BUDGET_SPLIT

    )

    # --------------------------------------------------------
    # NOTIONAL MAXIMO
    # --------------------------------------------------------

    max_notional = (

        max_margin
        * LEVERAGE

    )

    # --------------------------------------------------------
    # CANTIDAD
    # --------------------------------------------------------

    raw_quantity = (

        max_notional
        / entry

    )

    quantity = normalize_quantity(

        symbol,

        raw_quantity,

        entry

    )

    if quantity <= 0:

        log(

            f"{symbol} | "
            f"NO SE ABRE | "
            f"25% MARGIN PRODUCE "
            f"NOTIONAL INSUFICIENTE | "
            f"balance={balance:.6f} | "
            f"max_margin={max_margin:.6f} | "
            f"max_notional={max_notional:.6f}"

        )

        return 0, 0.0, 0.0, 0.0

    notional = (

        quantity
        * entry

    )

    estimated_margin = (

        notional
        / LEVERAGE

    )

    # --------------------------------------------------------
    # STOP
    # --------------------------------------------------------

    stop_distance = (

        atr
        * ATR_STOP_MULTIPLIER

    )

    if stop_distance <= 0:

        return 0, 0.0, 0.0, 0.0

    estimated_loss = (

        stop_distance
        * quantity

    )

    log(

        f"PRE-ORDER {symbol} | "
        f"BALANCE={balance:.6f} | "
        f"ENTRY={entry:.4f} | "
        f"ATR={atr:.5f} | "
        f"STOP_DIST={stop_distance:.5f} | "
        f"MAX_MARGIN={max_margin:.6f} | "
        f"EST_MARGIN={estimated_margin:.6f} | "
        f"QTY={quantity:.2f} | "
        f"NOTIONAL={notional:.6f} | "
        f"STOP_RISK≈-{estimated_loss:.6f}"

    )

    return (

        quantity,

        stop_distance,

        estimated_loss,

        max_margin

    )


# ============================================================
# ABRIR POSICION
# ============================================================

def open_position(

    symbol,
    signal

):

    if not ws_api_ready.is_set():

        log(
            f"{symbol} | "
            f"Señal ignorada: "
            f"WS API no esta lista"
        )

        return

    side = signal["side"]

    atr = float(
        signal["atr"]
    )

    breakout = float(
        signal["breakout"]
    )

    with state_lock:

        if (

            symbol in positions

            or len(positions)
            >= MAX_POSITIONS

        ):

            return

        price = market_data[
            symbol
        ]["price"]

    if price is None or price <= 0:

        return

    # ========================================================
    # ANTI-TARDANZA EN TIEMPO REAL
    # ========================================================

    if side == "LONG":

        live_distance = (

            (
                price
                - breakout
            )
            / atr

        )

    else:

        live_distance = (

            (
                breakout
                - price
            )
            / atr

        )

    if live_distance > MAX_ENTRY_DISTANCE_ATR:

        log(

            f"{symbol} | "
            f"ENTRADA CANCELADA | "
            f"MOVIMIENTO YA EXTENDIDO | "
            f"{live_distance:.2f} ATR"

        )

        return

    # --------------------------------------------------------
    # No permitimos que el precio se haya dado vuelta
    # demasiado antes del MARKET.
    # --------------------------------------------------------

    if side == "LONG":

        if price < breakout - (
            atr
            * DONCHIAN_PREBREAK_ATR
        ):

            log(
                f"{symbol} | "
                f"LONG CANCELADO | "
                f"precio abandono zona Donchian"
            )

            return

    else:

        if price > breakout + (
            atr
            * DONCHIAN_PREBREAK_ATR
        ):

            log(
                f"{symbol} | "
                f"SHORT CANCELADO | "
                f"precio abandono zona Donchian"
            )

            return

    try:

        (
            quantity,
            stop_distance,
            estimated_loss,
            max_margin

        ) = calculate_position_size(

            symbol,
            price,
            atr

        )

    except Exception as e:

        log(
            f"{symbol} | "
            f"No se pudo calcular cantidad: {e}"
        )

        return

    if quantity <= 0:

        return

    order_side = (

        "BUY"

        if side == "LONG"

        else

        "SELL"

    )

    try:

        result = ws_api_request(

            "order.place",

            {

                "symbol": symbol,

                "side": order_side,

                "type": "MARKET",

                "quantity": str(
                    quantity
                ),

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

            or quantity

        )

        avg_price = float(

            result.get(
                "avgPrice",
                0
            )

            or 0

        )

        if avg_price <= 0:

            avg_price = price

        if executed_qty <= 0:

            raise Exception(
                f"Orden sin cantidad ejecutada: "
                f"{result}"
            )

        # ----------------------------------------------------
        # STOP INICIAL
        # ----------------------------------------------------

        if side == "LONG":

            stop_price = (

                avg_price
                - stop_distance

            )

        else:

            stop_price = (

                avg_price
                + stop_distance

            )

        stop_price = normalize_price(

            symbol,
            stop_price

        )

        # ----------------------------------------------------
        # POSITION
        # ----------------------------------------------------

        position = {

            "symbol": symbol,

            "side": side,

            "entry": avg_price,

            "quantity": executed_qty,

            "atr": atr,

            "initial_atr": atr,

            "stop_distance": stop_distance,

            "stop_price": stop_price,

            "highest": avg_price,

            "lowest": avg_price,

            "peak_roi": 0.0,

            "opened_at": time.time(),

            "breakout": breakout,

            "trailing_active": False,

            "estimated_initial_loss": (
                estimated_loss
            ),

        }

        with state_lock:

            positions[symbol] = position

        log(

            f"LIVE OPEN | "
            f"{symbol} | "
            f"{side} | "
            f"entry={avg_price:.4f} | "
            f"qty={executed_qty:.2f} | "
            f"ATR={atr:.5f} | "
            f"STOP={stop_price:.4f} | "
            f"STOP_DIST={stop_distance:.5f} | "
            f"INITIAL_RISK≈-{estimated_loss:.6f} | "
            f"BREAKOUT={breakout:.4f}"

        )

    except Exception as e:

        log(

            f"ERROR ORDEN REAL ABRIENDO "
            f"{symbol}: {e}"

        )


# ============================================================
# CERRAR POSICION
# ============================================================

def close_position(

    symbol,
    price,
    reason

):

    with state_lock:

        position = positions.get(
            symbol
        )

        if position is None:

            return

        side = position["side"]

        entry = position["entry"]

        quantity = position["quantity"]

    close_side = (

        "SELL"

        if side == "LONG"

        else

        "BUY"

    )

    try:

        result = ws_api_request(

            "order.place",

            {

                "symbol": symbol,

                "side": close_side,

                "type": "MARKET",

                "quantity": str(
                    quantity
                ),

                "reduceOnly": True,

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

            or quantity

        )

        exit_price = float(

            result.get(
                "avgPrice",
                0
            )

            or 0

        )

        if exit_price <= 0:

            exit_price = price

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

        with state_lock:

            positions.pop(
                symbol,
                None
            )

        trade_history.append({

            "symbol": symbol,

            "side": side,

            "entry": entry,

            "exit": exit_price,

            "quantity": executed_qty,

            "pnl": pnl,

            "reason": reason,

            "time": time.time(),

        })

        log(

            f"LIVE CLOSE | "
            f"{symbol} | "
            f"{side} | "
            f"entry={entry:.4f} | "
            f"exit={exit_price:.4f} | "
            f"qty={executed_qty:.2f} | "
            f"PnL={pnl:+.6f} USDT | "
            f"reason={reason}"

        )

    except Exception as e:

        log(

            f"ERROR ORDEN REAL CERRANDO "
            f"{symbol}: {e}"

        )


# ============================================================
# GESTION POSICION
# ============================================================

def manage_position(symbol):

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

    side = position["side"]

    entry = float(
        position["entry"]
    )

    quantity = float(
        position["quantity"]
    )

    stop_price = float(
        position.get(
            "stop_price",
            0
        )
    )

    initial_stop_distance = float(

        position.get(
            "stop_distance",
            0
        )

    )

    if (
        quantity <= 0
        or entry <= 0
        or initial_stop_distance <= 0
    ):

        return

    # ========================================================
    # PNL
    # ========================================================

    if side == "LONG":

        unrealized_pnl = (

            price
            - entry

        ) * quantity

    else:

        unrealized_pnl = (

            entry
            - price

        ) * quantity

    # ========================================================
    # STOP INICIAL
    # ========================================================

    stop_hit = False

    if side == "LONG":

        if price <= stop_price:

            stop_hit = True

    else:

        if price >= stop_price:

            stop_hit = True

    if stop_hit:

        close_position(

            symbol,

            price,

            f"STOP ATR LOCAL | "
            f"PnL={unrealized_pnl:+.6f} | "
            f"STOP={stop_price:.4f}"

        )

        return

    # ========================================================
    # R-MULTIPLE
    # ========================================================

    current_r = (

        unrealized_pnl
        /
        (
            initial_stop_distance
            * quantity
        )

    )

    # ========================================================
    # EMERGENCY LOSS
    # ========================================================

    emergency_loss = (

        initial_stop_distance
        * quantity
        * EMERGENCY_LOSS_R_MULTIPLIER

    )

    if unrealized_pnl <= -emergency_loss:

        close_position(

            symbol,

            price,

            f"EMERGENCY LOSS CAP | "
            f"PnL={unrealized_pnl:+.6f} | "
            f"CAP=-{emergency_loss:.6f}"

        )

        return

    # ========================================================
    # TRAILING
    #
    # Cuando llega a +1R empieza a proteger.
    #
    # El trailing queda a 1.35 ATR del mejor precio.
    # ========================================================

    if side == "LONG":

        peak_price = max(

            float(
                position.get(
                    "highest",
                    entry
                )
            ),

            price

        )

        with state_lock:

            position["highest"] = peak_price

        favorable_move = (

            peak_price
            - entry

        )

        peak_r = (

            favorable_move
            /
            initial_stop_distance

        )

        if peak_r >= TRAILING_START_R:

            with state_lock:

                position["trailing_active"] = True

            trailing_stop = (

                peak_price
                -
                float(
                    position["atr"]
                )
                * TRAILING_ATR_MULTIPLIER

            )

            trailing_stop = normalize_price(

                symbol,
                trailing_stop

            )

            if trailing_stop > stop_price:

                with state_lock:

                    position["stop_price"] = (
                        trailing_stop
                    )

                stop_price = trailing_stop

                log(

                    f"{symbol} | "
                    f"TRAILING LONG | "
                    f"peak={peak_price:.4f} | "
                    f"R={peak_r:.2f} | "
                    f"NEW_STOP={trailing_stop:.4f}"

                )

    else:

        peak_price = min(

            float(
                position.get(
                    "lowest",
                    entry
                )
            ),

            price

        )

        with state_lock:

            position["lowest"] = peak_price

        favorable_move = (

            entry
            - peak_price

        )

        peak_r = (

            favorable_move
            /
            initial_stop_distance

        )

        if peak_r >= TRAILING_START_R:

            with state_lock:

                position["trailing_active"] = True

            trailing_stop = (

                peak_price
                +
                float(
                    position["atr"]
                )
                * TRAILING_ATR_MULTIPLIER

            )

            trailing_stop = normalize_price(

                symbol,
                trailing_stop

            )

            if (
                stop_price <= 0
                or trailing_stop < stop_price
            ):

                with state_lock:

                    position["stop_price"] = (
                        trailing_stop
                    )

                stop_price = trailing_stop

                log(

                    f"{symbol} | "
                    f"TRAILING SHORT | "
                    f"peak={peak_price:.4f} | "
                    f"R={peak_r:.2f} | "
                    f"NEW_STOP={trailing_stop:.4f}"

                )

    # ========================================================
    # SEGUNDA COMPROBACION DEL STOP
    # ========================================================

    if side == "LONG":

        if price <= stop_price:

            close_position(

                symbol,

                price,

                f"TRAIL/STOP LONG | "
                f"PnL={unrealized_pnl:+.6f}"

            )

            return

    else:

        if price >= stop_price:

            close_position(

                symbol,

                price,

                f"TRAIL/STOP SHORT | "
                f"PnL={unrealized_pnl:+.6f}"

            )

            return


# ============================================================
# PROCESAR VELA
# ============================================================

def process_candle(symbol):

    with state_lock:

        candles = list(
            market_data[
                symbol
            ]["candles"]
        )

    if len(candles) < MIN_CANDLES:

        return

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

    # ========================================================
    # SI YA HAY POSICION:
    #
    # NO LA CERRAMOS POR UNA SEÑAL CONTRARIA.
    #
    # La estrategia debe dejar correr al ganador.
    # El stop/trailing manda.
    # ========================================================

    if existing is not None:

        log(

            f"{symbol} | "
            f"SEÑAL NUEVA PERO POSICION ACTIVA | "
            f"NO SE CIERRA POR SEÑAL CONTRARIA"

        )

        return

    if (
        total_positions
        >= MAX_POSITIONS
    ):

        return

    open_position(
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

        data = data.get(
            "data",
            data
        )

        kline = data.get("k")

        if not kline:

            return

        price = float(
            kline["c"]
        )

        with state_lock:

            market_data[
                symbol
            ]["price"] = price

        if not kline["x"]:

            return

        total_volume = float(
            kline["v"]
        )

        taker_buy_volume = float(
            kline["V"]
        )

        candle = [

            int(
                kline["t"]
            ),

            float(
                kline["o"]
            ),

            float(
                kline["h"]
            ),

            float(
                kline["l"]
            ),

            float(
                kline["c"]
            ),

            total_volume,

            taker_buy_volume,

        ]

        with state_lock:

            candles = market_data[
                symbol
            ]["candles"]

            last_candle_time = (
                market_data[
                    symbol
                ]["last_candle_time"]
            )

            if (

                last_candle_time
                is not None

                and

                last_candle_time
                == candle[0]

            ):

                return

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

            market_data[
                symbol
            ]["last_candle_time"] = (
                candle[0]
            )

        taker_ratio = (

            taker_buy_volume
            / total_volume

            if total_volume > 0

            else 0.0

        )

        log(

            f"{symbol} | "
            f"VELA 5M CERRADA | "
            f"open={candle[1]:.4f} | "
            f"high={candle[2]:.4f} | "
            f"low={candle[3]:.4f} | "
            f"close={candle[4]:.4f} | "
            f"volume={candle[5]:.2f} | "
            f"TAKER_BUY={taker_ratio:.3f}"

        )

        process_candle(
            symbol
        )

    except Exception as e:

        log(

            f"{symbol} | "
            f"Error market WS: {e}"

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

                on_message=(

                    lambda ws, message:

                    on_market_message(

                        symbol,
                        ws,
                        message

                    )

                ),

                on_error=make_market_error(
                    symbol
                ),

                on_close=make_market_close(
                    symbol
                ),

            )

            ws.run_forever(

                ping_interval=60,

                ping_timeout=20

            )

        except Exception as e:

            log(

                f"{symbol} | "
                f"Market WS exception: {e}"

            )

        log(

            f"{symbol} | "
            f"Reconexión Market WS "
            f"en {RECONNECT_SECONDS}s..."

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

        "method":
            "userDataStream.start",

        "params": {

            "apiKey": API_KEY,

        },

    }

    ws.send(
        json.dumps(request)
    )

    response = json.loads(
        ws.recv()
    )

    if response.get(
        "status"
    ) != 200:

        ws.close()

        raise Exception(

            "UserDataStream.start "
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
        "USER DATA STREAM CREADO "
        "POR WS API"
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
        f"User WS cerrado: "
        f"{code} {msg}"
    )


# ============================================================
# SINCRONIZAR POSICIONES
# ============================================================

def sync_account_positions(data):

    account = data.get(
        "a",
        {}
    )

    for item in account.get(
        "P",
        []
    ):

        symbol = item.get("s")

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

                else

                "SHORT"

            )

            qty = abs(amount)

            old = positions.get(
                symbol
            )

            current_price = (

                market_data[
                    symbol
                ].get(
                    "price"
                )

                or entry

            )

            old_atr = (

                old.get(
                    "atr",
                    0.0
                )

                if old

                else 0.0

            )

            if old_atr <= 0:

                old_atr = 0.0

            old_stop_distance = (

                old.get(
                    "stop_distance",
                    0.0
                )

                if old

                else 0.0

            )

            if (
                old_stop_distance <= 0
                and old_atr > 0
            ):

                old_stop_distance = (

                    old_atr
                    * ATR_STOP_MULTIPLIER

                )

            if old_stop_distance > 0:

                if side == "LONG":

                    stop_price = (

                        entry
                        - old_stop_distance

                    )

                else:

                    stop_price = (

                        entry
                        + old_stop_distance

                    )

            else:

                stop_price = 0.0

            positions[symbol] = {

                "symbol": symbol,

                "side": side,

                "entry": entry,

                "quantity": qty,

                "atr": old_atr,

                "initial_atr": old_atr,

                "stop_distance": (
                    old_stop_distance
                ),

                "stop_price": stop_price,

                "highest": max(
                    entry,
                    current_price
                ),

                "lowest": min(
                    entry,
                    current_price
                ),

                "peak_roi": 0.0,

                "opened_at": (

                    old.get(
                        "opened_at",
                        time.time()
                    )

                    if old

                    else time.time()

                ),

                "breakout": (

                    old.get(
                        "breakout",
                        entry
                    )

                    if old

                    else entry

                ),

                "trailing_active": (

                    old.get(
                        "trailing_active",
                        False
                    )

                    if old

                    else False

                ),

            }

        log(

            f"ACCOUNT SYNC | "
            f"{symbol} | "
            f"{side} | "
            f"qty={qty:.2f} | "
            f"entry={entry:.4f} | "
            f"STOP={stop_price:.4f}"

        )


# ============================================================
# USER MESSAGE
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

        elif event_type == "ALGO_UPDATE":

            log(
                "ALGO UPDATE recibido"
            )

    except Exception as e:

        log(
            f"Error User WS: {e}"
        )


# ============================================================
# USER DATA WEBSOCKET LOOP
# ============================================================

def user_websocket_loop():

    global listen_key
    global user_stream_control

    while True:

        stream_ws = None

        try:

            key = start_user_data_stream()

            for symbol in SYMBOLS:

                rule = SYMBOL_RULES.get(
                    symbol
                )

                if not rule:

                    raise Exception(

                        f"No existen reglas "
                        f"locales para {symbol}"

                    )

                log(

                    f"{symbol} | "
                    f"REGLAS LOCALES | "
                    f"step={rule['step']} | "
                    f"min_qty={rule['min_qty']} | "
                    f"min_notional="
                    f"{rule['min_notional']} | "
                    f"tick={rule['tick']}"

                )

            log(

                f"APALANCAMIENTO OBJETIVO = "
                f"x{LEVERAGE}"

            )

            log(

                "El bot NO modifica el leverage. "
                "Debe estar configurado manualmente "
                f"en Binance en x{LEVERAGE}."

            )

            ws_api_ready.set()

            ws_url = (

                USER_STREAM_BASE
                + key

            )

            log(
                "Conectando User Data WebSocket..."
            )

            log(

                "User Data URL = "
                "wss://fstream.binance.com/private/ws/"
                "<listenKey>"

            )

            stream_ws = websocket.WebSocketApp(

                ws_url,

                on_open=on_user_open,

                on_message=on_user_message,

                on_error=on_user_error,

                on_close=on_user_close,

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

            except Exception:

                pass

        with user_stream_control_lock:

            try:

                if user_stream_control:

                    user_stream_control.close()

            except Exception:

                pass

            user_stream_control = None

        ws_api_ready.clear()

        log(

            f"Reconexión User Data "
            f"en {RECONNECT_SECONDS} segundos..."

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

                "method":
                    "userDataStream.ping",

                "params": {

                    "apiKey": API_KEY,

                },

            }

            with user_stream_control_lock:

                ws.send(
                    json.dumps(request)
                )

                ws.settimeout(15)

                while True:

                    response = json.loads(
                        ws.recv()
                    )

                    if (

                        response.get("id")
                        == request_id

                    ):

                        break

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

                    "USER DATA KEEPALIVE "
                    f"RESPUESTA: {response}"

                )

        except Exception as e:

            log(

                f"User Data keepalive "
                f"error: {e}"

            )

            with user_stream_control_lock:

                try:

                    if user_stream_control:

                        user_stream_control.close()

                except Exception:

                    pass

                user_stream_control = None

            ws_api_ready.clear()


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

        time.sleep(1)


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

            if ws_api_ready.is_set():

                try:

                    balance = (
                        get_usdt_balance()
                    )

                except Exception as e:

                    balance = None

                    log(

                        f"STATUS | "
                        f"Error balance WS: {e}"

                    )

            else:

                balance = None

            log(

                f"STATUS | "
                f"USDT_available="
                f"{balance if balance is not None else 'N/A'} | "
                f"positions="
                f"{active}/{MAX_POSITIONS} | "
                f"WS_READY="
                f"{ws_api_ready.is_set()} | "
                f"MAX_MARGIN="
                f"{MAX_MARGIN_PERCENT:.2%} | "
                f"STOP="
                f"{ATR_STOP_MULTIPLIER:.2f}ATR"

            )

        except Exception as e:

            log(
                f"Status error: {e}"
            )

        time.sleep(60)


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

            f"Health server "
            f"escuchando en puerto "
            f"{port}"

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
        "       BOT LINKUSDT - PLATA REAL"
    )

    log(
        "       BINANCE USD-M FUTURES"
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
        f"SYMBOLS = {SYMBOLS}"
    )

    log(
        f"MAX_POSITIONS = {MAX_POSITIONS}"
    )

    log(
        f"LEVERAGE = x{LEVERAGE}"
    )

    log(
        f"MAX_MARGIN_PERCENT = "
        f"{MAX_MARGIN_PERCENT:.2%}"
    )

    log(
        f"INTERVAL = {INTERVAL}"
    )

    log(
        f"DONCHIAN = "
        f"{DONCHIAN_PERIOD}"
    )

    log(
        f"EMA = "
        f"{EMA_FAST}/{EMA_SLOW}"
    )

    log(
        f"ATR STOP = "
        f"{ATR_STOP_MULTIPLIER:.2f}x"
    )

    log(
        f"DONCHIAN PREBREAK = "
        f"{DONCHIAN_PREBREAK_ATR:.2f} ATR"
    )

    log(
        f"ANTI-LATE = "
        f"{MAX_ENTRY_DISTANCE_ATR:.2f} ATR"
    )

    log(
        f"TAKER LONG >= "
        f"{TAKER_LONG_THRESHOLD:.2f}"
    )

    log(
        f"TAKER SHORT <= "
        f"{TAKER_SHORT_THRESHOLD:.2f}"
    )

    log(
        f"MOMENTUM MIN = "
        f"{MIN_MOMENTUM_ATR:.2f} ATR"
    )

    log(
        f"TRAIL START = "
        f"{TRAILING_START_R:.2f}R"
    )

    log(
        f"TRAIL DISTANCE = "
        f"{TRAILING_ATR_MULTIPLIER:.2f} ATR"
    )

    log(
        "ENTRADA = MARKET"
    )

    log(
        "STOP = LOCAL ATR"
    )

    log(
        "SIN STOP ALGO EN BINANCE"
    )

    log(
        "25% CAPITAL = MARGEN MAXIMO"
    )

    log(
        "SIN TP FIJO"
    )

    log(
        "SEÑAL CONTRARIA NO CIERRA"
    )

    log(
        "=========================================="
    )

    log(
        "BINANCE REST = DESACTIVADO PARA TRADING"
    )

    log(
        "BINANCE MARKET = WEBSOCKET"
    )

    log(
        "BINANCE BALANCE = WS API"
    )

    log(
        "BINANCE ORDENES = WS API"
    )

    log(
        "BINANCE USER DATA = WEBSOCKET"
    )

    log(
        "KUCOIN = SPOT | SOLO HISTORIAL INICIAL"
    )

    log(
        f"KUCOIN SYMBOL = {KUCOIN_SYMBOL}"
    )

    log(
        "=========================================="
    )

    log_public_ip()

    if not API_KEY:

        log(
            "FATAL ERROR: "
            "Falta BINANCE_API_KEY"
        )

        return

    if not API_SECRET:

        log(
            "FATAL ERROR: "
            "Falta BINANCE_API_SECRET"
        )

        return

    threading.Thread(

        target=health_server,

        daemon=True

    ).start()

    threading.Thread(

        target=user_websocket_loop,

        daemon=True

    ).start()

    threading.Thread(

        target=user_stream_keepalive_loop,

        daemon=True

    ).start()

    log(
        "Esperando conexión "
        "de la WS API de ejecución..."
    )

    ready = ws_api_ready.wait(
        timeout=30
    )

    if ready:

        log(
            "WS API de ejecución LISTA"
        )

    else:

        log(
            "WS API todavía no conectó "
            "tras 30 segundos."
        )

        log(
            "Se continúa arrancando "
            "el Market WebSocket."
        )

        log(
            "Las señales se ignorarán "
            "hasta que WS_READY=True."
        )

    for symbol in SYMBOLS:

        try:

            load_initial_history_from_kucoin(
                symbol
            )

        except Exception as e:

            log(

                f"{symbol} | "
                f"ERROR HISTORIAL KUCOIN: "
                f"{e}"

            )

            log(

                f"{symbol} | "
                f"El bot no puede calcular "
                f"la estrategia correctamente "
                f"sin las {MIN_CANDLES} velas iniciales."

            )

    for symbol in SYMBOLS:

        threading.Thread(

            target=market_websocket_loop,

            args=(symbol,),

            daemon=True

        ).start()

        time.sleep(
            0.10
        )

    threading.Thread(

        target=position_manager_loop,

        daemon=True

    ).start()

    threading.Thread(

        target=status_loop,

        daemon=True

    ).start()

    log(
        "=========================================="
    )

    log(
        "BOT LINKUSDT INICIADO"
    )

    log(
        "ESTRATEGIA = "
        "EMA + DONCHIAN + ATR + TAKER + MOMENTUM"
    )

    log(
        "ENTRADA = PERMISIVA"
    )

    log(
        "SALIDA = ESTRICTA"
    )

    log(
        "DONCHIAN = 20"
    )

    log(
        "EMA = 9/21"
    )

    log(
        "ATR STOP = 1.80 ATR"
    )

    log(
        "TRAIL = 1.35 ATR"
    )

    log(
        "MARGEN MAXIMO = 25%"
    )

    log(
        "TAKER PRESSURE = ACTIVO"
    )

    log(
        "ANTI-TARDANZA = ACTIVO"
    )

    log(
        "SIN 5/6"
    )

    log(
        "SIN 6/6"
    )

    log(
        "SIN TP FIJO"
    )

    log(
        "MAX POSITIONS = 1"
    )

    log(
        "Esperando datos de mercado..."
    )

    log(
        "=========================================="
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
