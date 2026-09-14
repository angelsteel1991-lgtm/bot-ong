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
# BOT - BINANCE USD-M FUTURES - PLATA REAL
#
# HISTORIAL:
#   KUCOIN FUTURES 5m
#
# MERCADO EN VIVO:
#   BINANCE FUTURES WEBSOCKET
#
# SIN BINANCE REST PARA TRADING
# ============================================================


# ============================================================
# CONFIGURACION GENERAL
# ============================================================

LIVE_TRADING = True
USE_TESTNET = False

INTERVAL = "5m"

MAX_POSITIONS = 1

LEVERAGE = 6

# Margen máximo utilizado por operación
MAX_MARGIN_PERCENT = 0.25

MIN_CANDLES = 80

# ============================================================
# SIMBOLOS BINANCE
# ============================================================

SYMBOLS = [
    "1000PEPEUSDT",
    "WIFUSDT",
    "1000BONKUSDT",
    "1000FLOKIUSDT",
]


# ============================================================
# MAPEOS KUCOIN FUTURES
#
# IMPORTANTE:
# SON CONTRATOS FUTURES, NO SPOT.
#
# multiplier:
#   convierte la cotización de KuCoin Futures
#   a la escala de precio del contrato Binance.
#
# PEPE:
#   KuCoin PEPEUSDTM -> precio PEPE
#   Binance 1000PEPEUSDT -> precio x1000
#
# FLOKI:
#   KuCoin FLOKIUSDTM -> precio FLOKI
#   Binance 1000FLOKIUSDT -> precio x1000
#
# BONK:
#   KuCoin 1000BONKUSDTM -> misma escala
#
# WIF:
#   KuCoin WIFUSDTM -> misma escala
# ============================================================

KUCOIN_FUTURES = {
    "1000PEPEUSDT": {
        "symbol": "PEPEUSDTM",
        "multiplier": 1000.0,
    },

    "WIFUSDT": {
        "symbol": "WIFUSDTM",
        "multiplier": 1.0,
    },

    "1000BONKUSDT": {
        "symbol": "1000BONKUSDTM",
        "multiplier": 1.0,
    },

    "1000FLOKIUSDT": {
        "symbol": "FLOKIUSDTM",
        "multiplier": 1000.0,
    },
}


# ============================================================
# REGLAS LOCALES BINANCE
#
# SIN REST exchangeInfo.
# ============================================================

SYMBOL_RULES = {

    "1000PEPEUSDT": {
        "step": 1.0,
        "min_qty": 1.0,
        "min_notional": 5.0,
        "tick": 0.0000001,
    },

    "WIFUSDT": {
        "step": 1.0,
        "min_qty": 1.0,
        "min_notional": 5.0,
        "tick": 0.0001,
    },

    "1000BONKUSDT": {
        "step": 1.0,
        "min_qty": 1.0,
        "min_notional": 5.0,
        "tick": 0.000001,
    },

    "1000FLOKIUSDT": {
        "step": 1.0,
        "min_qty": 1.0,
        "min_notional": 5.0,
        "tick": 0.00001,
    },
}


# ============================================================
# INDICADORES
# ============================================================

ATR_PERIOD = 14

DONCHIAN_PERIOD = 20

EMA_FAST = 9
EMA_SLOW = 21

VOLUME_PERIOD = 20

TAKER_PRESSURE_LOOKBACK = 3

DONCHIAN_PREBREAK_ATR = 0.20

MAX_ENTRY_DISTANCE_ATR = 0.45

MOMENTUM_LOOKBACK = 2

MIN_MOMENTUM_ATR = 0.05


# ============================================================
# ATR STOP
# ============================================================

ATR_STOP_MULTIPLIER = 1.80


# ============================================================
# PERDIDA MAXIMA
#
# EL RIESGO TEORICO DEL STOP ATR NO PUEDE SUPERAR
# ESTE VALOR.
#
# ADEMAS EL WATCHER CIERRA SI EL PNL REAL
# LLEGA A -0.30 USDT.
# ============================================================

MAX_LOSS_USDT = 0.30


# ============================================================
# PROFIT TRAILING
#
# +2.00 activa
# distancia 1.50
#
# +2.00 -> protege +0.50
# +5.00 -> protege +3.50
# +10.00 -> protege +8.50
#
# SIN LIMITE SUPERIOR
# ============================================================

PROFIT_TRAIL_START_USDT = 2.00
PROFIT_TRAIL_DISTANCE_USDT = 1.50


# ============================================================
# BINANCE WS
# ============================================================

if USE_TESTNET:
    WS_API_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"
    MARKET_WS_BASE = "wss://stream.binancefuture.com/market/ws/"
else:
    WS_API_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"
    MARKET_WS_BASE = "wss://fstream.binance.com/market/ws/"


# ============================================================
# KUCOIN FUTURES
# ============================================================

KUCOIN_FUTURES_URL = "https://api-futures.kucoin.com/api/v1/kline/query"


# ============================================================
# VARIABLES GLOBALES
# ============================================================

API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()


data_lock = threading.RLock()

market_data = {}

positions = {}

balance_usdt = 0.0

binance_ws_api = None

user_stream_ws = None

user_stream_key = None

ws_api_lock = threading.RLock()

ws_api_connected = False

user_stream_connected = False

last_public_ip = None


# ============================================================
# ESTRUCTURA DE MERCADO
# ============================================================

for symbol in SYMBOLS:

    market_data[symbol] = {
        "candles": [],
        "last_candle_time": None,
        "price": 0.0,
        "closed": False,
        "ws_ready": False,
        "history_loaded": False,
    }


# ============================================================
# LOG
# ============================================================

def log(message):

    now = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    print(
        f"[{now}] {message}",
        flush=True
    )


# ============================================================
# PUBLIC IP
# ============================================================

def get_public_ip():

    global last_public_ip

    try:

        req = urllib.request.Request(
            "https://api.ipify.org",
            headers={
                "User-Agent": "Mozilla/5.0"
            }
        )

        with urllib.request.urlopen(
            req,
            timeout=10
        ) as response:

            ip = response.read().decode().strip()

        last_public_ip = ip

        log(f"PUBLIC IP: {ip}")

        return ip

    except Exception as e:

        log(f"IPIFY ERROR: {e}")

        return None


# ============================================================
# HELPERS
# ============================================================

def floor_step(value, step):

    if step <= 0:
        return value

    return math.floor(
        value / step + 1e-12
    ) * step


def normalize_quantity(symbol, quantity):

    rules = SYMBOL_RULES[symbol]

    step = rules["step"]

    qty = floor_step(
        quantity,
        step
    )

    if qty < rules["min_qty"]:
        return 0.0

    return qty


def normalize_price(symbol, price):

    tick = SYMBOL_RULES[symbol]["tick"]

    if tick <= 0:
        return price

    return round(
        floor_step(price, tick),
        12
    )


def total_open_positions():

    with data_lock:

        total = 0

        for p in positions.values():

            if abs(p.get("qty", 0.0)) > 0:
                total += 1

        return total


# ============================================================
# KUCOIN FUTURES - HISTORIAL
# ============================================================

def load_initial_history_from_kucoin(symbol):

    cfg = KUCOIN_FUTURES[symbol]

    kucoin_symbol = cfg["symbol"]

    multiplier = cfg["multiplier"]

    now_ms = int(time.time() * 1000)

    # 100 velas de 5m
    # 100 * 300 segundos
    start_ms = now_ms - (
        100 * 5 * 60 * 1000
    )

    params = {
        "symbol": kucoin_symbol,
        "granularity": 300,
        "from": start_ms,
        "to": now_ms,
    }

    url = (
        KUCOIN_FUTURES_URL
        + "?"
        + urlencode(params)
    )

    log(
        f"{symbol} | Cargando {MIN_CANDLES} velas "
        f"desde KUCOIN FUTURES: {kucoin_symbol}"
    )

    try:

        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0"
            }
        )

        with urllib.request.urlopen(
            req,
            timeout=20
        ) as response:

            raw = response.read().decode()

        payload = json.loads(raw)

        if payload.get("code") != "200000":

            raise RuntimeError(
                f"KuCoin code={payload.get('code')} "
                f"message={payload.get('msg')}"
            )

        rows = payload.get("data", [])

        if not rows:

            raise RuntimeError(
                "KuCoin devolvio 0 velas"
            )

        candles = []

        for row in rows:

            if len(row) < 7:
                continue

            try:

                ts = int(row[0])

                # KuCoin Futures:
                # [time, open, close, high, low, volume, turnover]

                open_price = float(row[1]) * multiplier
                close_price = float(row[2]) * multiplier
                high_price = float(row[3]) * multiplier
                low_price = float(row[4]) * multiplier
                volume = float(row[5])

                candles.append({
                    "time": ts,
                    "open": open_price,
                    "high": high_price,
                    "low": low_price,
                    "close": close_price,
                    "volume": volume,

                    # KuCoin Futures Kline no entrega
                    # taker-buy-volume como Binance.
                    # Se completara con Binance WS.
                    "taker_buy_volume": None,
                    "taker_ratio": None,
                })

            except Exception:
                continue

        candles.sort(
            key=lambda x: x["time"]
        )

        # Eliminamos la vela actual/incompleta.
        current_bucket = (
            int(time.time()) // 300
        ) * 300

        candles = [
            c for c in candles
            if c["time"] < current_bucket
        ]

        candles = candles[-MIN_CANDLES:]

        if len(candles) < MIN_CANDLES:

            raise RuntimeError(
                f"Solo se obtuvieron {len(candles)} "
                f"velas cerradas"
            )

        with data_lock:

            market_data[symbol]["candles"] = candles

            market_data[symbol][
                "last_candle_time"
            ] = candles[-1]["time"]

            market_data[symbol][
                "history_loaded"
            ] = True

        log(
            f"{symbol} | HISTORIAL FUTURES OK | "
            f"KuCoin={kucoin_symbol} | "
            f"velas={len(candles)} | "
            f"ultima={candles[-1]['time']} | "
            f"multiplier={multiplier}"
        )

        return True

    except Exception as e:

        log(
            f"{symbol} | ERROR HISTORIAL KUCOIN FUTURES: {e}"
        )

        return False


# ============================================================
# INDICADORES
# ============================================================

def dataframe_for_symbol(symbol):

    with data_lock:

        candles = list(
            market_data[symbol]["candles"]
        )

    if not candles:
        return None

    df = pd.DataFrame(candles)

    if len(df) < 30:
        return None

    return df


def calculate_atr(df):

    high = df["high"]

    low = df["low"]

    close = df["close"]

    previous_close = close.shift(1)

    tr1 = high - low

    tr2 = (
        high - previous_close
    ).abs()

    tr3 = (
        low - previous_close
    ).abs()

    tr = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    atr = tr.rolling(
        ATR_PERIOD
    ).mean()

    return atr


def calculate_indicators(df):

    df = df.copy()

    df["ema_fast"] = (
        df["close"]
        .ewm(
            span=EMA_FAST,
            adjust=False
        )
        .mean()
    )

    df["ema_slow"] = (
        df["close"]
        .ewm(
            span=EMA_SLOW,
            adjust=False
        )
        .mean()
    )

    df["atr"] = calculate_atr(df)

    df["volume_ma"] = (
        df["volume"]
        .rolling(VOLUME_PERIOD)
        .mean()
    )

    df["donchian_high"] = (
        df["high"]
        .rolling(DONCHIAN_PERIOD)
        .max()
        .shift(1)
    )

    df["donchian_low"] = (
        df["low"]
        .rolling(DONCHIAN_PERIOD)
        .min()
        .shift(1)
    )

    df["prev_close"] = (
        df["close"].shift(1)
    )

    return df


# ============================================================
# TAKER PRESSURE
# ============================================================

def taker_pressure(df):

    recent = df.tail(
        TAKER_PRESSURE_LOOKBACK
    )

    ratios = recent[
        "taker_ratio"
    ].dropna()

    if len(ratios) < TAKER_PRESSURE_LOOKBACK:

        return None

    return float(
        ratios.mean()
    )


# ============================================================
# SEÑAL
# ============================================================

def calculate_signal(symbol):

    df = dataframe_for_symbol(symbol)

    if df is None:
        return None

    df = calculate_indicators(df)

    if len(df) < MIN_CANDLES:
        return None

    row = df.iloc[-1]

    previous = df.iloc[-2]

    required = [
        row["ema_fast"],
        row["ema_slow"],
        row["atr"],
        row["volume_ma"],
        row["donchian_high"],
        row["donchian_low"],
    ]

    if any(
        pd.isna(x)
        for x in required
    ):
        return None

    atr = float(row["atr"])

    if atr <= 0:
        return None

    price = float(row["close"])

    volume = float(row["volume"])

    volume_ma = float(
        row["volume_ma"]
    )

    ema_fast = float(
        row["ema_fast"]
    )

    ema_slow = float(
        row["ema_slow"]
    )

    prev_close = float(
        previous["close"]
    )

    donchian_high = float(
        row["donchian_high"]
    )

    donchian_low = float(
        row["donchian_low"]
    )

    pressure = taker_pressure(df)

    if pressure is None:
        return None

    momentum = (
        price
        - float(
            df.iloc[
                -1 - MOMENTUM_LOOKBACK
            ]["close"]
        )
    )

    # ========================================================
    # CONDICIONES LONG
    # ========================================================

    long_conditions = [

        ema_fast > ema_slow,

        price > ema_fast,

        volume > volume_ma,

        price > prev_close,

        price >= (
            donchian_high
            - atr * DONCHIAN_PREBREAK_ATR
        ),

        momentum >= (
            atr * MIN_MOMENTUM_ATR
        ),

        pressure >= 0.52,
    ]

    # ========================================================
    # CONDICIONES SHORT
    # ========================================================

    short_conditions = [

        ema_fast < ema_slow,

        price < ema_fast,

        volume > volume_ma,

        price < prev_close,

        price <= (
            donchian_low
            + atr * DONCHIAN_PREBREAK_ATR
        ),

        momentum <= (
            -atr * MIN_MOMENTUM_ATR
        ),

        pressure <= 0.48,
    ]

    long_score = sum(
        bool(x)
        for x in long_conditions
    )

    short_score = sum(
        bool(x)
        for x in short_conditions
    )

    # ========================================================
    # 6/6 REAL
    #
    # Se mantienen 6 condiciones obligatorias.
    #
    # Las 7 condiciones tienen:
    # estructura + precio + volumen + momentum +
    # prebreak + momentum ATR + taker.
    #
    # Para conservar el filtro fuerte:
    # exigimos al menos 6/7.
    # ========================================================

    if long_score >= 6:

        # No perseguir demasiado el precio.
        distance = (
            price - ema_fast
        )

        if distance <= (
            atr * MAX_ENTRY_DISTANCE_ATR
        ):

            return {
                "side": "LONG",
                "price": price,
                "atr": atr,
                "score": long_score,
                "pressure": pressure,
            }

    if short_score >= 6:

        distance = (
            ema_fast - price
        )

        if distance <= (
            atr * MAX_ENTRY_DISTANCE_ATR
        ):

            return {
                "side": "SHORT",
                "price": price,
                "atr": atr,
                "score": short_score,
                "pressure": pressure,
            }

    return None


# ============================================================
# POSICIONAMIENTO
# ============================================================

def calculate_quantity(
    symbol,
    entry_price,
    atr,
):

    if atr <= 0:
        return 0.0

    rules = SYMBOL_RULES[symbol]

    with data_lock:
        balance = balance_usdt

    if balance <= 0:

        log(
            f"{symbol} | Sin balance USDT disponible"
        )

        return 0.0

    # ========================================================
    # LIMITE POR MARGEN
    # ========================================================

    max_margin = (
        balance
        * MAX_MARGIN_PERCENT
    )

    max_notional = (
        max_margin
        * LEVERAGE
    )

    qty_by_margin = (
        max_notional
        / entry_price
    )

    # ========================================================
    # LIMITE POR RIESGO
    #
    # ATR STOP = ATR * 1.80
    #
    # riesgo = distancia * cantidad
    #
    # cantidad <= 0.30 / distancia
    # ========================================================

    stop_distance = (
        atr * ATR_STOP_MULTIPLIER
    )

    if stop_distance <= 0:
        return 0.0

    qty_by_risk = (
        MAX_LOSS_USDT
        / stop_distance
    )

    raw_quantity = min(
        qty_by_margin,
        qty_by_risk
    )

    quantity = normalize_quantity(
        symbol,
        raw_quantity
    )

    if quantity <= 0:

        log(
            f"{symbol} | SIN ENTRADA | "
            f"qty calculada menor al minimo | "
            f"margin_qty={qty_by_margin:.4f} | "
            f"risk_qty={qty_by_risk:.4f}"
        )

        return 0.0

    notional = (
        quantity
        * entry_price
    )

    if notional < rules["min_notional"]:

        log(
            f"{symbol} | SIN ENTRADA | "
            f"notional {notional:.4f} < "
            f"minNotional {rules['min_notional']}"
        )

        return 0.0

    theoretical_risk = (
        quantity
        * stop_distance
    )

    if theoretical_risk > (
        MAX_LOSS_USDT + 1e-9
    ):

        log(
            f"{symbol} | SIN ENTRADA | "
            f"riesgo teorico {theoretical_risk:.4f} "
            f"> {MAX_LOSS_USDT:.4f}"
        )

        return 0.0

    return quantity


# ============================================================
# FIRMA BINANCE WS API
# ============================================================

def sign_params(params):

    query = urlencode(
        sorted(params.items())
    )

    signature = hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()

    return signature


# ============================================================
# WS API REQUEST
# ============================================================

def ws_api_request(
    method,
    params=None,
    timeout=15,
):

    global binance_ws_api

    if params is None:
        params = {}

    with ws_api_lock:

        if (
            binance_ws_api is None
            or not binance_ws_api.sock
            or not binance_ws_api.sock.connected
        ):

            connect_ws_api()

        if binance_ws_api is None:
            return None

        request_id = str(
            uuid.uuid4()
        )

        request_params = dict(
            params
        )

        request_params[
            "apiKey"
        ] = API_KEY

        request_params[
            "timestamp"
        ] = int(
            time.time() * 1000
        )

        request_params[
            "signature"
        ] = sign_params(
            request_params
        )

        payload = {
            "id": request_id,
            "method": method,
            "params": request_params,
        }

        try:

            binance_ws_api.send(
                json.dumps(payload)
            )

            deadline = (
                time.time()
                + timeout
            )

            while time.time() < deadline:

                raw = (
                    binance_ws_api.recv()
                )

                response = json.loads(
                    raw
                )

                # Eventos async no son la respuesta
                # directa del request.
                if response.get("id") != request_id:

                    process_async_ws_message(
                        response
                    )

                    continue

                return response

        except Exception as e:

            log(
                f"WS API request error "
                f"{method}: {e}"
            )

            try:
                binance_ws_api.close()
            except Exception:
                pass

            binance_ws_api = None

            return None

    return None


# ============================================================
# CONEXION WS API
# ============================================================

def connect_ws_api():

    global binance_ws_api
    global ws_api_connected

    if not API_KEY or not API_SECRET:

        log(
            "ERROR: faltan BINANCE_API_KEY "
            "o BINANCE_API_SECRET"
        )

        return False

    try:

        if binance_ws_api is not None:

            try:
                binance_ws_api.close()
            except Exception:
                pass

        binance_ws_api = websocket.create_connection(
            WS_API_URL,
            timeout=20,
            enable_multithread=True,
        )

        ws_api_connected = True

        log(
            "WS API Binance conectada"
        )

        return True

    except Exception as e:

        ws_api_connected = False

        log(
            f"ERROR conectando WS API: {e}"
        )

        return False


# ============================================================
# MENSAJES ASYNC
# ============================================================

def process_async_ws_message(message):

    if not isinstance(message, dict):
        return

    if (
        message.get("e")
        == "ACCOUNT_UPDATE"
    ):

        process_account_update(
            message
        )


# ============================================================
# USER DATA STREAM
# ============================================================

def start_user_data_stream():

    global user_stream_ws
    global user_stream_key
    global user_stream_connected

    while True:

        try:

            if binance_ws_api is None:

                connect_ws_api()

            response = ws_api_request(
                "userDataStream.start"
            )

            if not response:

                time.sleep(10)

                continue

            if response.get("status") != 200:

                log(
                    f"UserDataStream.start "
                    f"rechazado: {response}"
                )

                time.sleep(30)

                continue

            result = response.get(
                "result",
                {}
            )

            user_stream_key = (
                result.get(
                    "listenKey"
                )
                or result.get(
                    "subscriptionId"
                )
            )

            if not user_stream_key:

                log(
                    f"UserDataStream sin key: "
                    f"{response}"
                )

                time.sleep(30)

                continue

            log(
                "User Data Stream iniciado"
            )

            # La implementación actual del bot
            # mantiene la conexión WS API para eventos.
            #
            # Si Binance devuelve listenKey,
            # abrimos market/user websocket clásico.

            if str(
                user_stream_key
            ).startswith("ws"):

                user_stream_ws = websocket.create_connection(
                    user_stream_key,
                    timeout=30,
                )

            else:

                user_stream_connected = True

                # En Binance WS API moderna,
                # los eventos de usuario llegan por
                # la misma sesión autenticada.
                #
                # Hacemos lectura periódica mediante
                # el loop dedicado.

            user_stream_keepalive_loop()

        except Exception as e:

            user_stream_connected = False

            log(
                f"User WS exception: {e}"
            )

            time.sleep(10)


# ============================================================
# USER STREAM KEEPALIVE
# ============================================================

def user_stream_keepalive_loop():

    global user_stream_connected

    user_stream_connected = True

    last_keepalive = time.time()

    while True:

        try:

            if time.time() - last_keepalive > 25 * 60:

                response = ws_api_request(
                    "userDataStream.ping",
                    {},
                    timeout=10,
                )

                log(
                    f"User Data Stream keepalive: "
                    f"{response}"
                )

                last_keepalive = time.time()

            # Si existe websocket separado
            if user_stream_ws is not None:

                user_stream_ws.settimeout(1)

                try:

                    raw = (
                        user_stream_ws.recv()
                    )

                    if raw:

                        message = json.loads(
                            raw
                        )

                        process_user_event(
                            message
                        )

                except websocket.WebSocketTimeoutException:

                    pass

            else:

                time.sleep(1)

        except Exception as e:

            user_stream_connected = False

            log(
                f"User stream loop error: {e}"
            )

            break


# ============================================================
# USER EVENT
# ============================================================

def process_user_event(message):

    if not isinstance(message, dict):
        return

    event_type = message.get("e")

    if event_type == "ACCOUNT_UPDATE":

        process_account_update(
            message
        )

    elif event_type == "ORDER_TRADE_UPDATE":

        process_order_update(
            message
        )


# ============================================================
# ACCOUNT UPDATE
# ============================================================

def process_account_update(message):

    global balance_usdt

    account = message.get(
        "a",
        {}
    )

    balances = account.get(
        "B",
        []
    )

    for item in balances:

        if item.get("a") == "USDT":

            try:

                balance_usdt = float(
                    item.get("wb", 0)
                )

            except Exception:
                pass

    positions_update = account.get(
        "P",
        []
    )

    for p in positions_update:

        symbol = p.get("s")

        if symbol not in SYMBOLS:
            continue

        try:

            qty = float(
                p.get("pa", 0)
            )

            entry = float(
                p.get("ep", 0)
            )

            unrealized = float(
                p.get("up", 0)
            )

        except Exception:

            continue

        if abs(qty) <= 0:

            with data_lock:

                if symbol in positions:

                    log(
                        f"{symbol} | POSICION CERRADA"
                    )

                    positions.pop(
                        symbol,
                        None
                    )

            continue

        side = (
            "LONG"
            if qty > 0
            else "SHORT"
        )

        with data_lock:

            existing = positions.get(
                symbol,
                {}
            )

            positions[symbol] = {
                "symbol": symbol,
                "side": side,
                "qty": abs(qty),
                "entry_price": entry,
                "unrealized_pnl": unrealized,

                "atr": existing.get(
                    "atr",
                    0.0
                ),

                "stop_price": existing.get(
                    "stop_price",
                    0.0
                ),

                "peak_pnl": max(
                    existing.get(
                        "peak_pnl",
                        0.0
                    ),
                    unrealized
                ),

                "protected_pnl": existing.get(
                    "protected_pnl",
                    None
                ),

                "trail_active": existing.get(
                    "trail_active",
                    False
                ),
            }


# ============================================================
# ORDER UPDATE
# ============================================================

def process_order_update(message):

    order = message.get(
        "o",
        {}
    )

    symbol = order.get("s")

    if symbol not in SYMBOLS:
        return

    status = order.get("X")

    side = order.get("S")

    avg_price = order.get(
        "ap",
        "0"
    )

    executed_qty = order.get(
        "z",
        "0"
    )

    log(
        f"{symbol} | ORDER UPDATE | "
        f"side={side} | status={status} | "
        f"qty={executed_qty} | avg={avg_price}"
    )


# ============================================================
# BALANCE
# ============================================================

def update_balance():

    global balance_usdt

    response = ws_api_request(
        "account.balance"
    )

    if not response:
        return False

    if response.get("status") != 200:

        log(
            f"account.balance error: "
            f"{response}"
        )

        return False

    result = response.get(
        "result",
        []
    )

    if isinstance(result, list):

        for item in result:

            if item.get(
                "asset"
            ) == "USDT":

                try:

                    balance_usdt = float(
                        item.get(
                            "balance",
                            0
                        )
                    )

                    log(
                        f"USDT balance: "
                        f"{balance_usdt:.8f}"
                    )

                    return True

                except Exception:
                    pass

    return False


# ============================================================
# CERRAR POSICION
# ============================================================

def close_position(
    symbol,
    reason,
):

    with data_lock:

        position = positions.get(
            symbol
        )

    if not position:

        return False

    qty = float(
        position["qty"]
    )

    if qty <= 0:

        return False

    side = position["side"]

    close_side = (
        "SELL"
        if side == "LONG"
        else "BUY"
    )

    qty = normalize_quantity(
        symbol,
        qty
    )

    if qty <= 0:
        return False

    log(
        f"{symbol} | CERRANDO MARKET "
        f"| side={close_side} "
        f"| qty={qty} "
        f"| motivo={reason}"
    )

    if not LIVE_TRADING:

        log(
            f"{symbol} | PAPER CLOSE"
        )

        with data_lock:

            positions.pop(
                symbol,
                None
            )

        return True

    response = ws_api_request(
        "order.place",
        {
            "symbol": symbol,
            "side": close_side,
            "type": "MARKET",
            "quantity": qty,
            "reduceOnly": True,
        },
        timeout=15,
    )

    if not response:

        log(
            f"{symbol} | ERROR cierre: "
            f"sin respuesta"
        )

        return False

    if response.get("status") != 200:

        log(
            f"{symbol} | ERROR cierre: "
            f"{response}"
        )

        return False

    log(
        f"{symbol} | MARKET CLOSE enviado"
    )

    return True


# ============================================================
# ENTRADA
# ============================================================

def open_position(
    symbol,
    signal,
):

    if total_open_positions() >= MAX_POSITIONS:

        return False

    side = signal["side"]

    entry_price = signal["price"]

    atr = signal["atr"]

    quantity = calculate_quantity(
        symbol,
        entry_price,
        atr
    )

    if quantity <= 0:

        return False

    stop_distance = (
        atr * ATR_STOP_MULTIPLIER
    )

    if side == "LONG":

        stop_price = (
            entry_price
            - stop_distance
        )

        order_side = "BUY"

    else:

        stop_price = (
            entry_price
            + stop_distance
        )

        order_side = "SELL"

    stop_price = normalize_price(
        symbol,
        stop_price
    )

    theoretical_risk = (
        quantity
        * stop_distance
    )

    log(
        f"{symbol} | "
        f"SEÑAL {side} {signal['score']}/7 | "
        f"entry={entry_price:.12f} | "
        f"ATR={atr:.12f} | "
        f"qty={quantity} | "
        f"ATR_STOP={stop_price:.12f} | "
        f"risk={theoretical_risk:.4f} USDT | "
        f"taker={signal['pressure']:.3f}"
    )

    if not LIVE_TRADING:

        log(
            f"{symbol} | PAPER ENTRY"
        )

        with data_lock:

            positions[symbol] = {
                "symbol": symbol,
                "side": side,
                "qty": quantity,
                "entry_price": entry_price,
                "atr": atr,
                "stop_price": stop_price,
                "unrealized_pnl": 0.0,
                "peak_pnl": 0.0,
                "protected_pnl": None,
                "trail_active": False,
            }

        return True

    response = ws_api_request(
        "order.place",
        {
            "symbol": symbol,
            "side": order_side,
            "type": "MARKET",
            "quantity": quantity,
        },
        timeout=15,
    )

    if not response:

        log(
            f"{symbol} | ERROR ENTRY: "
            f"sin respuesta"
        )

        return False

    if response.get("status") != 200:

        log(
            f"{symbol} | ERROR ENTRY: "
            f"{response}"
        )

        return False

    log(
        f"{symbol} | MARKET ENTRY ENVIADA | "
        f"{side} | qty={quantity}"
    )

    # Guardamos provisionalmente la informacion.
    # ACCOUNT_UPDATE luego reemplaza entry/qty
    # por los datos reales ejecutados.

    with data_lock:

        positions[symbol] = {
            "symbol": symbol,
            "side": side,
            "qty": quantity,
            "entry_price": entry_price,
            "atr": atr,
            "stop_price": stop_price,
            "unrealized_pnl": 0.0,
            "peak_pnl": 0.0,
            "protected_pnl": None,
            "trail_active": False,
        }

    return True


# ============================================================
# PNL
# ============================================================

def calculate_position_pnl(
    position,
    current_price,
):

    entry = float(
        position["entry_price"]
    )

    qty = float(
        position["qty"]
    )

    side = position["side"]

    if side == "LONG":

        return (
            current_price
            - entry
        ) * qty

    else:

        return (
            entry
            - current_price
        ) * qty


# ============================================================
# POSITION MANAGER
#
# SE EJECUTA APROXIMADAMENTE CADA SEGUNDO.
# ============================================================

def position_manager():

    while True:

        try:

            with data_lock:

                current_positions = list(
                    positions.items()
                )

            for symbol, position in current_positions:

                with data_lock:

                    current_price = float(
                        market_data[symbol][
                            "price"
                        ]
                    )

                if current_price <= 0:
                    continue

                entry = float(
                    position["entry_price"]
                )

                qty = float(
                    position["qty"]
                )

                if entry <= 0 or qty <= 0:
                    continue

                pnl = calculate_position_pnl(
                    position,
                    current_price
                )

                with data_lock:

                    position["unrealized_pnl"] = pnl

                    old_peak = float(
                        position.get(
                            "peak_pnl",
                            0.0
                        )
                    )

                    if pnl > old_peak:

                        position["peak_pnl"] = pnl

                    peak = float(
                        position.get(
                            "peak_pnl",
                            0.0
                        )
                    )

                # =================================================
                # PERDIDA MAXIMA ABSOLUTA
                # =================================================

                if pnl <= -MAX_LOSS_USDT:

                    log(
                        f"{symbol} | "
                        f"MAX LOSS ACTIVADO | "
                        f"PnL={pnl:+.4f} | "
                        f"limite=-{MAX_LOSS_USDT:.2f}"
                    )

                    close_position(
                        symbol,
                        "MAX_LOSS_USDT"
                    )

                    continue

                # =================================================
                # ATR STOP
                # =================================================

                stop_price = float(
                    position.get(
                        "stop_price",
                        0.0
                    )
                )

                if stop_price > 0:

                    if (
                        position["side"]
                        == "LONG"
                        and current_price <= stop_price
                    ):

                        log(
                            f"{symbol} | "
                            f"ATR STOP LONG | "
                            f"price={current_price:.12f} "
                            f"stop={stop_price:.12f} "
                            f"PnL={pnl:+.4f}"
                        )

                        close_position(
                            symbol,
                            "ATR_STOP"
                        )

                        continue

                    if (
                        position["side"]
                        == "SHORT"
                        and current_price >= stop_price
                    ):

                        log(
                            f"{symbol} | "
                            f"ATR STOP SHORT | "
                            f"price={current_price:.12f} "
                            f"stop={stop_price:.12f} "
                            f"PnL={pnl:+.4f}"
                        )

                        close_position(
                            symbol,
                            "ATR_STOP"
                        )

                        continue

                # =================================================
                # PROFIT TRAILING
                #
                # +2 -> protege +0.50
                # +5 -> protege +3.50
                # +10 -> protege +8.50
                # =================================================

                if peak >= PROFIT_TRAIL_START_USDT:

                    protected = (
                        peak
                        - PROFIT_TRAIL_DISTANCE_USDT
                    )

                    with data_lock:

                        previous_protected = position.get(
                            "protected_pnl"
                        )

                        position[
                            "protected_pnl"
                        ] = protected

                        position[
                            "trail_active"
                        ] = True

                    # Log activation
                    if (
                        previous_protected is None
                        or protected
                        > float(previous_protected) + 0.01
                    ):

                        log(
                            f"{symbol} | "
                            f"PROFIT TRAIL | "
                            f"peak={peak:+.4f} | "
                            f"protege={protected:+.4f} | "
                            f"actual={pnl:+.4f}"
                        )

                    if pnl <= protected:

                        log(
                            f"{symbol} | "
                            f"PROFIT TRAIL EJECUTADO | "
                            f"PnL={pnl:+.4f} | "
                            f"protegido={protected:+.4f}"
                        )

                        close_position(
                            symbol,
                            "PROFIT_TRAIL"
                        )

                        continue

            time.sleep(1)

        except Exception as e:

            log(
                f"POSITION MANAGER ERROR: {e}"
            )

            time.sleep(1)


# ============================================================
# PROCESAR VELA BINANCE
# ============================================================

def process_binance_closed_candle(
    symbol,
    candle,
):

    with data_lock:

        candles = market_data[
            symbol
        ]["candles"]

        candle_time = candle["time"]

        # Si es la misma vela final que dejó KuCoin,
        # la reemplazamos con la vela Futures de Binance.
        if candles:

            if candles[-1]["time"] == candle_time:

                candles[-1] = candle

            elif candle_time > candles[-1]["time"]:

                candles.append(
                    candle
                )

            else:

                return

        else:

            candles.append(
                candle
            )

        candles[:] = candles[-MIN_CANDLES:]

        market_data[
            symbol
        ]["last_candle_time"] = candle_time

    log(
        f"{symbol} | "
        f"VELA FUTURES BINANCE CERRADA | "
        f"O={candle['open']:.12f} "
        f"H={candle['high']:.12f} "
        f"L={candle['low']:.12f} "
        f"C={candle['close']:.12f} "
        f"V={candle['volume']:.4f}"
    )

    # ========================================================
    # SI YA HAY POSICION, NO ABRIMOS OTRA.
    # ========================================================

    if total_open_positions() >= MAX_POSITIONS:

        return

    signal = calculate_signal(
        symbol
    )

    if signal is None:

        return

    log(
        f"{symbol} | "
        f"SEÑAL DETECTADA | "
        f"{signal['side']} | "
        f"{signal['score']}/7 | "
        f"price={signal['price']:.12f}"
    )

    # ========================================================
    # ANTI-ENTRADA TARDE
    #
    # Comparamos el precio vivo actual.
    # ========================================================

    with data_lock:

        live_price = float(
            market_data[symbol]["price"]
        )

    if live_price <= 0:
        return

    atr = signal["atr"]

    if signal["side"] == "LONG":

        if (
            live_price
            > signal["price"]
            + atr * MAX_ENTRY_DISTANCE_ATR
        ):

            log(
                f"{symbol} | "
                f"ENTRADA CANCELADA | "
                f"LONG demasiado extendido"
            )

            return

    else:

        if (
            live_price
            < signal["price"]
            - atr * MAX_ENTRY_DISTANCE_ATR
        ):

            log(
                f"{symbol} | "
                f"ENTRADA CANCELADA | "
                f"SHORT demasiado extendido"
            )

            return

    signal["price"] = live_price

    open_position(
        symbol,
        signal
    )


# ============================================================
# BINANCE MARKET WS
# ============================================================

def market_ws_worker(symbol):

    stream_name = (
        symbol.lower()
        + "@kline_5m"
    )

    url = (
        MARKET_WS_BASE
        + stream_name
    )

    while True:

        try:

            log(
                f"{symbol} | "
                f"Abriendo Market WS: "
                f"{url}"
            )

            def on_open(ws):

                log(
                    f"{symbol} | "
                    f"MARKET WS CONECTADO"
                )

                with data_lock:

                    market_data[
                        symbol
                    ]["ws_ready"] = True

            def on_message(
                ws,
                message
            ):

                try:

                    payload = json.loads(
                        message
                    )

                    if "data" in payload:

                        payload = payload[
                            "data"
                        ]

                    event = payload.get(
                        "e"
                    )

                    if event != "kline":
                        return

                    k = payload.get(
                        "k",
                        {}
                    )

                    open_time = int(
                        k["t"]
                    ) // 1000

                    open_price = float(
                        k["o"]
                    )

                    high_price = float(
                        k["h"]
                    )

                    low_price = float(
                        k["l"]
                    )

                    close_price = float(
                        k["c"]
                    )

                    volume = float(
                        k["v"]
                    )

                    taker_buy_volume = float(
                        k.get(
                            "V",
                            0
                        )
                    )

                    is_closed = bool(
                        k.get(
                            "x",
                            False
                        )
                    )

                    with data_lock:

                        market_data[
                            symbol
                        ]["price"] = close_price

                        market_data[
                            symbol
                        ]["closed"] = is_closed

                    # ==========================================
                    # ACTUALIZACION PNL IMPLICITA:
                    # position_manager lee market_data cada 1s.
                    # ==========================================

                    if not is_closed:

                        return

                    if volume > 0:

                        taker_ratio = (
                            taker_buy_volume
                            / volume
                        )

                    else:

                        taker_ratio = None

                    candle = {
                        "time": open_time,
                        "open": open_price,
                        "high": high_price,
                        "low": low_price,
                        "close": close_price,
                        "volume": volume,
                        "taker_buy_volume":
                            taker_buy_volume,
                        "taker_ratio":
                            taker_ratio,
                    }

                    process_binance_closed_candle(
                        symbol,
                        candle
                    )

                except Exception as e:

                    log(
                        f"{symbol} | "
                        f"Market WS message error: "
                        f"{e}"
                    )

            def on_error(
                ws,
                error
            ):

                log(
                    f"{symbol} | "
                    f"Market WS ERROR: {error}"
                )

            def on_close(
                ws,
                code,
                msg
            ):

                log(
                    f"{symbol} | "
                    f"Market WS cerrado | "
                    f"code={code} msg={msg}"
                )

                with data_lock:

                    market_data[
                        symbol
                    ]["ws_ready"] = False

            ws = websocket.WebSocketApp(
                url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )

            ws.run_forever(
                ping_interval=20,
                ping_timeout=10,
            )

        except Exception as e:

            log(
                f"{symbol} | "
                f"Market WS exception: {e}"
            )

        with data_lock:

            market_data[
                symbol
            ]["ws_ready"] = False

        time.sleep(5)


# ============================================================
# HEALTH SERVER
# ============================================================

def health_server():

    from http.server import (
        BaseHTTPRequestHandler,
        HTTPServer,
    )

    class Handler(
        BaseHTTPRequestHandler
    ):

        def do_GET(self):

            if self.path == "/health":

                with data_lock:

                    state = {
                        "live": LIVE_TRADING,
                        "symbols": SYMBOLS,
                        "balance_usdt":
                            balance_usdt,
                        "positions": len(
                            positions
                        ),
                        "ws_api":
                            ws_api_connected,
                        "user_stream":
                            user_stream_connected,
                        "markets": {
                            s: {
                                "price":
                                    market_data[s][
                                        "price"
                                    ],
                                "candles":
                                    len(
                                        market_data[s][
                                            "candles"
                                        ]
                                    ),
                                "history":
                                    market_data[s][
                                        "history_loaded"
                                    ],
                                "ws":
                                    market_data[s][
                                        "ws_ready"
                                    ],
                            }
                            for s in SYMBOLS
                        },
                    }

                body = json.dumps(
                    state
                ).encode()

                self.send_response(200)

                self.send_header(
                    "Content-Type",
                    "application/json"
                )

                self.send_header(
                    "Content-Length",
                    str(len(body))
                )

                self.end_headers()

                self.wfile.write(
                    body
                )

            else:

                self.send_response(200)

                self.end_headers()

                self.wfile.write(
                    b"OK"
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
            "8080"
        )
    )

    server = HTTPServer(
        ("0.0.0.0", port),
        Handler
    )

    log(
        f"Health server escuchando en {port}"
    )

    server.serve_forever()


# ============================================================
# STATUS
# ============================================================

def status_loop():

    while True:

        try:

            with data_lock:

                ws_ready = sum(
                    1
                    for s in SYMBOLS
                    if market_data[s][
                        "ws_ready"
                    ]
                )

                history_ready = sum(
                    1
                    for s in SYMBOLS
                    if market_data[s][
                        "history_loaded"
                    ]
                )

                active = len(
                    positions
                )

                balance = balance_usdt

                pos_text = []

                for symbol, p in positions.items():

                    pos_text.append(
                        f"{symbol} "
                        f"{p['side']} "
                        f"PnL={p.get('unrealized_pnl',0):+.3f} "
                        f"peak={p.get('peak_pnl',0):+.3f}"
                    )

            log(
                f"STATUS | "
                f"USDT={balance:.6f} | "
                f"HIST={history_ready}/{len(SYMBOLS)} | "
                f"WS={ws_ready}/{len(SYMBOLS)} | "
                f"POS={active}/{MAX_POSITIONS} | "
                f"MAX_LOSS=-{MAX_LOSS_USDT:.2f} | "
                f"TRAIL_START=+{PROFIT_TRAIL_START_USDT:.2f} | "
                f"TRAIL_DIST={PROFIT_TRAIL_DISTANCE_USDT:.2f}"
            )

            for text in pos_text:

                log(
                    f"POSITION | {text}"
                )

        except Exception as e:

            log(
                f"STATUS ERROR: {e}"
            )

        time.sleep(30)


# ============================================================
# MAIN
# ============================================================

def main():

    log(
        "=========================================================="
    )

    log(
        "BOT BINANCE FUTURES - MULTI MEME"
    )

    log(
        "=========================================================="
    )

    log(
        f"LIVE_TRADING={LIVE_TRADING}"
    )

    log(
        f"SYMBOLS={SYMBOLS}"
    )

    log(
        f"MAX_POSITIONS={MAX_POSITIONS}"
    )

    log(
        f"LEVERAGE={LEVERAGE}"
    )

    log(
        f"MAX_MARGIN_PERCENT={MAX_MARGIN_PERCENT}"
    )

    log(
        f"ATR_STOP_MULTIPLIER={ATR_STOP_MULTIPLIER}"
    )

    log(
        f"MAX_LOSS_USDT={MAX_LOSS_USDT}"
    )

    log(
        f"PROFIT_TRAIL_START_USDT="
        f"{PROFIT_TRAIL_START_USDT}"
    )

    log(
        f"PROFIT_TRAIL_DISTANCE_USDT="
        f"{PROFIT_TRAIL_DISTANCE_USDT}"
    )

    log(
        "HISTORIAL = KUCOIN FUTURES"
    )

    log(
        "LIVE = BINANCE FUTURES WEBSOCKET"
    )

    log(
        "=========================================================="
    )

    if not API_KEY or not API_SECRET:

        log(
            "ERROR: faltan credenciales Binance"
        )

        return

    # ========================================================
    # IP
    # ========================================================

    get_public_ip()

    # ========================================================
    # HEALTH
    # ========================================================

    threading.Thread(
        target=health_server,
        daemon=True,
    ).start()

    # ========================================================
    # WS API BINANCE
    # ========================================================

    connect_ws_api()

    # ========================================================
    # BALANCE
    # ========================================================

    update_balance()

    # ========================================================
    # HISTORIAL FUTURES
    #
    # ANTES DE ARRANCAR LAS SEÑALES.
    # ========================================================

    all_history_ok = True

    for symbol in SYMBOLS:

        ok = load_initial_history_from_kucoin(
            symbol
        )

        if not ok:

            all_history_ok = False

    if not all_history_ok:

        log(
            "ERROR: no se pudo cargar todo "
            "el historial Futures."
        )

        log(
            "No se inicia trading."
        )

        return

    log(
        "TODAS LAS VELAS INICIALES "
        "CARGADAS DESDE KUCOIN FUTURES"
    )

    # ========================================================
    # USER DATA
    # ========================================================

    threading.Thread(
        target=start_user_data_stream,
        daemon=True,
    ).start()

    # ========================================================
    # POSITION MANAGER
    # ========================================================

    threading.Thread(
        target=position_manager,
        daemon=True,
    ).start()

    # ========================================================
    # MARKET WS
    # ========================================================

    for symbol in SYMBOLS:

        threading.Thread(
            target=market_ws_worker,
            args=(symbol,),
            daemon=True,
        ).start()

    # ========================================================
    # STATUS
    # ========================================================

    threading.Thread(
        target=status_loop,
        daemon=True,
    ).start()

    # ========================================================
    # LOOP PRINCIPAL
    # ========================================================

    while True:

        try:

            time.sleep(60)

        except KeyboardInterrupt:

            log(
                "Bot detenido."
            )

            break

        except Exception as e:

            log(
                f"MAIN ERROR: {e}"
            )

            time.sleep(5)


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    main()
