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
# ESTRATEGIA: MOMENTUM / BREAKOUT MEME COINS - 5m
#
# BINANCE:
#   1000PEPEUSDT
#   WIFUSDT
#   1000BONKUSDT
#   1000FLOKIUSDT
#
# KUCOIN HISTORIAL:
#   PEPEUSDTM
#   WIFUSDTM
#   1000BONKUSDTM
#   FLOKIUSDTM
#
# ENTRADA:
#   EMA9 > EMA21 > EMA50      LONG
#   EMA9 < EMA21 < EMA50      SHORT
#   Ruptura máximo/mínimo vela anterior
#   Volumen >= media 20
#   VWAP
#   RSI
#
# RIESGO:
#   US$0.25 objetivo
#   ATR14 x 1.0
#
# SALIDA:
#   STOP LOCAL ATR
#   TRAILING ROI
#
# IMPORTANTE:
#   El leverage NO lo modifica el bot.
#   Se configura manualmente en Binance.
#
# NO USA:
#   Binance STOP_MARKET
#   Binance Algo Orders
#   REST Binance para mercado
# ============================================================


LIVE_TRADING = True
USE_TESTNET = False

INTERVAL = "5m"

MAX_POSITIONS = 1
MAX_MARGIN_PERCENT = 0.25
BUDGET_SPLIT = MAX_POSITIONS

MIN_CANDLES = 40

ATR_PERIOD = 14

EMA_FAST = 9
EMA_MID = 21
EMA_SLOW = 50

RSI_PERIOD = 14
VWAP_PERIOD = 20
VOLUME_PERIOD = 20

RECONNECT_SECONDS = 60


# ============================================================
# ESTRATEGIA
# ============================================================

RSI_LONG_MIN = 50.0
RSI_LONG_MAX = 75.0

RSI_SHORT_MIN = 25.0
RSI_SHORT_MAX = 50.0

VOLUME_RATIO_MIN = 1.00


# ============================================================
# RIESGO
# ============================================================

RISK_PER_TRADE_USDT = 0.25

ATR_STOP_MULTIPLIER = 1.00


# ============================================================
# TRAILING
# ============================================================

ROI_TRAILING_START = 10.0
ROI_TRAILING_DISTANCE = 12.0


# ============================================================
# BINANCE SYMBOLS
# ============================================================

SYMBOLS = [
    "1000PEPEUSDT",
    "WIFUSDT",
    "1000BONKUSDT",
    "1000FLOKIUSDT",
]


# ============================================================
# KUCOIN FUTURES
# ============================================================

KUCOIN_BASE_URL = "https://api-futures.kucoin.com"

KUCOIN_SYMBOLS = {
    "1000PEPEUSDT": "PEPEUSDTM",
    "WIFUSDT": "WIFUSDTM",
    "1000BONKUSDT": "1000BONKUSDTM",
    "1000FLOKIUSDT": "FLOKIUSDTM",
}


# Conversión de precio KuCoin -> unidad Binance
KUCOIN_PRICE_SCALE = {
    "1000PEPEUSDT": 1000.0,
    "WIFUSDT": 1.0,
    "1000BONKUSDT": 1.0,
    "1000FLOKIUSDT": 1000.0,
}


KUCOIN_GRANULARITY = 300


# ============================================================
# REGLAS LOCALES
# ============================================================

SYMBOL_RULES = {

    "1000PEPEUSDT": {
        "step": 1.0,
        "min_qty": 1.0,
        "max_qty": 100000000.0,
        "min_notional": 5.0,
        "tick": 0.000001,
    },

    "WIFUSDT": {
        "step": 1.0,
        "min_qty": 1.0,
        "max_qty": 1000000.0,
        "min_notional": 5.0,
        "tick": 0.0001,
    },

    "1000BONKUSDT": {
        "step": 1.0,
        "min_qty": 1.0,
        "max_qty": 100000000.0,
        "min_notional": 5.0,
        "tick": 0.000001,
    },

    "1000FLOKIUSDT": {
        "step": 1.0,
        "min_qty": 1.0,
        "max_qty": 100000000.0,
        "min_notional": 5.0,
        "tick": 0.000001,
    },
}


# ============================================================
# BINANCE CONNECTIONS
# ============================================================

if USE_TESTNET:

    BINANCE_MARKET_WS = (
        "wss://stream.binancefuture.com/market/ws/"
    )

    BINANCE_WS_API = (
        "wss://testnet.binancefuture.com/ws-fapi/v1"
    )

    BINANCE_PRIVATE_WS = (
        "wss://stream.binancefuture.com/private/ws/"
    )

else:

    BINANCE_MARKET_WS = (
        "wss://fstream.binance.com/market/ws/"
    )

    BINANCE_WS_API = (
        "wss://ws-fapi.binance.com/ws-fapi/v1"
    )

    BINANCE_PRIVATE_WS = (
        "wss://fstream.binance.com/private/ws/"
    )


# ============================================================
# API
# ============================================================

API_KEY = os.getenv(
    "BINANCE_API_KEY",
    ""
)

API_SECRET = os.getenv(
    "BINANCE_API_SECRET",
    ""
)


# ============================================================
# ESTADO
# ============================================================

market_data = {}

for symbol in SYMBOLS:

    market_data[symbol] = {
        "candles": [],
        "price": None,
        "last_signal": None,
        "last_candle_time": None,
    }


positions = {}

trade_history = []

symbol_leverage = {}

leverage_lock = threading.Lock()

ws_api_request_lock = threading.Lock()

user_stream_control = None

user_stream_control_lock = threading.Lock()

listen_key = None

ws_api_ready = threading.Event()


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

def get_public_ip():

    try:

        with urllib.request.urlopen(
            "https://api.ipify.org",
            timeout=10
        ) as response:

            return response.read().decode().strip()

    except Exception as e:

        log(
            f"IP pública no disponible: {e}"
        )

        return "N/A"


# ============================================================
# FIRMA
# ============================================================

def sign_params(params):

    query_string = urlencode(
        params
    )

    return hmac.new(
        API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


# ============================================================
# NORMALIZAR CANTIDAD
# ============================================================

def normalize_quantity(
    symbol,
    quantity
):

    rule = SYMBOL_RULES[symbol]

    step = rule["step"]

    min_qty = rule["min_qty"]

    max_qty = rule["max_qty"]

    if quantity <= 0:
        return 0.0

    quantity = (
        math.floor(
            quantity / step
        )
        * step
    )

    quantity = max(
        quantity,
        min_qty
    )

    quantity = min(
        quantity,
        max_qty
    )

    if step >= 1:
        decimals = 0
    else:
        decimals = max(
            0,
            int(
                round(
                    -math.log10(step)
                )
            )
        )

    return round(
        quantity,
        decimals
    )


# ============================================================
# NORMALIZAR PRECIO
# ============================================================

def normalize_price(
    symbol,
    price
):

    tick = SYMBOL_RULES[
        symbol
    ]["tick"]

    if tick <= 0:
        return price

    value = (
        math.floor(
            price / tick
        )
        * tick
    )

    if tick >= 1:
        decimals = 0
    else:
        decimals = max(
            0,
            int(
                round(
                    -math.log10(tick)
                )
            )
        )

    return round(
        value,
        decimals
    )


# ============================================================
# KUCOIN HISTORIAL
# ============================================================

def load_kucoin_history(symbol):

    kucoin_symbol = KUCOIN_SYMBOLS[
        symbol
    ]

    price_scale = KUCOIN_PRICE_SCALE[
        symbol
    ]

    end_at = int(
        time.time()
    )

    start_at = (
        end_at
        -
        (
            60
            *
            KUCOIN_GRANULARITY
        )
    )

    url = (
        f"{KUCOIN_BASE_URL}"
        f"/api/v1/kline/query"
        f"?symbol={kucoin_symbol}"
        f"&granularity={KUCOIN_GRANULARITY}"
        f"&from={start_at}"
        f"&to={end_at}"
    )

    log(
        f"{symbol} | "
        f"Cargando {kucoin_symbol}..."
    )

    try:

        request = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0"
            }
        )

        with urllib.request.urlopen(
            request,
            timeout=15
        ) as response:

            raw = response.read().decode()

        data = json.loads(raw)

        if data.get("code") != "200000":

            raise RuntimeError(
                f"KuCoin error: {data}"
            )

        rows = data.get(
            "data",
            []
        )

        if not rows:

            raise RuntimeError(
                "KuCoin no devolvió velas"
            )

        candles = []

        now = int(
            time.time()
        )

        for row in rows:

            if len(row) < 6:
                continue

            timestamp = int(
                row[0]
            )

            if (
                timestamp
                +
                KUCOIN_GRANULARITY
                >
                now
            ):
                continue

            candles.append({

                "timestamp":
                    timestamp * 1000,

                "open":
                    float(row[1])
                    * price_scale,

                "high":
                    float(row[2])
                    * price_scale,

                "low":
                    float(row[3])
                    * price_scale,

                "close":
                    float(row[4])
                    * price_scale,

                "volume":
                    float(row[5]),

                "taker_buy_volume":
                    None,
            })

        candles.sort(
            key=lambda x:
                x["timestamp"]
        )

        candles = candles[
            -MIN_CANDLES:
        ]

        if len(candles) < MIN_CANDLES:

            raise RuntimeError(
                f"Historial insuficiente: "
                f"{len(candles)}"
            )

        market_data[
            symbol
        ]["candles"] = candles

        market_data[
            symbol
        ]["last_candle_time"] = (
            candles[-1]["timestamp"]
        )

        market_data[
            symbol
        ]["price"] = (
            candles[-1]["close"]
        )

        log(
            f"{symbol} | "
            f"Historial OK: "
            f"{len(candles)} velas"
        )

        return True

    except Exception as e:

        log(
            f"{symbol} | "
            f"Error KuCoin: {e}"
        )

        return False


# ============================================================
# INDICADORES
# ============================================================

def calculate_indicators(candles):

    if len(candles) < MIN_CANDLES:
        return None

    df = pd.DataFrame(candles)

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
            span=EMA_MID,
            adjust=False
        )
        .mean()
    )

    df["ema50"] = (
        df["close"]
        .ewm(
            span=EMA_SLOW,
            adjust=False
        )
        .mean()
    )

    previous_close = (
        df["close"].shift(1)
    )

    tr1 = (
        df["high"]
        -
        df["low"]
    )

    tr2 = (
        df["high"]
        -
        previous_close
    ).abs()

    tr3 = (
        df["low"]
        -
        previous_close
    ).abs()

    true_range = pd.concat(
        [
            tr1,
            tr2,
            tr3
        ],
        axis=1
    ).max(axis=1)

    df["atr14"] = (
        true_range
        .rolling(
            ATR_PERIOD
        )
        .mean()
    )

    delta = (
        df["close"].diff()
    )

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

    avg_gain = (
        gain
        .ewm(
            alpha=1 / RSI_PERIOD,
            adjust=False
        )
        .mean()
    )

    avg_loss = (
        loss
        .ewm(
            alpha=1 / RSI_PERIOD,
            adjust=False
        )
        .mean()
    )

    rs = (
        avg_gain
        /
        avg_loss.replace(
            0,
            float("nan")
        )
    )

    df["rsi14"] = (
        100
        -
        (
            100
            /
            (1 + rs)
        )
    )

    typical_price = (
        df["high"]
        +
        df["low"]
        +
        df["close"]
    ) / 3.0

    volume_sum = (
        df["volume"]
        .rolling(
            VWAP_PERIOD
        )
        .sum()
    )

    pv_sum = (
        typical_price
        *
        df["volume"]
    ).rolling(
        VWAP_PERIOD
    ).sum()

    df["vwap"] = (
        pv_sum
        /
        volume_sum.replace(
            0,
            float("nan")
        )
    )

    df["volume_ma20"] = (
        df["volume"]
        .shift(1)
        .rolling(
            VOLUME_PERIOD
        )
        .mean()
    )

    df["volume_ratio"] = (
        df["volume"]
        /
        df["volume_ma20"].replace(
            0,
            float("nan")
        )
    )

    return df


# ============================================================
# SEÑAL
# ============================================================

def calculate_signal(symbol):

    candles = market_data[
        symbol
    ]["candles"]

    if len(candles) < MIN_CANDLES:
        return None

    df = calculate_indicators(
        candles
    )

    if df is None:
        return None

    current = df.iloc[-1]
    previous = df.iloc[-2]

    close = float(
        current["close"]
    )

    previous_high = float(
        previous["high"]
    )

    previous_low = float(
        previous["low"]
    )

    ema9 = float(
        current["ema9"]
    )

    ema21 = float(
        current["ema21"]
    )

    ema50 = float(
        current["ema50"]
    )

    rsi = float(
        current["rsi14"]
    )

    vwap = float(
        current["vwap"]
    )

    atr = float(
        current["atr14"]
    )

    volume_ratio = float(
        current["volume_ratio"]
    )

    values = [
        close,
        previous_high,
        previous_low,
        ema9,
        ema21,
        ema50,
        rsi,
        vwap,
        atr,
        volume_ratio,
    ]

    if any(
        math.isnan(x)
        for x in values
    ):
        return None

    if atr <= 0:
        return None

    bullish_trend = (
        ema9 > ema21
        and
        ema21 > ema50
    )

    bearish_trend = (
        ema9 < ema21
        and
        ema21 < ema50
    )

    bullish_breakout = (
        close > previous_high
    )

    bearish_breakout = (
        close < previous_low
    )

    volume_ok = (
        volume_ratio
        >=
        VOLUME_RATIO_MIN
    )

    bullish_rsi = (
        RSI_LONG_MIN
        <= rsi
        <= RSI_LONG_MAX
    )

    bearish_rsi = (
        RSI_SHORT_MIN
        <= rsi
        <= RSI_SHORT_MAX
    )

    log(
        f"{symbol} | "
        f"C={close:.10f} | "
        f"EMA9={ema9:.10f} | "
        f"EMA21={ema21:.10f} | "
        f"EMA50={ema50:.10f} | "
        f"RSI={rsi:.2f} | "
        f"VWAP={vwap:.10f} | "
        f"VOLx={volume_ratio:.2f} | "
        f"PH={previous_high:.10f} | "
        f"PL={previous_low:.10f}"
    )

    if (
        bullish_trend
        and
        bullish_breakout
        and
        volume_ok
        and
        close > vwap
        and
        bullish_rsi
    ):

        log(
            f"{symbol} | "
            f"SEÑAL LONG | "
            f"MOMENTUM BREAKOUT"
        )

        return {
            "side": "LONG",
            "atr": atr,
            "reference": previous_high,
            "setup":
                "MOMENTUM_BREAKOUT_LONG",
        }

    if (
        bearish_trend
        and
        bearish_breakout
        and
        volume_ok
        and
        close < vwap
        and
        bearish_rsi
    ):

        log(
            f"{symbol} | "
            f"SEÑAL SHORT | "
            f"MOMENTUM BREAKOUT"
        )

        return {
            "side": "SHORT",
            "atr": atr,
            "reference": previous_low,
            "setup":
                "MOMENTUM_BREAKOUT_SHORT",
        }

    return None


# ============================================================
# BINANCE WS API
# ============================================================

def ws_api_request(
    method,
    params=None,
    timeout=15
):

    if params is None:
        params = {}

    if not API_KEY or not API_SECRET:

        raise RuntimeError(
            "Faltan API KEY/SECRET"
        )

    with ws_api_request_lock:

        with user_stream_control_lock:

            ws = user_stream_control

        if ws is None:

            raise RuntimeError(
                "WS API no disponible"
            )

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
            "recvWindow"
        ] = 10000

        signature_params = {
            k: request_params[k]
            for k in sorted(
                request_params
            )
        }

        request_params[
            "signature"
        ] = sign_params(
            signature_params
        )

        payload = {
            "id": request_id,
            "method": method,
            "params": request_params,
        }

        ws.send(
            json.dumps(payload)
        )

        deadline = (
            time.time()
            +
            timeout
        )

        while time.time() < deadline:

            raw = ws.recv()

            if not raw:
                continue

            data = json.loads(raw)

            if (
                data.get("id")
                ==
                request_id
            ):

                if (
                    data.get("status")
                    != 200
                ):

                    raise RuntimeError(
                        f"{method} rechazado: "
                        f"{data}"
                    )

                return data

        raise TimeoutError(
            f"Timeout {method}"
        )


# ============================================================
# BALANCE
# ============================================================

def get_usdt_balance():

    try:

        response = ws_api_request(
            "v2/account.balance"
        )

        result = response.get(
            "result",
            {}
        )

        assets = result.get(
            "assets",
            []
        )

        for asset in assets:

            if asset.get(
                "asset"
            ) == "USDT":

                available = asset.get(
                    "availableBalance"
                )

                if available is not None:

                    return float(
                        available
                    )

                wallet = asset.get(
                    "walletBalance"
                )

                if wallet is not None:

                    return float(
                        wallet
                    )

        return None

    except Exception as e:

        log(
            f"Error balance: {e}"
        )

        return None


# ============================================================
# LEVERAGE - SOLO LECTURA
# ============================================================

def get_symbol_leverage(symbol):

    with leverage_lock:

        if symbol in symbol_leverage:

            return symbol_leverage[
                symbol
            ]

    try:

        response = ws_api_request(
            "account.status"
        )

        result = response.get(
            "result",
            {}
        )

        positions_data = result.get(
            "positions",
            []
        )

        for position in positions_data:

            if (
                position.get("symbol")
                ==
                symbol
            ):

                leverage = position.get(
                    "leverage"
                )

                if leverage is not None:

                    leverage = float(
                        leverage
                    )

                    with leverage_lock:

                        symbol_leverage[
                            symbol
                        ] = leverage

                    log(
                        f"{symbol} | "
                        f"Leverage leído: "
                        f"x{leverage:g}"
                    )

                    return leverage

        raise RuntimeError(
            f"No se encontró leverage "
            f"para {symbol}"
        )

    except Exception as e:

        log(
            f"{symbol} | "
            f"Error leverage: {e}"
        )

        return None


# ============================================================
# TAMAÑO DE POSICIÓN
# ============================================================

def calculate_position_size(
    symbol,
    atr
):

    balance = get_usdt_balance()

    if balance is None:
        return 0.0, None, None

    leverage = get_symbol_leverage(
        symbol
    )

    if (
        leverage is None
        or
        leverage <= 0
    ):

        return 0.0, None, None

    if atr <= 0:
        return 0.0, None, None

    stop_distance = (
        atr
        *
        ATR_STOP_MULTIPLIER
    )

    raw_qty = (
        RISK_PER_TRADE_USDT
        /
        stop_distance
    )

    quantity = normalize_quantity(
        symbol,
        raw_qty
    )

    price = market_data[
        symbol
    ]["price"]

    if (
        price is None
        or
        price <= 0
    ):

        return 0.0, None, None

    notional = (
        quantity
        *
        price
    )

    min_notional = SYMBOL_RULES[
        symbol
    ]["min_notional"]

    if notional < min_notional:

        quantity = normalize_quantity(
            symbol,
            min_notional / price
        )

        notional = (
            quantity
            *
            price
        )

    max_margin = (
        balance
        *
        MAX_MARGIN_PERCENT
        /
        BUDGET_SPLIT
    )

    estimated_margin = (
        notional
        /
        leverage
    )

    if estimated_margin > max_margin:

        max_notional = (
            max_margin
            *
            leverage
        )

        quantity = normalize_quantity(
            symbol,
            max_notional / price
        )

        notional = (
            quantity
            *
            price
        )

        estimated_margin = (
            notional
            /
            leverage
        )

    if quantity <= 0:
        return 0.0, None, None

    estimated_risk = (
        quantity
        *
        stop_distance
    )

    log(
        f"{symbol} | "
        f"QTY={quantity} | "
        f"NOTIONAL={notional:.6f} | "
        f"MARGIN≈{estimated_margin:.6f} | "
        f"RISK≈{estimated_risk:.6f} | "
        f"OBJETIVO={RISK_PER_TRADE_USDT:.2f} | "
        f"LEV=x{leverage:g}"
    )

    return (
        quantity,
        stop_distance,
        leverage
    )


# ============================================================
# MARKET ORDER
# ============================================================

def place_market_order(
    symbol,
    side,
    quantity,
    reduce_only=False
):

    if not LIVE_TRADING:

        return {
            "avgPrice":
                market_data[
                    symbol
                ]["price"],

            "executedQty":
                quantity,
        }

    order_side = (
        "BUY"
        if side == "LONG"
        else "SELL"
    )

    if reduce_only:

        order_side = (
            "SELL"
            if side == "LONG"
            else "BUY"
        )

    params = {
        "symbol": symbol,
        "side": order_side,
        "type": "MARKET",
        "quantity": quantity,
        "newOrderRespType": "RESULT",
    }

    if reduce_only:

        params[
            "reduceOnly"
        ] = "true"

    response = ws_api_request(
        "order.place",
        params=params,
        timeout=20
    )

    result = response.get(
        "result",
        {}
    )

    executed_qty = result.get(
        "executedQty"
    )

    avg_price = result.get(
        "avgPrice"
    )

    if executed_qty is None:

        executed_qty = result.get(
            "origQty",
            quantity
        )

    if avg_price is None:

        avg_price = result.get(
            "price"
        )

    if (
        avg_price is None
        or
        float(avg_price) <= 0
    ):

        avg_price = market_data[
            symbol
        ]["price"]

    return {
        "avgPrice":
            float(avg_price),

        "executedQty":
            float(executed_qty),

        "raw":
            result,
    }


# ============================================================
# ABRIR
# ============================================================

def open_position(
    symbol,
    signal
):

    if symbol in positions:
        return False

    if len(positions) >= MAX_POSITIONS:
        return False

    atr = signal["atr"]

    side = signal["side"]

    (
        quantity,
        stop_distance,
        leverage
    ) = calculate_position_size(
        symbol,
        atr
    )

    if quantity <= 0:
        return False

    log(
        f"{symbol} | "
        f"ABRIENDO {side} | "
        f"qty={quantity}"
    )

    try:

        result = place_market_order(
            symbol,
            side,
            quantity,
            False
        )

        entry = float(
            result["avgPrice"]
        )

        executed_qty = float(
            result["executedQty"]
        )

        if side == "LONG":

            stop_price = (
                entry
                -
                stop_distance
            )

        else:

            stop_price = (
                entry
                +
                stop_distance
            )

        stop_price = normalize_price(
            symbol,
            stop_price
        )

        positions[
            symbol
        ] = {

            "symbol": symbol,

            "side": side,

            "qty": executed_qty,

            "entry": entry,

            "atr": atr,

            "stop_distance":
                stop_distance,

            "stop_price":
                stop_price,

            "leverage":
                leverage,

            "opened_at":
                time.time(),

            "highest_price":
                entry,

            "lowest_price":
                entry,

            "peak_roi":
                0.0,

            "setup":
                signal["setup"],
        }

        log(
            f"{symbol} | "
            f"ABIERTA {side} | "
            f"ENTRY={entry:.10f} | "
            f"QTY={executed_qty} | "
            f"STOP LOCAL={stop_price:.10f}"
        )

        return True

    except Exception as e:

        log(
            f"{symbol} | "
            f"ERROR ABRIENDO: {e}"
        )

        return False


# ============================================================
# PNL
# ============================================================

def calculate_pnl(
    position,
    current_price
):

    entry = position["entry"]

    qty = position["qty"]

    if position["side"] == "LONG":

        return (
            current_price
            -
            entry
        ) * qty

    return (
        entry
        -
        current_price
    ) * qty


# ============================================================
# ROI
# ============================================================

def calculate_roi(
    position,
    current_price
):

    entry = position["entry"]

    leverage = position["leverage"]

    if entry <= 0:
        return 0.0

    if position["side"] == "LONG":

        change = (
            current_price
            -
            entry
        ) / entry

    else:

        change = (
            entry
            -
            current_price
        ) / entry

    return (
        change
        *
        leverage
        *
        100.0
    )


# ============================================================
# CERRAR
# ============================================================

def close_position(
    symbol,
    reason
):

    position = positions.get(
        symbol
    )

    if position is None:
        return False

    price = market_data[
        symbol
    ]["price"]

    if price is None:
        return False

    pnl = calculate_pnl(
        position,
        price
    )

    log(
        f"{symbol} | "
        f"CERRANDO | "
        f"reason={reason} | "
        f"PnL≈{pnl:.4f}"
    )

    try:

        result = place_market_order(
            symbol,
            position["side"],
            position["qty"],
            True
        )

        exit_price = float(
            result["avgPrice"]
        )

        executed_qty = float(
            result["executedQty"]
        )

        final_pnl = calculate_pnl(
            position,
            exit_price
        )

        trade_history.append({

            "symbol":
                symbol,

            "side":
                position["side"],

            "qty":
                executed_qty,

            "entry":
                position["entry"],

            "exit":
                exit_price,

            "pnl":
                final_pnl,

            "reason":
                reason,

            "opened_at":
                position["opened_at"],

            "closed_at":
                time.time(),
        })

        log(
            f"{symbol} | "
            f"CERRADA | "
            f"ENTRY={position['entry']:.10f} | "
            f"EXIT={exit_price:.10f} | "
            f"PNL={final_pnl:.4f} | "
            f"{reason}"
        )

        del positions[
            symbol
        ]

        return True

    except Exception as e:

        log(
            f"{symbol} | "
            f"ERROR CERRANDO: {e}"
        )

        return False


# ============================================================
# POSITION MANAGER
# ============================================================

def position_manager():

    while True:

        try:

            for symbol in list(
                positions.keys()
            ):

                position = positions.get(
                    symbol
                )

                if position is None:
                    continue

                price = market_data[
                    symbol
                ]["price"]

                if price is None:
                    continue

                side = position["side"]

                if side == "LONG":

                    position[
                        "highest_price"
                    ] = max(
                        position[
                            "highest_price"
                        ],
                        price
                    )

                else:

                    position[
                        "lowest_price"
                    ] = min(
                        position[
                            "lowest_price"
                        ],
                        price
                    )

                pnl = calculate_pnl(
                    position,
                    price
                )

                roi = calculate_roi(
                    position,
                    price
                )

                position[
                    "peak_roi"
                ] = max(
                    position[
                        "peak_roi"
                    ],
                    roi
                )

                stop_price = position[
                    "stop_price"
                ]

                if side == "LONG":

                    stop_hit = (
                        price
                        <=
                        stop_price
                    )

                else:

                    stop_hit = (
                        price
                        >=
                        stop_price
                    )

                if stop_hit:

                    close_position(
                        symbol,
                        "ATR_STOP_LOCAL"
                    )

                    continue

                if (
                    roi
                    >=
                    ROI_TRAILING_START
                ):

                    protected_roi = (
                        position[
                            "peak_roi"
                        ]
                        -
                        ROI_TRAILING_DISTANCE
                    )

                    if roi <= protected_roi:

                        close_position(
                            symbol,
                            "ROI_TRAILING"
                        )

                        continue

                log(
                    f"{symbol} | "
                    f"{side} | "
                    f"PRICE={price:.10f} | "
                    f"PNL={pnl:.4f} | "
                    f"ROI={roi:.2f}% | "
                    f"PEAK={position['peak_roi']:.2f}%"
                )

        except Exception as e:

            log(
                f"Position manager error: {e}"
            )

        time.sleep(1)


# ============================================================
# VELA CERRADA
# ============================================================

def process_closed_candle(
    symbol,
    candle
):

    timestamp = candle[
        "timestamp"
    ]

    last_timestamp = market_data[
        symbol
    ]["last_candle_time"]

    if (
        last_timestamp is not None
        and
        timestamp <= last_timestamp
    ):

        return

    market_data[
        symbol
    ]["candles"].append(
        candle
    )

    market_data[
        symbol
    ]["candles"] = market_data[
        symbol
    ]["candles"][
        -MIN_CANDLES:
    ]

    market_data[
        symbol
    ]["last_candle_time"] = (
        timestamp
    )

    market_data[
        symbol
    ]["price"] = (
        candle["close"]
    )

    log(
        f"{symbol} | "
        f"VELA 5m | "
        f"O={candle['open']:.10f} | "
        f"H={candle['high']:.10f} | "
        f"L={candle['low']:.10f} | "
        f"C={candle['close']:.10f}"
    )

    if symbol in positions:
        return

    if len(positions) >= MAX_POSITIONS:
        return

    signal = calculate_signal(
        symbol
    )

    market_data[
        symbol
    ]["last_signal"] = signal

    if signal is None:
        return

    open_position(
        symbol,
        signal
    )


# ============================================================
# MARKET WEBSOCKET
# ============================================================

def market_ws_loop(symbol):

    stream_name = (
        f"{symbol.lower()}"
        f"@kline_{INTERVAL}"
    )

    url = (
        BINANCE_MARKET_WS
        +
        stream_name
    )

    while True:

        ws = None

        try:

            log(
                f"{symbol} | "
                f"Conectando Market WS..."
            )

            ws = websocket.create_connection(
                url,
                timeout=30,
                origin="https://www.binance.com"
            )

            log(
                f"{symbol} | "
                f"Market WS conectado."
            )

            while True:

                raw = ws.recv()

                if not raw:
                    continue

                data = json.loads(
                    raw
                )

                if "k" in data:

                    kline = data["k"]

                elif (
                    "data" in data
                    and
                    isinstance(
                        data["data"],
                        dict
                    )
                    and
                    "k" in data["data"]
                ):

                    kline = data[
                        "data"
                    ]["k"]

                else:

                    continue

                current_price = float(
                    kline["c"]
                )

                market_data[
                    symbol
                ]["price"] = (
                    current_price
                )

                if not kline["x"]:
                    continue

                candle = {

                    "timestamp":
                        int(kline["t"]),

                    "open":
                        float(kline["o"]),

                    "high":
                        float(kline["h"]),

                    "low":
                        float(kline["l"]),

                    "close":
                        float(kline["c"]),

                    "volume":
                        float(kline["v"]),

                    "taker_buy_volume":
                        float(
                            kline.get(
                                "V",
                                0
                            )
                        ),
                }

                process_closed_candle(
                    symbol,
                    candle
                )

        except Exception as e:

            log(
                f"{symbol} | "
                f"Market WS error: {e}"
            )

        finally:

            try:

                if ws:
                    ws.close()

            except Exception:
                pass

        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# USER DATA STREAM
# ============================================================

def start_user_data_stream():

    global user_stream_control
    global listen_key

    try:

        log(
            "Abriendo conexión WS API "
            "para User Data Stream..."
        )

        ws = websocket.create_connection(
            BINANCE_WS_API,
            timeout=30,
            origin="https://www.binance.com"
        )

        with user_stream_control_lock:

            user_stream_control = ws

        request_id = str(
            uuid.uuid4()
        )

        payload = {

            "id":
                request_id,

            "method":
                "userDataStream.start",

            "params": {
                "apiKey":
                    API_KEY
            },
        }

        ws.send(
            json.dumps(payload)
        )

        deadline = (
            time.time()
            +
            20
        )

        while time.time() < deadline:

            raw = ws.recv()

            if not raw:
                continue

            response = json.loads(raw)

            if (
                response.get("id")
                ==
                request_id
            ):

                break

        if (
            response.get("status")
            != 200
        ):

            raise RuntimeError(
                f"UserDataStream.start "
                f"rechazado: {response}"
            )

        listen_key = response[
            "result"
        ]["listenKey"]

        log(
            "User Data Stream iniciado."
        )

        ws_api_ready.set()

        return True

    except Exception as e:

        log(
            f"UserDataStream.start "
            f"error: {e}"
        )

        with user_stream_control_lock:

            user_stream_control = None

        ws_api_ready.clear()

        return False


# ============================================================
# USER DATA SOCKET
# ============================================================

def user_data_socket_loop():

    global listen_key

    while True:

        if not listen_key:

            time.sleep(5)

            continue

        ws = None

        try:

            url = (
                BINANCE_PRIVATE_WS
                +
                listen_key
            )

            log(
                "Conectando User Data Stream..."
            )

            ws = websocket.create_connection(
                url,
                timeout=60,
                origin="https://www.binance.com"
            )

            log(
                "User Data Stream conectado."
            )

            while True:

                raw = ws.recv()

                if not raw:
                    continue

                data = json.loads(
                    raw
                )

                event_type = data.get(
                    "e"
                )

                if (
                    event_type
                    ==
                    "ACCOUNT_UPDATE"
                ):

                    account = data.get(
                        "a",
                        {}
                    )

                    balances = account.get(
                        "B",
                        []
                    )

                    for balance in balances:

                        if (
                            balance.get("a")
                            ==
                            "USDT"
                        ):

                            log(
                                "ACCOUNT_UPDATE | "
                                f"USDT="
                                f"{balance.get('cw')}"
                            )

                elif (
                    event_type
                    ==
                    "ORDER_TRADE_UPDATE"
                ):

                    order = data.get(
                        "o",
                        {}
                    )

                    symbol = order.get(
                        "s"
                    )

                    if symbol in SYMBOLS:

                        log(
                            "ORDER_UPDATE | "
                            f"{symbol} | "
                            f"side={order.get('S')} | "
                            f"status={order.get('X')} | "
                            f"qty={order.get('z')} | "
                            f"avg={order.get('ap')}"
                        )

        except Exception as e:

            log(
                f"User Data error: {e}"
            )

        finally:

            try:

                if ws:
                    ws.close()

            except Exception:
                pass

        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# KEEPALIVE
# ============================================================

def user_stream_keepalive_loop():

    while True:

        time.sleep(
            45 * 60
        )

        try:

            if not listen_key:
                continue

            ws_api_request(
                "userDataStream.ping",
                {
                    "listenKey":
                        listen_key
                }
            )

            log(
                "User Data keepalive OK."
            )

        except Exception as e:

            log(
                f"Keepalive error: {e}"
            )


# ============================================================
# STATUS
# ============================================================

def status_loop():

    while True:

        try:

            balance = get_usdt_balance()

            public_ip = get_public_ip()

            log(
                "STATUS | "
                f"USDT={balance} | "
                f"POSITIONS="
                f"{len(positions)}/"
                f"{MAX_POSITIONS} | "
                f"WS_READY="
                f"{ws_api_ready.is_set()} | "
                f"IP={public_ip}"
            )

            for symbol in SYMBOLS:

                price = market_data[
                    symbol
                ]["price"]

                leverage = symbol_leverage.get(
                    symbol
                )

                log(
                    f"STATUS | "
                    f"{symbol} | "
                    f"price={price} | "
                    f"lev={leverage}"
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

    from http.server import (
        BaseHTTPRequestHandler,
        HTTPServer
    )

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    class Handler(
        BaseHTTPRequestHandler
    ):

        def do_GET(self):

            self.send_response(200)

            self.send_header(
                "Content-Type",
                "text/plain"
            )

            self.end_headers()

            self.wfile.write(
                b"BOT OK\n"
            )

        def log_message(
            self,
            format,
            *args
        ):

            return

    server = HTTPServer(
        (
            "0.0.0.0",
            port
        ),
        Handler
    )

    log(
        f"Health server en {port}"
    )

    server.serve_forever()


# ============================================================
# HISTORIAL
# ============================================================

def initialize_history():

    success = 0

    for symbol in SYMBOLS:

        if load_kucoin_history(
            symbol
        ):

            success += 1

        time.sleep(0.5)

    log(
        f"Historial: "
        f"{success}/{len(SYMBOLS)}"
    )

    return (
        success
        ==
        len(SYMBOLS)
    )


# ============================================================
# MAIN
# ============================================================

def main():

    log(
        "=================================================="
    )

    log(
        "BINANCE FUTURES MEME MOMENTUM BOT"
    )

    log(
        f"SYMBOLS={SYMBOLS}"
    )

    log(
        f"LIVE_TRADING={LIVE_TRADING}"
    )

    log(
        f"INTERVAL={INTERVAL}"
    )

    log(
        f"MAX_POSITIONS={MAX_POSITIONS}"
    )

    log(
        f"RISK={RISK_PER_TRADE_USDT} USDT"
    )

    log(
        f"ATR STOP={ATR_STOP_MULTIPLIER}x"
    )

    log(
        "LEVERAGE MANUAL - "
        "EL BOT NO LO MODIFICA"
    )

    log(
        "=================================================="
    )

    log(
        f"IP pública: "
        f"{get_public_ip()}"
    )

    threading.Thread(
        target=health_server,
        daemon=True
    ).start()

    threading.Thread(
        target=start_user_data_stream,
        daemon=True
    ).start()

    log(
        "Esperando Binance WS API..."
    )

    ws_api_ready.wait(
        timeout=30
    )

    threading.Thread(
        target=user_data_socket_loop,
        daemon=True
    ).start()

    threading.Thread(
        target=user_stream_keepalive_loop,
        daemon=True
    ).start()

    if not initialize_history():

        log(
            "ERROR: no se pudo cargar "
            "todo el historial."
        )

        return

    for symbol in SYMBOLS:

        try:

            get_symbol_leverage(
                symbol
            )

        except Exception as e:

            log(
                f"{symbol} | "
                f"Leverage no leído: "
                f"{e}"
            )

    for symbol in SYMBOLS:

        threading.Thread(
            target=market_ws_loop,
            args=(symbol,),
            daemon=True
        ).start()

        time.sleep(0.5)

    threading.Thread(
        target=position_manager,
        daemon=True
    ).start()

    threading.Thread(
        target=status_loop,
        daemon=True
    ).start()

    log(
        "BOT OPERATIVO."
    )

    log(
        "Esperando BREAKOUT + MOMENTUM..."
    )

    while True:

        try:

            time.sleep(10)

        except KeyboardInterrupt:

            log(
                "BOT DETENIDO."
            )

            break

        except Exception as e:

            log(
                f"Main loop error: {e}"
            )

            time.sleep(5)


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        log(
            "BOT DETENIDO."
        )

    except Exception as e:

        log(
            f"ERROR FATAL: {e}"
        )

        raise
