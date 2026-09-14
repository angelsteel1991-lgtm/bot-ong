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
# ESTRATEGIA:
#   EMA 9/21 + RSI 14 + VWAP
#
# LONG:
#   1) EMA9 cruza arriba de EMA21
#   2) Cierre arriba de EMA9 y EMA21
#   3) RSI 14 entre 50 y 70
#   4) Cierre arriba de VWAP
#
# SHORT:
#   1) EMA9 cruza abajo de EMA21
#   2) Cierre debajo de EMA9 y EMA21
#   3) RSI 14 entre 30 y 50
#   4) Cierre debajo de VWAP
#
# NO:
#   - 6/6
#   - 7 condiciones
#   - Donchian
#   - volumen obligatorio
#   - momentum inventado
#   - cambio de leverage desde el bot
#
# RIESGO:
#   Objetivo por operación: US$0.25
#   Stop local: 1 ATR
#
# IMPORTANTE:
#   El leverage se configura MANUALMENTE en Binance.
#   El bot solamente lo consulta.
#
# ============================================================


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

LIVE_TRADING = True
USE_TESTNET = False

INTERVAL = "5m"

MAX_POSITIONS = 1

# Máximo porcentaje del balance utilizado como margen.
MAX_MARGIN_PERCENT = 0.25

BUDGET_SPLIT = MAX_POSITIONS

MIN_CANDLES = 40

ATR_PERIOD = 14

EMA_FAST = 9
EMA_SLOW = 21

RSI_PERIOD = 14

VWAP_PERIOD = 20

RECONNECT_SECONDS = 60


# ============================================================
# ESTRATEGIA
# ============================================================

RSI_LONG_MIN = 50.0
RSI_LONG_MAX = 70.0

RSI_SHORT_MIN = 30.0
RSI_SHORT_MAX = 50.0


# ============================================================
# RIESGO
# ============================================================

RISK_PER_TRADE_USDT = 0.25

ATR_STOP_MULTIPLIER = 1.00


# ============================================================
# PROTECCIÓN DE GANANCIA
# ============================================================

ROI_TRAILING_START = 2.0
ROI_TRAILING_DISTANCE = 1.5


# ============================================================
# SÍMBOLOS
# ============================================================

SYMBOLS = [
    "ETHUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
]


# ============================================================
# KUCOIN FUTURES
# ============================================================

KUCOIN_BASE_URL = "https://api-futures.kucoin.com"

KUCOIN_SYMBOLS = {
    "ETHUSDT": "ETHUSDTM",
    "SOLUSDT": "SOLUSDTM",
    "XRPUSDT": "XRPUSDTM",
    "DOGEUSDT": "DOGEUSDTM",
}

KUCOIN_GRANULARITY = 300


# ============================================================
# REGLAS LOCALES DE CANTIDAD
#
# Se utilizan únicamente para normalizar cantidades.
# NO se utilizan para inventar reglas de Binance.
# ============================================================

SYMBOL_RULES = {
    "ETHUSDT": {
        "step": 0.001,
        "min_qty": 0.001,
        "max_qty": 100000.0,
        "min_notional": 5.0,
        "tick": 0.01,
    },

    "SOLUSDT": {
        "step": 0.01,
        "min_qty": 0.01,
        "max_qty": 100000.0,
        "min_notional": 5.0,
        "tick": 0.01,
    },

    "XRPUSDT": {
        "step": 1.0,
        "min_qty": 1.0,
        "max_qty": 50000000.0,
        "min_notional": 5.0,
        "tick": 0.0001,
    },

    "DOGEUSDT": {
        "step": 1.0,
        "min_qty": 1.0,
        "max_qty": 50000000.0,
        "min_notional": 5.0,
        "tick": 0.00001,
    },
}


# ============================================================
# BINANCE
# ============================================================

if USE_TESTNET:
    BINANCE_MARKET_WS = "wss://stream.binancefuture.com/market/ws/"
    BINANCE_WS_API = "wss://testnet.binancefuture.com/ws-fapi/v1"
    BINANCE_PRIVATE_WS = "wss://stream.binancefuture.com/private/ws/"
else:
    BINANCE_MARKET_WS = "wss://fstream.binance.com/market/ws/"
    BINANCE_WS_API = "wss://ws-fapi.binance.com/ws-fapi/v1"
    BINANCE_PRIVATE_WS = "wss://fstream.binance.com/private/ws/"


API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")


# ============================================================
# ESTADO GLOBAL
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
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] {message}", flush=True)


# ============================================================
# PUBLIC IP
# ============================================================

def get_public_ip():
    try:
        with urllib.request.urlopen(
            "https://api.ipify.org",
            timeout=10
        ) as response:
            return response.read().decode().strip()

    except Exception as e:
        log(f"IP pública no disponible: {e}")
        return "N/A"


# ============================================================
# HMAC BINANCE WS
# ============================================================

def sign_params(params):
    query_string = urlencode(params)

    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    return signature


# ============================================================
# NORMALIZAR CANTIDAD
# ============================================================

def normalize_quantity(symbol, quantity):

    rule = SYMBOL_RULES[symbol]

    step = rule["step"]
    min_qty = rule["min_qty"]
    max_qty = rule["max_qty"]

    if quantity <= 0:
        return 0.0

    quantity = math.floor(quantity / step) * step

    quantity = max(quantity, min_qty)

    quantity = min(quantity, max_qty)

    decimals = max(
        0,
        int(round(-math.log10(step))) if step < 1 else 0
    )

    return round(quantity, decimals)


# ============================================================
# NORMALIZAR PRECIO
# ============================================================

def normalize_price(symbol, price):

    tick = SYMBOL_RULES[symbol]["tick"]

    if tick <= 0:
        return price

    value = math.floor(price / tick) * tick

    decimals = max(
        0,
        int(round(-math.log10(tick))) if tick < 1 else 0
    )

    return round(value, decimals)


# ============================================================
# KUCOIN HISTÓRICO
# ============================================================

def load_kucoin_history(symbol):

    kucoin_symbol = KUCOIN_SYMBOLS[symbol]

    end_at = int(time.time())

    start_at = end_at - (60 * KUCOIN_GRANULARITY)

    url = (
        f"{KUCOIN_BASE_URL}/api/v1/kline/query"
        f"?symbol={kucoin_symbol}"
        f"&granularity={KUCOIN_GRANULARITY}"
        f"&from={start_at}"
        f"&to={end_at}"
    )

    log(
        f"{symbol} | Cargando historial KuCoin Futures "
        f"{kucoin_symbol}..."
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

        rows = data.get("data", [])

        if not rows:
            raise RuntimeError(
                "KuCoin no devolvió velas"
            )

        candles = []

        now = int(time.time())

        for row in rows:

            if len(row) < 6:
                continue

            timestamp = int(row[0])

            open_price = float(row[1])
            high_price = float(row[2])
            low_price = float(row[3])
            close_price = float(row[4])
            volume = float(row[5])

            # No usar vela todavía abierta.
            if timestamp + KUCOIN_GRANULARITY > now:
                continue

            candles.append({
                "timestamp": timestamp * 1000,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "taker_buy_volume": None,
            })

        candles.sort(
            key=lambda x: x["timestamp"]
        )

        candles = candles[-MIN_CANDLES:]

        if len(candles) < MIN_CANDLES:
            raise RuntimeError(
                f"Historial insuficiente: {len(candles)} velas"
            )

        market_data[symbol]["candles"] = candles

        market_data[symbol]["last_candle_time"] = (
            candles[-1]["timestamp"]
        )

        market_data[symbol]["price"] = (
            candles[-1]["close"]
        )

        log(
            f"{symbol} | OK historial: "
            f"{len(candles)} velas"
        )

        return True

    except Exception as e:

        log(
            f"{symbol} | Error historial KuCoin: {e}"
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
            span=EMA_SLOW,
            adjust=False
        )
        .mean()
    )

    # --------------------------------------------------------
    # ATR 14
    # --------------------------------------------------------

    previous_close = df["close"].shift(1)

    tr1 = df["high"] - df["low"]

    tr2 = (
        df["high"] - previous_close
    ).abs()

    tr3 = (
        df["low"] - previous_close
    ).abs()

    true_range = pd.concat(
        [tr1, tr2, tr3],
        axis=1
    ).max(axis=1)

    df["atr14"] = (
        true_range
        .rolling(ATR_PERIOD)
        .mean()
    )

    # --------------------------------------------------------
    # RSI 14
    # --------------------------------------------------------

    delta = df["close"].diff()

    gain = delta.clip(lower=0)

    loss = -delta.clip(upper=0)

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

    rs = avg_gain / avg_loss.replace(
        0,
        float("nan")
    )

    df["rsi14"] = 100 - (
        100 / (1 + rs)
    )

    # --------------------------------------------------------
    # VWAP rolling 20
    # --------------------------------------------------------

    typical_price = (
        df["high"]
        + df["low"]
        + df["close"]
    ) / 3.0

    volume_sum = (
        df["volume"]
        .rolling(VWAP_PERIOD)
        .sum()
    )

    pv_sum = (
        typical_price * df["volume"]
    ).rolling(VWAP_PERIOD).sum()

    df["vwap"] = (
        pv_sum / volume_sum.replace(
            0,
            float("nan")
        )
    )

    return df


# ============================================================
# ESTRATEGIA
# ============================================================

def calculate_signal(symbol):

    candles = market_data[symbol]["candles"]

    if len(candles) < MIN_CANDLES:
        return None

    df = calculate_indicators(candles)

    if df is None or len(df) < 3:
        return None

    # --------------------------------------------------------
    # ÚLTIMA VELA CERRADA
    # --------------------------------------------------------

    current = df.iloc[-1]

    previous = df.iloc[-2]

    close = float(current["close"])

    ema9 = float(current["ema9"])

    ema21 = float(current["ema21"])

    previous_ema9 = float(previous["ema9"])

    previous_ema21 = float(previous["ema21"])

    rsi = float(current["rsi14"])

    vwap = float(current["vwap"])

    atr = float(current["atr14"])

    if any(
        math.isnan(x)
        for x in [
            close,
            ema9,
            ema21,
            previous_ema9,
            previous_ema21,
            rsi,
            vwap,
            atr,
        ]
    ):
        return None

    if atr <= 0:
        return None

    # --------------------------------------------------------
    # CRUCE REAL EMA 9 / EMA 21
    # --------------------------------------------------------

    bullish_cross = (
        previous_ema9 <= previous_ema21
        and
        ema9 > ema21
    )

    bearish_cross = (
        previous_ema9 >= previous_ema21
        and
        ema9 < ema21
    )

    # --------------------------------------------------------
    # LONG
    #
    # 1. EMA9 cruza arriba EMA21
    # 2. Precio arriba de EMA9 y EMA21
    # 3. RSI 50-70
    # 4. Precio arriba VWAP
    # --------------------------------------------------------

    long_signal = (
        bullish_cross
        and
        close > ema9
        and
        close > ema21
        and
        RSI_LONG_MIN < rsi < RSI_LONG_MAX
        and
        close > vwap
    )

    # --------------------------------------------------------
    # SHORT
    #
    # 1. EMA9 cruza abajo EMA21
    # 2. Precio debajo EMA9 y EMA21
    # 3. RSI 30-50
    # 4. Precio debajo VWAP
    # --------------------------------------------------------

    short_signal = (
        bearish_cross
        and
        close < ema9
        and
        close < ema21
        and
        RSI_SHORT_MIN < rsi < RSI_SHORT_MAX
        and
        close < vwap
    )

    # --------------------------------------------------------
    # LOG DEL SETUP
    # --------------------------------------------------------

    log(
        f"{symbol} | "
        f"CLOSE={close:.8f} | "
        f"EMA9={ema9:.8f} | "
        f"EMA21={ema21:.8f} | "
        f"RSI={rsi:.2f} | "
        f"VWAP={vwap:.8f}"
    )

    # --------------------------------------------------------
    # SEÑAL LONG
    # --------------------------------------------------------

    if long_signal:

        log(
            f"{symbol} | "
            f"SEÑAL LONG | "
            f"EMA9 CROSS UP | "
            f"PRICE>EMA9/EMA21 | "
            f"PRICE>VWAP | "
            f"RSI={rsi:.2f}"
        )

        return {
            "side": "LONG",
            "atr": atr,
            "reference": ema21,
            "setup": "EMA_CROSS_RSI_VWAP",
        }

    # --------------------------------------------------------
    # SEÑAL SHORT
    # --------------------------------------------------------

    if short_signal:

        log(
            f"{symbol} | "
            f"SEÑAL SHORT | "
            f"EMA9 CROSS DOWN | "
            f"PRICE<EMA9/EMA21 | "
            f"PRICE<VWAP | "
            f"RSI={rsi:.2f}"
        )

        return {
            "side": "SHORT",
            "atr": atr,
            "reference": ema21,
            "setup": "EMA_CROSS_RSI_VWAP",
        }

    return None


# ============================================================
# WS API REQUEST
# ============================================================

def ws_api_request(
    method,
    params=None,
    timeout=15
):

    global user_stream_control

    if params is None:
        params = {}

    if not API_KEY or not API_SECRET:
        raise RuntimeError(
            "BINANCE_API_KEY / BINANCE_API_SECRET no configuradas"
        )

    with ws_api_request_lock:

        with user_stream_control_lock:

            ws = user_stream_control

        if ws is None:
            raise RuntimeError(
                "WS API no disponible"
            )

        request_id = str(uuid.uuid4())

        timestamp = int(time.time() * 1000)

        request_params = dict(params)

        request_params["apiKey"] = API_KEY

        request_params["timestamp"] = timestamp

        request_params["recvWindow"] = 10000

        signature_params = {
            k: request_params[k]
            for k in sorted(request_params)
        }

        signature = sign_params(
            signature_params
        )

        request_params["signature"] = signature

        payload = {
            "id": request_id,
            "method": method,
            "params": request_params,
        }

        ws.send(
            json.dumps(payload)
        )

        deadline = time.time() + timeout

        while time.time() < deadline:

            remaining = max(
                0.1,
                deadline - time.time()
            )

            ws.settimeout(remaining)

            raw = ws.recv()

            if not raw:
                continue

            data = json.loads(raw)

            if data.get("id") == request_id:

                if data.get("status") != 200:

                    raise RuntimeError(
                        f"{method} rechazado: {data}"
                    )

                return data

        raise TimeoutError(
            f"Timeout WS API: {method}"
        )


# ============================================================
# BALANCE USDT
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

        balances = result.get(
            "assets",
            []
        )

        for asset in balances:

            if asset.get("asset") == "USDT":

                available = asset.get(
                    "availableBalance"
                )

                if available is not None:
                    return float(available)

                wallet = asset.get(
                    "walletBalance"
                )

                if wallet is not None:
                    return float(wallet)

        return None

    except Exception as e:

        log(
            f"Error balance USDT: {e}"
        )

        return None


# ============================================================
# LEVERAGE
#
# IMPORTANTE:
# NO SE MODIFICA.
# SOLO SE LEE EL CONFIGURADO EN BINANCE.
# ============================================================

def get_symbol_leverage(symbol):

    with leverage_lock:

        if symbol in symbol_leverage:

            return symbol_leverage[symbol]

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

        found = None

        for position in positions_data:

            if position.get("symbol") == symbol:

                leverage = position.get(
                    "leverage"
                )

                if leverage is not None:

                    found = float(leverage)

                    break

        if found is None:

            raise RuntimeError(
                f"No se encontró leverage para {symbol}"
            )

        with leverage_lock:

            symbol_leverage[symbol] = found

        log(
            f"{symbol} | "
            f"Leverage Binance detectado: x{found:g} "
            f"(NO modificado por el bot)"
        )

        return found

    except Exception as e:

        log(
            f"{symbol} | Error leyendo leverage: {e}"
        )

        return None


# ============================================================
# CALCULAR TAMAÑO
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

    if leverage is None or leverage <= 0:
        return 0.0, None, None

    if atr <= 0:
        return 0.0, None, None

    # --------------------------------------------------------
    # DISTANCIA DEL STOP
    # --------------------------------------------------------

    stop_distance = (
        atr * ATR_STOP_MULTIPLIER
    )

    # --------------------------------------------------------
    # TAMAÑO BASADO EN RIESGO
    # --------------------------------------------------------

    raw_qty = (
        RISK_PER_TRADE_USDT
        / stop_distance
    )

    quantity = normalize_quantity(
        symbol,
        raw_qty
    )

    if quantity <= 0:
        return 0.0, None, None

    # --------------------------------------------------------
    # PRECIO ACTUAL
    # --------------------------------------------------------

    price = market_data[symbol]["price"]

    if price is None or price <= 0:
        return 0.0, None, None

    notional = (
        quantity * price
    )

    min_notional = (
        SYMBOL_RULES[symbol]["min_notional"]
    )

    if notional < min_notional:

        quantity = normalize_quantity(
            symbol,
            min_notional / price
        )

        notional = (
            quantity * price
        )

    # --------------------------------------------------------
    # MARGEN MÁXIMO
    # --------------------------------------------------------

    max_margin = (
        balance
        * MAX_MARGIN_PERCENT
        / BUDGET_SPLIT
    )

    estimated_margin = (
        notional / leverage
    )

    if estimated_margin > max_margin:

        max_notional = (
            max_margin * leverage
        )

        quantity = normalize_quantity(
            symbol,
            max_notional / price
        )

        if quantity <= 0:
            return 0.0, None, None

        notional = (
            quantity * price
        )

        estimated_margin = (
            notional / leverage
        )

    if quantity <= 0:
        return 0.0, None, None

    # --------------------------------------------------------
    # RIESGO REAL APROXIMADO
    # --------------------------------------------------------

    estimated_risk = (
        quantity * stop_distance
    )

    log(
        f"{symbol} | "
        f"SIZE={quantity} | "
        f"NOTIONAL={notional:.4f} | "
        f"MARGIN≈{estimated_margin:.4f} | "
        f"RISK≈{estimated_risk:.4f} | "
        f"LEV=x{leverage:g}"
    )

    return (
        quantity,
        stop_distance,
        leverage
    )


# ============================================================
# COLOCAR ORDEN MARKET
# ============================================================

def place_market_order(
    symbol,
    side,
    quantity,
    reduce_only=False
):

    if not LIVE_TRADING:

        log(
            f"{symbol} | "
            f"PAPER ORDER | "
            f"{side} qty={quantity}"
        )

        return {
            "avgPrice": market_data[symbol]["price"],
            "executedQty": quantity,
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
        params["reduceOnly"] = "true"

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

    if avg_price is None or float(avg_price) <= 0:

        avg_price = market_data[symbol]["price"]

    return {
        "avgPrice": float(avg_price),
        "executedQty": float(executed_qty),
        "raw": result,
    }


# ============================================================
# ABRIR POSICIÓN
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

    quantity, stop_distance, leverage = (
        calculate_position_size(
            symbol,
            atr
        )
    )

    if quantity <= 0:
        log(
            f"{symbol} | "
            f"No se puede calcular tamaño."
        )
        return False

    price_before = (
        market_data[symbol]["price"]
    )

    log(
        f"{symbol} | "
        f"ABRIENDO {side} | "
        f"qty={quantity} | "
        f"precio≈{price_before}"
    )

    try:

        result = place_market_order(
            symbol,
            side,
            quantity,
            reduce_only=False
        )

        entry = float(
            result["avgPrice"]
        )

        executed_qty = float(
            result["executedQty"]
        )

        if executed_qty <= 0:
            raise RuntimeError(
                "La orden no tuvo cantidad ejecutada"
            )

        if side == "LONG":

            stop_price = (
                entry - stop_distance
            )

        else:

            stop_price = (
                entry + stop_distance
            )

        stop_price = normalize_price(
            symbol,
            stop_price
        )

        positions[symbol] = {
            "symbol": symbol,
            "side": side,
            "qty": executed_qty,
            "entry": entry,
            "atr": atr,
            "stop_distance": stop_distance,
            "stop_price": stop_price,
            "risk": (
                executed_qty
                * stop_distance
            ),
            "leverage": leverage,
            "opened_at": time.time(),
            "highest_price": entry,
            "lowest_price": entry,
            "peak_roi": 0.0,
            "setup": signal.get(
                "setup",
                "EMA_CROSS_RSI_VWAP"
            ),
        }

        log(
            f"{symbol} | "
            f"POSICIÓN ABIERTA | "
            f"{side} | "
            f"entry={entry:.8f} | "
            f"qty={executed_qty} | "
            f"stop_local={stop_price:.8f} | "
            f"ATR={atr:.8f} | "
            f"risk≈{executed_qty * stop_distance:.4f} USDT"
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

    side = position["side"]

    if side == "LONG":

        return (
            current_price - entry
        ) * qty

    else:

        return (
            entry - current_price
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

    side = position["side"]

    if entry <= 0:
        return 0.0

    if side == "LONG":

        raw_change = (
            current_price - entry
        ) / entry

    else:

        raw_change = (
            entry - current_price
        ) / entry

    return (
        raw_change
        * leverage
        * 100.0
    )


# ============================================================
# CERRAR POSICIÓN
# ============================================================

def close_position(
    symbol,
    reason
):

    position = positions.get(symbol)

    if position is None:
        return False

    current_price = (
        market_data[symbol]["price"]
    )

    if current_price is None:
        return False

    side = position["side"]

    qty = position["qty"]

    pnl_before = calculate_pnl(
        position,
        current_price
    )

    log(
        f"{symbol} | "
        f"CERRANDO {side} | "
        f"reason={reason} | "
        f"PnL≈{pnl_before:.4f} USDT"
    )

    try:

        result = place_market_order(
            symbol,
            side,
            qty,
            reduce_only=True
        )

        exit_price = float(
            result["avgPrice"]
        )

        executed_qty = float(
            result["executedQty"]
        )

        if executed_qty <= 0:
            executed_qty = qty

        final_pnl = calculate_pnl(
            position,
            exit_price
        )

        trade = {
            "symbol": symbol,
            "side": side,
            "qty": executed_qty,
            "entry": position["entry"],
            "exit": exit_price,
            "pnl": final_pnl,
            "reason": reason,
            "opened_at": position["opened_at"],
            "closed_at": time.time(),
        }

        trade_history.append(
            trade
        )

        log(
            f"{symbol} | "
            f"POSICIÓN CERRADA | "
            f"entry={position['entry']:.8f} | "
            f"exit={exit_price:.8f} | "
            f"PnL={final_pnl:.4f} USDT | "
            f"reason={reason}"
        )

        del positions[symbol]

        return True

    except Exception as e:

        log(
            f"{symbol} | "
            f"ERROR CERRANDO: {e}"
        )

        return False


# ============================================================
# GESTOR DE POSICIONES
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

                price = (
                    market_data[symbol]["price"]
                )

                if price is None:
                    continue

                side = position["side"]

                entry = position["entry"]

                # ------------------------------------------------
                # ACTUALIZAR MÁXIMO / MÍNIMO
                # ------------------------------------------------

                if side == "LONG":

                    position["highest_price"] = max(
                        position["highest_price"],
                        price
                    )

                else:

                    position["lowest_price"] = min(
                        position["lowest_price"],
                        price
                    )

                # ------------------------------------------------
                # PNL
                # ------------------------------------------------

                pnl = calculate_pnl(
                    position,
                    price
                )

                roi = calculate_roi(
                    position,
                    price
                )

                position["peak_roi"] = max(
                    position["peak_roi"],
                    roi
                )

                # ------------------------------------------------
                # STOP LOCAL ATR
                # ------------------------------------------------

                stop_price = position[
                    "stop_price"
                ]

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
                        "ATR_STOP_LOCAL"
                    )

                    continue

                # ------------------------------------------------
                # EMERGENCY LOSS
                #
                # 120% del riesgo objetivo.
                # US$0.25 -> aproximadamente US$0.30.
                # ------------------------------------------------

                emergency_loss = (
                    RISK_PER_TRADE_USDT
                    * 1.20
                )

                if pnl <= -emergency_loss:

                    close_position(
                        symbol,
                        "EMERGENCY_LOSS"
                    )

                    continue

                # ------------------------------------------------
                # ROI TRAILING
                # ------------------------------------------------

                if roi >= ROI_TRAILING_START:

                    protected_roi = (
                        position["peak_roi"]
                        - ROI_TRAILING_DISTANCE
                    )

                    if roi <= protected_roi:

                        close_position(
                            symbol,
                            "ROI_TRAILING"
                        )

                        continue

                # ------------------------------------------------
                # LOG DE MONITOREO
                # ------------------------------------------------

                log(
                    f"{symbol} | "
                    f"{side} | "
                    f"price={price:.8f} | "
                    f"PnL={pnl:.4f} | "
                    f"ROI={roi:.2f}% | "
                    f"peakROI={position['peak_roi']:.2f}% | "
                    f"stop={stop_price:.8f}"
                )

        except Exception as e:

            log(
                f"Position manager error: {e}"
            )

        time.sleep(1)


# ============================================================
# PROCESAR VELA CERRADA
# ============================================================

def process_closed_candle(
    symbol,
    candle
):

    timestamp = candle["timestamp"]

    last_timestamp = (
        market_data[symbol]["last_candle_time"]
    )

    if (
        last_timestamp is not None
        and timestamp <= last_timestamp
    ):
        return

    market_data[symbol]["candles"].append(
        candle
    )

    market_data[symbol]["candles"] = (
        market_data[symbol]["candles"][-MIN_CANDLES:]
    )

    market_data[symbol]["last_candle_time"] = (
        timestamp
    )

    market_data[symbol]["price"] = (
        candle["close"]
    )

    log(
        f"{symbol} | "
        f"VELA 5m CERRADA | "
        f"O={candle['open']:.8f} "
        f"H={candle['high']:.8f} "
        f"L={candle['low']:.8f} "
        f"C={candle['close']:.8f}"
    )

    # --------------------------------------------------------
    # NO ABRIR OTRA SI YA HAY POSICIÓN
    # --------------------------------------------------------

    if symbol in positions:
        return

    if len(positions) >= MAX_POSITIONS:
        return

    signal = calculate_signal(
        symbol
    )

    market_data[symbol]["last_signal"] = (
        signal
    )

    if signal is None:
        return

    open_position(
        symbol,
        signal
    )


# ============================================================
# MARKET WS
# ============================================================

def market_ws_loop(symbol):

    stream_name = (
        f"{symbol.lower()}@kline_{INTERVAL}"
    )

    url = (
        BINANCE_MARKET_WS
        + stream_name
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

                data = json.loads(raw)

                # ------------------------------------------------
                # STREAM DIRECTO
                # ------------------------------------------------

                if "k" in data:

                    kline = data["k"]

                # ------------------------------------------------
                # EVENTUAL WRAPPER
                # ------------------------------------------------

                elif "data" in data and isinstance(
                    data["data"],
                    dict
                ):

                    inner = data["data"]

                    if "k" not in inner:
                        continue

                    kline = inner["k"]

                else:

                    continue

                current_price = float(
                    kline["c"]
                )

                market_data[symbol]["price"] = (
                    current_price
                )

                # ------------------------------------------------
                # SOLO VELA CERRADA
                # ------------------------------------------------

                if not kline["x"]:
                    continue

                candle = {
                    "timestamp": int(
                        kline["t"]
                    ),
                    "open": float(
                        kline["o"]
                    ),
                    "high": float(
                        kline["h"]
                    ),
                    "low": float(
                        kline["l"]
                    ),
                    "close": float(
                        kline["c"]
                    ),
                    "volume": float(
                        kline["v"]
                    ),
                    "taker_buy_volume": float(
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

        log(
            f"{symbol} | "
            f"Reconectando Market WS "
            f"en {RECONNECT_SECONDS}s..."
        )

        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# USER DATA STREAM START
# ============================================================

def start_user_data_stream():

    global user_stream_control
    global listen_key

    if not API_KEY:
        log(
            "ERROR: BINANCE_API_KEY no configurada"
        )
        return False

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

        params = {
            "apiKey": API_KEY,
        }

        request_id = str(
            uuid.uuid4()
        )

        payload = {
            "id": request_id,
            "method": "userDataStream.start",
            "params": params,
        }

        ws.send(
            json.dumps(payload)
        )

        deadline = time.time() + 20

        response = None

        while time.time() < deadline:

            raw = ws.recv()

            if not raw:
                continue

            data = json.loads(raw)

            if data.get("id") == request_id:

                response = data

                break

        if response is None:

            raise RuntimeError(
                "Timeout UserDataStream.start"
            )

        if response.get("status") != 200:

            raise RuntimeError(
                f"UserDataStream.start rechazado: "
                f"{response}"
            )

        result = response.get(
            "result",
            {}
        )

        listen_key = result.get(
            "listenKey"
        )

        if not listen_key:

            raise RuntimeError(
                f"No se recibió listenKey: "
                f"{response}"
            )

        log(
            "User Data Stream iniciado correctamente."
        )

        log(
            f"ListenKey recibido: "
            f"{listen_key[:12]}..."
        )

        ws_api_ready.set()

        return True

    except Exception as e:

        log(
            f"UserDataStream.start error: {e}"
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
                + listen_key
            )

            log(
                "Abriendo User Data Stream..."
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

                data = json.loads(raw)

                event_type = data.get(
                    "e"
                )

                # ------------------------------------------------
                # ACCOUNT UPDATE
                # ------------------------------------------------

                if event_type == "ACCOUNT_UPDATE":

                    account = data.get(
                        "a",
                        {}
                    )

                    balances = account.get(
                        "B",
                        []
                    )

                    for balance in balances:

                        if balance.get(
                            "a"
                        ) == "USDT":

                            available = (
                                balance.get(
                                    "cw"
                                )
                            )

                            if available is not None:

                                log(
                                    f"ACCOUNT_UPDATE | "
                                    f"USDT available={available}"
                                )

                    positions_update = (
                        account.get(
                            "P",
                            []
                        )
                    )

                    for pos in positions_update:

                        symbol = pos.get(
                            "s"
                        )

                        if symbol not in SYMBOLS:
                            continue

                        position_amt = float(
                            pos.get(
                                "pa",
                                0
                            )
                        )

                        entry_price = float(
                            pos.get(
                                "ep",
                                0
                            )
                        )

                        if position_amt != 0:

                            log(
                                f"ACCOUNT_UPDATE | "
                                f"{symbol} | "
                                f"position={position_amt} | "
                                f"entry={entry_price}"
                            )

                # ------------------------------------------------
                # ORDER TRADE UPDATE
                # ------------------------------------------------

                elif event_type == "ORDER_TRADE_UPDATE":

                    order = data.get(
                        "o",
                        {}
                    )

                    symbol = order.get(
                        "s"
                    )

                    status = order.get(
                        "X"
                    )

                    side = order.get(
                        "S"
                    )

                    executed = order.get(
                        "z"
                    )

                    avg_price = order.get(
                        "ap"
                    )

                    if symbol in SYMBOLS:

                        log(
                            f"ORDER_UPDATE | "
                            f"{symbol} | "
                            f"side={side} | "
                            f"status={status} | "
                            f"executed={executed} | "
                            f"avg={avg_price}"
                        )

        except Exception as e:

            log(
                f"User Data Stream error: {e}"
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
# KEEPALIVE USER DATA STREAM
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
                    "listenKey": listen_key
                },
                timeout=15
            )

            log(
                "User Data Stream keepalive OK."
            )

        except Exception as e:

            log(
                f"User Data Stream keepalive error: {e}"
            )


# ============================================================
# STATUS
# ============================================================

def status_loop():

    while True:

        try:

            balance = get_usdt_balance()

            positions_count = len(
                positions
            )

            public_ip = get_public_ip()

            log(
                f"STATUS | "
                f"USDT_available={balance} | "
                f"positions={positions_count}/{MAX_POSITIONS} | "
                f"WS_READY={ws_api_ready.is_set()} | "
                f"IP={public_ip}"
            )

            for symbol in SYMBOLS:

                price = (
                    market_data[symbol]["price"]
                )

                leverage = (
                    symbol_leverage.get(
                        symbol
                    )
                )

                log(
                    f"STATUS | "
                    f"{symbol} | "
                    f"price={price} | "
                    f"leverage={leverage}"
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
        ("0.0.0.0", port),
        Handler
    )

    log(
        f"Health server escuchando "
        f"en puerto {port}"
    )

    server.serve_forever()


# ============================================================
# INICIALIZAR HISTORIAL
# ============================================================

def initialize_history():

    log(
        "=================================================="
    )

    log(
        "CARGANDO HISTORIAL KUCOIN FUTURES"
    )

    log(
        "=================================================="
    )

    success_count = 0

    for symbol in SYMBOLS:

        if load_kucoin_history(symbol):

            success_count += 1

        time.sleep(0.5)

    log(
        f"Historial listo: "
        f"{success_count}/{len(SYMBOLS)} símbolos."
    )

    return (
        success_count == len(SYMBOLS)
    )


# ============================================================
# MAIN
# ============================================================

def main():

    log(
        "=========================================================="
    )

    log(
        "BINANCE USD-M FUTURES BOT"
    )

    log(
        "ESTRATEGIA: EMA9/EMA21 + RSI14 + VWAP"
    )

    log(
        f"LIVE_TRADING={LIVE_TRADING}"
    )

    log(
        f"TESTNET={USE_TESTNET}"
    )

    log(
        f"INTERVAL={INTERVAL}"
    )

    log(
        f"MAX_POSITIONS={MAX_POSITIONS}"
    )

    log(
        f"RISK_PER_TRADE={RISK_PER_TRADE_USDT} USDT"
    )

    log(
        f"ATR_STOP_MULTIPLIER={ATR_STOP_MULTIPLIER}"
    )

    log(
        f"ROI_TRAILING_START={ROI_TRAILING_START}%"
    )

    log(
        f"ROI_TRAILING_DISTANCE={ROI_TRAILING_DISTANCE}%"
    )

    log(
        "LEVERAGE: CONFIGURADO MANUALMENTE EN BINANCE"
    )

    log(
        "EL BOT NO MODIFICA EL LEVERAGE"
    )

    log(
        "=========================================================="
    )

    # --------------------------------------------------------
    # IP
    # --------------------------------------------------------

    public_ip = get_public_ip()

    log(
        f"IP pública detectada: {public_ip}"
    )

    # --------------------------------------------------------
    # HEALTH SERVER
    # --------------------------------------------------------

    threading.Thread(
        target=health_server,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # USER DATA WS API
    # --------------------------------------------------------

    threading.Thread(
        target=start_user_data_stream,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # ESPERAR WS API
    # --------------------------------------------------------

    log(
        "Esperando Binance WS API..."
    )

    if not ws_api_ready.wait(
        timeout=30
    ):

        log(
            "ADVERTENCIA: "
            "WS API no quedó lista en 30 segundos."
        )

    else:

        log(
            "Binance WS API lista."
        )

    # --------------------------------------------------------
    # USER DATA STREAM
    # --------------------------------------------------------

    threading.Thread(
        target=user_data_socket_loop,
        daemon=True
    ).start()

    threading.Thread(
        target=user_stream_keepalive_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # HISTORIAL
    # --------------------------------------------------------

    if not initialize_history():

        log(
            "ERROR: "
            "No se pudo cargar historial completo."
        )

        return

    # --------------------------------------------------------
    # LEVERAGE
    #
    # SOLAMENTE LECTURA.
    # --------------------------------------------------------

    for symbol in SYMBOLS:

        try:

            get_symbol_leverage(
                symbol
            )

        except Exception as e:

            log(
                f"{symbol} | "
                f"No se pudo leer leverage: {e}"
            )

    # --------------------------------------------------------
    # MARKET WS
    # --------------------------------------------------------

    for symbol in SYMBOLS:

        threading.Thread(
            target=market_ws_loop,
            args=(symbol,),
            daemon=True
        ).start()

        time.sleep(0.5)

    # --------------------------------------------------------
    # POSITION MANAGER
    # --------------------------------------------------------

    threading.Thread(
        target=position_manager,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    threading.Thread(
        target=status_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # MAIN LOOP
    # --------------------------------------------------------

    log(
        "BOT OPERATIVO."
    )

    log(
        "Esperando cruces EMA9/EMA21 en velas 5m cerradas..."
    )

    while True:

        try:

            time.sleep(10)

        except KeyboardInterrupt:

            log(
                "Bot detenido manualmente."
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
