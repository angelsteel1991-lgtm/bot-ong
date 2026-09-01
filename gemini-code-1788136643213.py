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

from urllib.parse import urlencode
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler


# ============================================================
# CONFIGURACIÓN
# ============================================================

SYMBOL = "ONGUSDT"

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

LIVE_TRADING = (
    os.getenv("LIVE_TRADING", "false").lower() == "true"
)

BASE_URL = "https://fapi.binance.com"

y
# ------------------------------------------------------------
# CAPITAL
# ------------------------------------------------------------

# Margen REAL utilizado por operación.
# Con 6x -> aproximadamente 12 USDT de posición.
MARGIN_PER_TRADE_USDT = float(
    os.getenv("MARGIN_PER_TRADE_USDT", "2.0")
)

LEVERAGE = 6


# ------------------------------------------------------------
# GESTIÓN DE SALIDA
# ------------------------------------------------------------

STOP_LOSS_PCT = 0.009        # 0.9%
TAKE_PROFIT_PCT = 0.012      # 1.2%
TRAILING_DROP_PCT = 0.008    # 0.8%

COOLDOWN_SECONDS = 60


# ------------------------------------------------------------
# ACTIVIDAD
# ------------------------------------------------------------

MIN_SCORE = 2
MIN_SCORE_DIFFERENCE = 1


# ============================================================
# VARIABLES
# ============================================================

exchange_info = None

qty_step = None
min_qty = None
price_tick = None

last_trade_time = 0

position_side = None
entry_price = 0.0

highest_price = 0.0
lowest_price = 0.0

candles = []


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
# SERVIDOR RENDER
# ============================================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        self.send_response(200)

        self.send_header(
            "Content-Type",
            "text/plain"
        )

        self.end_headers()

        self.wfile.write(
            b"BOT ONGUSDT FUTURES OK"
        )

    def log_message(
        self,
        format,
        *args
    ):
        return


def start_http_server():

    port = int(
        os.getenv(
            "PORT",
            "10000"
        )
    )

    server = HTTPServer(
        ("0.0.0.0", port),
        HealthHandler
    )

    log(
        f"Servidor HTTP iniciado en puerto {port}"
    )

    server.serve_forever()


# ============================================================
# UTILIDADES
# ============================================================

def floor_step(
    value,
    step
):

    if step <= 0:
        return value

    return math.floor(
        value / step
    ) * step


def format_number(
    value,
    decimals=8
):

    return (
        f"{value:.{decimals}f}"
        .rstrip("0")
        .rstrip(".")
    )


# ============================================================
# BINANCE API
# ============================================================

def signed_request(
    method,
    endpoint,
    params=None
):

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan BINANCE_API_KEY "
            "o BINANCE_API_SECRET"
        )

    if params is None:
        params = {}

    params["timestamp"] = int(
        time.time() * 1000
    )

    params["recvWindow"] = 5000

    query_string = urlencode(
        params
    )

    signature = hmac.new(
        API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    params["signature"] = signature

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    url = BASE_URL + endpoint

    if method == "GET":

        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=10
        )

    elif method == "POST":

        response = requests.post(
            url,
            params=params,
            headers=headers,
            timeout=10
        )

    elif method == "DELETE":

        response = requests.delete(
            url,
            params=params,
            headers=headers,
            timeout=10
        )

    else:

        raise Exception(
            "Método HTTP no soportado"
        )

    if response.status_code != 200:

        raise Exception(
            f"Binance HTTP "
            f"{response.status_code}: "
            f"{response.text}"
        )

    return response.json()


# ============================================================
# INFORMACIÓN DEL CONTRATO
# ============================================================

def load_exchange_info():

    global exchange_info
    global qty_step
    global min_qty
    global price_tick

    response = requests.get(
        BASE_URL + "/fapi/v1/exchangeInfo",
        params={
            "symbol": SYMBOL
        },
        timeout=10
    )

    if response.status_code != 200:

        raise Exception(
            "Error exchangeInfo: "
            + response.text
        )

    data = response.json()

    symbol_data = None

    for item in data["symbols"]:

        if item["symbol"] == SYMBOL:

            symbol_data = item
            break

    if symbol_data is None:

        raise Exception(
            f"{SYMBOL} no está disponible "
            f"en Binance Futures"
        )

    exchange_info = symbol_data

    for f in symbol_data["filters"]:

        if f["filterType"] == "LOT_SIZE":

            qty_step = float(
                f["stepSize"]
            )

            min_qty = float(
                f["minQty"]
            )

        elif f["filterType"] == "PRICE_FILTER":

            price_tick = float(
                f["tickSize"]
            )

    log(
        f"Contrato: {SYMBOL}"
    )

    log(
        f"Cantidad mínima: {min_qty}"
    )

    log(
        f"Paso cantidad: {qty_step}"
    )

    log(
        f"Tick precio: {price_tick}"
    )


# ============================================================
# PRECIO
# ============================================================

def get_price():

    response = requests.get(
        BASE_URL + "/fapi/v1/ticker/price",
        params={
            "symbol": SYMBOL
        },
        timeout=10
    )

    if response.status_code != 200:

        raise Exception(
            "Error precio: "
            + response.text
        )

    return float(
        response.json()["price"]
    )


# ============================================================
# BALANCE
# ============================================================

def get_usdt_balance():

    data = signed_request(
        "GET",
        "/fapi/v2/balance"
    )

    for asset in data:

        if asset["asset"] == "USDT":

            return float(
                asset["availableBalance"]
            )

    return 0.0


# ============================================================
# POSICIÓN
# ============================================================

def get_current_position():

    data = signed_request(
        "GET",
        "/fapi/v3/positionRisk",
        {
            "symbol": SYMBOL
        }
    )

    for p in data:

        if p["symbol"] != SYMBOL:
            continue

        amount = float(
            p["positionAmt"]
        )

        if amount > 0:

            return (
                "LONG",
                amount,
                float(
                    p["entryPrice"]
                )
            )

        if amount < 0:

            return (
                "SHORT",
                abs(amount),
                float(
                    p["entryPrice"]
                )
            )

    return (
        None,
        0.0,
        0.0
    )


# ============================================================
# APALANCAMIENTO
# ============================================================

def set_leverage():

    result = signed_request(
        "POST",
        "/fapi/v1/leverage",
        {
            "symbol": SYMBOL,
            "leverage": LEVERAGE
        }
    )

    log(
        f"Apalancamiento configurado: "
        f"{LEVERAGE}x"
    )

    return LEVERAGE


# ============================================================
# INDICADORES
# ============================================================

def calculate_indicators(df):

    # EMA
    df["ema9"] = df[
        "close"
    ].ewm(
        span=9,
        adjust=False
    ).mean()

    df["ema21"] = df[
        "close"
    ].ewm(
        span=21,
        adjust=False
    ).mean()

    df["ema50"] = df[
        "close"
    ].ewm(
        span=50,
        adjust=False
    ).mean()


    # RSI
    delta = df[
        "close"
    ].diff()

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
        avg_gain /
        avg_loss.replace(
            0,
            1e-10
        )
    )

    df["rsi"] = (
        100 -
        (
            100 /
            (1 + rs)
        )
    )


    # MACD
    ema12 = df[
        "close"
    ].ewm(
        span=12,
        adjust=False
    ).mean()

    ema26 = df[
        "close"
    ].ewm(
        span=26,
        adjust=False
    ).mean()

    df["macd"] = (
        ema12 - ema26
    )

    df["macd_signal"] = df[
        "macd"
    ].ewm(
        span=9,
        adjust=False
    ).mean()

    df["macd_hist"] = (
        df["macd"] -
        df["macd_signal"]
    )


    # Volumen
    df["volume_ma"] = df[
        "volume"
    ].rolling(
        20
    ).mean()


    # ATR
    high_low = (
        df["high"] -
        df["low"]
    )

    high_close = abs(
        df["high"] -
        df["close"].shift()
    )

    low_close = abs(
        df["low"] -
        df["close"].shift()
    )

    tr = pd.concat(
        [
            high_low,
            high_close,
            low_close
        ],
        axis=1
    ).max(
        axis=1
    )

    df["atr"] = tr.rolling(
        14
    ).mean()


    return df


# ============================================================
# ANÁLISIS PRINCIPAL
# ============================================================

def analyze_market():

    if len(candles) < 60:

        return (
            "NEUTRAL",
            0
        )

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

    numeric_columns = [
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

    for col in numeric_columns:

        df[col] = df[
            col
        ].astype(float)

    df = calculate_indicators(
        df
    )

    last = df.iloc[-1]

    score_long = 0
    score_short = 0


    # ========================================================
    # 1. EMA 9 / 21
    # ========================================================

    if last["ema9"] > last["ema21"]:

        score_long += 1

    elif last["ema9"] < last["ema21"]:

        score_short += 1


    # ========================================================
    # 2. EMA 21 / 50
    # ========================================================

    if last["ema21"] > last["ema50"]:

        score_long += 1

    elif last["ema21"] < last["ema50"]:

        score_short += 1


    # ========================================================
    # 3. PRECIO VS EMA9
    # ========================================================

    if last["close"] > last["ema9"]:

        score_long += 1

    elif last["close"] < last["ema9"]:

        score_short += 1


    # ========================================================
    # 4. RSI
    # ========================================================

    rsi = last["rsi"]

    if 50 <= rsi <= 70:

        score_long += 1

    elif 30 <= rsi < 50:

        score_short += 1


    # ========================================================
    # 5. MACD
    # ========================================================

    if last["macd_hist"] > 0:

        score_long += 1

    elif last["macd_hist"] < 0:

        score_short += 1


    # ========================================================
    # 6. VOLUMEN + DIRECCIÓN
    # ========================================================

    if (
        last["volume"] >
        last["volume_ma"]
    ):

        if last["close"] > last["open"]:

            score_long += 1

        elif last["close"] < last["open"]:

            score_short += 1


    # ========================================================
    # 7. MOVIMIENTO
    # ========================================================

    atr = last["atr"]

    if (
        pd.notna(atr)
        and atr > 0
    ):

        movement = (
            abs(
                last["close"] -
                last["open"]
            )
            / last["close"]
        )

        if movement > 0.001:

            if last["close"] > last["open"]:

                score_long += 1

            else:

                score_short += 1


    # ========================================================
    # LOG
    # ========================================================

    log(
        f"PRECIO={last['close']:.5f} | "
        f"RSI={last['rsi']:.1f} | "
        f"EMA9={last['ema9']:.5f} | "
        f"EMA21={last['ema21']:.5f} | "
        f"MACD={last['macd_hist']:.6f} | "
        f"L={score_long} | "
        f"S={score_short}"
    )


    # ========================================================
    # SEÑAL MÁS ACTIVA
    # ========================================================

    if (
        score_long >= MIN_SCORE
        and
        score_long >= (
            score_short +
            MIN_SCORE_DIFFERENCE
        )
    ):

        return (
            "LONG",
            score_long
        )


    if (
        score_short >= MIN_SCORE
        and
        score_short >= (
            score_long +
            MIN_SCORE_DIFFERENCE
        )
    ):

        return (
            "SHORT",
            score_short
        )


    return (
        "NEUTRAL",
        max(
            score_long,
            score_short
        )
    )


# ============================================================
# CANTIDAD
# ============================================================

def calculate_quantity(
    price
):

    balance = get_usdt_balance()

    if balance <= 0:

        raise Exception(
            "No hay balance USDT"
        )


    # Nunca usar más del 30%
    # del balance disponible como margen.
    margin = min(
        MARGIN_PER_TRADE_USDT,
        balance * 0.30
    )


    notional = (
        margin *
        LEVERAGE
    )

    quantity = (
        notional /
        price
    )


    quantity = floor_step(
        quantity,
        qty_step
    )


    if quantity < min_qty:

        raise Exception(
            f"Cantidad {quantity} "
            f"menor al mínimo "
            f"{min_qty}"
        )


    log(
        f"BALANCE={balance:.4f} USDT | "
        f"MARGEN={margin:.4f} | "
        f"NOCIONAL={notional:.4f} | "
        f"QTY={quantity}"
    )

    return quantity


# ============================================================
# ABRIR POSICIÓN
# ============================================================

def open_position(
    side
):

    global last_trade_time
    global position_side
    global entry_price
    global highest_price
    global lowest_price


    now = time.time()


    # Cooldown
    if (
        now -
        last_trade_time
        <
        COOLDOWN_SECONDS
    ):

        return


    current_position, amount, current_entry = (
        get_current_position()
    )


    if current_position is not None:

        log(
            f"Ya existe "
            f"{current_position}. "
            f"No se abre otra."
        )

        return


    price = get_price()


    set_leverage()


    quantity = calculate_quantity(
        price
    )


    if side == "LONG":

        order_side = "BUY"

    else:

        order_side = "SELL"


    log(
        "================================================"
    )

    log(
        f"ENTRADA {side}"
    )

    log(
        f"Precio: {price}"
    )

    log(
        f"Cantidad: {quantity}"
    )

    log(
        f"Leverage: {LEVERAGE}x"
    )

    log(
        "================================================"
    )


    if not LIVE_TRADING:

        log(
            "MODO PRUEBA: "
            "NO SE ENVÍA ORDEN."
        )

        return


    result = signed_request(
        "POST",
        "/fapi/v1/order",
        {
            "symbol": SYMBOL,
            "side": order_side,
            "type": "MARKET",
            "quantity": format_number(
                quantity
            ),
            "newOrderRespType": "RESULT"
        }
    )


    log(
        f"ORDEN EJECUTADA: {result}"
    )


    position_side = side


    entry_price = float(
        result.get(
            "avgPrice",
            price
        )
    )


    highest_price = entry_price
    lowest_price = entry_price


    last_trade_time = now


# ============================================================
# CERRAR POSICIÓN
# ============================================================

def close_position(
    reason
):

    global position_side
    global entry_price
    global highest_price
    global lowest_price


    current_position, amount, current_entry = (
        get_current_position()
    )


    if current_position is None:

        position_side = None

        return


    if current_position == "LONG":

        order_side = "SELL"

    else:

        order_side = "BUY"


    log(
        f"CERRANDO {current_position}"
    )

    log(
        f"RAZÓN: {reason}"
    )


    if not LIVE_TRADING:

        log(
            "MODO PRUEBA: "
            "NO SE CIERRA."
        )

        return


    result = signed_request(
        "POST",
        "/fapi/v1/order",
        {
            "symbol": SYMBOL,
            "side": order_side,
            "type": "MARKET",
            "quantity": format_number(
                amount
            ),
            "reduceOnly": "true",
            "newOrderRespType": "RESULT"
        }
    )


    log(
        f"CIERRE EJECUTADO: {result}"
    )


    position_side = None

    entry_price = 0.0

    highest_price = 0.0

    lowest_price = 0.0


# ============================================================
# GESTIÓN DE POSICIÓN
# ============================================================

def manage_position(
    price
):

    global highest_price
    global lowest_price


    current_position, amount, current_entry = (
        get_current_position()
    )


    if current_position is None:

        return


    # --------------------------------------------------------
    # LONG
    # --------------------------------------------------------

    if current_position == "LONG":


        if highest_price == 0:

            highest_price = current_entry


        if price > highest_price:

            highest_price = price


        stop_price = (
            current_entry *
            (
                1 -
                STOP_LOSS_PCT
            )
        )


        take_profit = (
            current_entry *
            (
                1 +
                TAKE_PROFIT_PCT
            )
        )


        trailing_price = (
            highest_price *
            (
                1 -
                TRAILING_DROP_PCT
            )
        )


        if price <= stop_price:

            close_position(
                "STOP LOSS"
            )

            return


        if price >= take_profit:

            close_position(
                "TAKE PROFIT"
            )

            return


        if (
            price <= trailing_price
            and
            price > current_entry
        ):

            close_position(
                "TRAILING STOP"
            )

            return


    # --------------------------------------------------------
    # SHORT
    # --------------------------------------------------------

    elif current_position == "SHORT":


        if lowest_price == 0:

            lowest_price = current_entry


        if price < lowest_price:

            lowest_price = price


        stop_price = (
            current_entry *
            (
                1 +
                STOP_LOSS_PCT
            )
        )


        take_profit = (
            current_entry *
            (
                1 -
                TAKE_PROFIT_PCT
            )
        )


        trailing_price = (
            lowest_price *
            (
                1 +
                TRAILING_DROP_PCT
            )
        )


        if price >= stop_price:

            close_position(
                "STOP LOSS"
            )

            return


        if price <= take_profit:

            close_position(
                "TAKE PROFIT"
            )

            return


        if (
            price >= trailing_price
            and
            price < current_entry
        ):

            close_position(
                "TRAILING STOP"
            )

            return


# ============================================================
# CARGAR VELAS
# ============================================================

def load_initial_candles():

    response = requests.get(
        BASE_URL + "/fapi/v1/klines",
        params={
            "symbol": SYMBOL,
            "interval": "1m",
            "limit": 200
        },
        timeout=10
    )


    if response.status_code != 200:

        raise Exception(
            "Error descargando velas: "
            + response.text
        )


    data = response.json()


    candles.clear()


    for k in data:

        candles.append(
            [
                k[0],
                float(k[1]),
                float(k[2]),
                float(k[3]),
                float(k[4]),
                float(k[5])
            ]
        )


    log(
        f"Velas cargadas: "
        f"{len(candles)}"
    )


# ============================================================
# WEBSOCKET
# ============================================================

def on_message(
    ws,
    message
):

    try:

        data = json.loads(
            message
        )


        if data.get("e") != "kline":

            return


        k = data["k"]


        price = float(
            k["c"]
        )


        # Gestionar posición
        # continuamente.
        manage_position(
            price
        )


        # Solo analizar nueva vela
        # cuando termina.
        if k["x"]:


            candle = [
                k["t"],
                float(k["o"]),
                float(k["h"]),
                float(k["l"]),
                float(k["c"]),
                float(k["v"])
            ]


            if (
                candles
                and
                candles[-1][0] ==
                k["t"]
            ):

                candles[-1] = candle

            else:

                candles.append(
                    candle
                )


            if len(candles) > 200:

                candles.pop(0)


            signal, score = (
                analyze_market()
            )


            log(
                f"SEÑAL: "
                f"{signal} "
                f"(score {score})"
            )


            if signal == "LONG":

                open_position(
                    "LONG"
                )


            elif signal == "SHORT":

                open_position(
                    "SHORT"
                )


    except Exception as e:

        log(
            "ERROR mensaje: "
            f"{e}"
        )


# ============================================================
# WEBSOCKET EVENTS
# ============================================================

def on_error(
    ws,
    error
):

    log(
        f"WebSocket ERROR: "
        f"{error}"
    )


def on_close(
    ws,
    close_status_code,
    close_msg
):

    log(
        "WebSocket cerrado: "
        f"{close_status_code} "
        f"{close_msg}"
    )


def on_open(
    ws
):

    log(
        "WebSocket conectado."
    )


# ============================================================
# MAIN
# ============================================================

def main():

    log(
        "=========================================="
    )

    log(
        "BOT ONGUSDT FUTURES - ACTIVE V2"
    )

    log(
        "=========================================="
    )


    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan "
            "BINANCE_API_KEY "
            "o "
            "BINANCE_API_SECRET"
        )


    log(
        f"LIVE_TRADING = "
        f"{LIVE_TRADING}"
    )

    log(
        f"MARGEN = "
        f"{MARGIN_PER_TRADE_USDT} USDT"
    )

    log(
        f"LEVERAGE = "
        f"{LEVERAGE}x"
    )

    log(
        f"MIN_SCORE = "
        f"{MIN_SCORE}"
    )

    log(
        f"COOLDOWN = "
        f"{COOLDOWN_SECONDS}s"
    )


    load_exchange_info()

    load_initial_candles()


    balance = get_usdt_balance()


    log(
        f"Balance Futures: "
        f"{balance:.4f} USDT"
    )


    current_position, amount, entry = (
        get_current_position()
    )


    if current_position:

        log(
            f"POSICIÓN EXISTENTE: "
            f"{current_position} "
            f"cantidad={amount} "
            f"entrada={entry}"
        )


    websocket_url = (
        "wss://fstream.binance.com/ws/"
        f"{SYMBOL.lower()}"
        "@kline_1m"
    )


    log(
        "Conectando al stream..."
    )


    while True:

        try:

            ws = websocket.WebSocketApp(
                websocket_url,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )


            ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )


        except Exception as e:

            log(
                f"WebSocket exception: "
                f"{e}"
            )


        log(
            "Reconectando en 10 segundos..."
        )


        time.sleep(10)


# ============================================================
# ARRANQUE
# ============================================================

if __name__ == "__main__":


    http_thread = threading.Thread(
        target=start_http_server,
        daemon=True
    )


    http_thread.start()


    main()
