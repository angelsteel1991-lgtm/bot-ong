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

LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"

BASE_URL = "https://fapi.binance.com"

# 3 USDT de margen por operación
MARGIN_PER_TRADE_USDT = float(
    os.getenv("MARGIN_PER_TRADE_USDT", "3.0")
)

# Apalancamiento
MIN_LEVERAGE = 6
MAX_LEVERAGE = 7

# Gestión de riesgo
STOP_LOSS_PCT = 0.025
TAKE_PROFIT_PCT = 0.045
TRAILING_DROP_PCT = 0.020

# Tiempo mínimo entre operaciones
COOLDOWN_SECONDS = 180

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
# SERVIDOR HTTP PARA RENDER
# ============================================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"BOT ONGUSDT OK")

    def log_message(self, format, *args):
        return


def start_http_server():

    port = int(os.getenv("PORT", "10000"))

    server = HTTPServer(
        ("0.0.0.0", port),
        HealthHandler
    )

    log(f"Servidor HTTP iniciado en puerto {port}")

    server.serve_forever()


# ============================================================
# UTILIDADES
# ============================================================

def log(message):

    now = datetime.now(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    print(
        f"[{now}] {message}",
        flush=True
    )


def floor_step(value, step):

    if step <= 0:
        return value

    return math.floor(value / step) * step


def format_number(value, decimals=8):

    return f"{value:.{decimals}f}".rstrip("0").rstrip(".")


# ============================================================
# FIRMA BINANCE
# ============================================================

def signed_request(method, endpoint, params=None):

    if not API_KEY or not API_SECRET:
        raise Exception(
            "Faltan BINANCE_API_KEY o BINANCE_API_SECRET"
        )

    if params is None:
        params = {}

    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000

    query_string = urlencode(params)

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
        raise Exception("Método HTTP no soportado")

    if response.status_code != 200:

        raise Exception(
            f"Binance HTTP {response.status_code}: "
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

    url = BASE_URL + "/fapi/v1/exchangeInfo"

    response = requests.get(
        url,
        params={"symbol": SYMBOL},
        timeout=10
    )

    if response.status_code != 200:

        raise Exception(
            f"Error exchangeInfo: {response.text}"
        )

    data = response.json()

    symbol_data = None

    for item in data["symbols"]:

        if item["symbol"] == SYMBOL:
            symbol_data = item
            break

    if symbol_data is None:

        raise Exception(
            f"{SYMBOL} no está disponible en Binance Futures"
        )

    exchange_info = symbol_data

    for f in symbol_data["filters"]:

        if f["filterType"] == "LOT_SIZE":

            qty_step = float(f["stepSize"])
            min_qty = float(f["minQty"])

        elif f["filterType"] == "PRICE_FILTER":

            price_tick = float(f["tickSize"])

    log(f"Contrato encontrado: {SYMBOL}")
    log(f"Cantidad mínima: {min_qty}")
    log(f"Paso de cantidad: {qty_step}")
    log(f"Tick de precio: {price_tick}")


# ============================================================
# PRECIO
# ============================================================

def get_price():

    url = BASE_URL + "/fapi/v1/ticker/price"

    response = requests.get(
        url,
        params={"symbol": SYMBOL},
        timeout=10
    )

    if response.status_code != 200:

        raise Exception(
            f"Error obteniendo precio: {response.text}"
        )

    return float(
        response.json()["price"]
    )


# ============================================================
# BALANCE FUTURES
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
# POSICIÓN ACTUAL
# ============================================================

def get_current_position():

    data = signed_request(
        "GET",
        "/fapi/v3/positionRisk",
        {"symbol": SYMBOL}
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
                float(p["entryPrice"])
            )

        if amount < 0:

            return (
                "SHORT",
                abs(amount),
                float(p["entryPrice"])
            )

    return None, 0.0, 0.0


# ============================================================
# APALANCAMIENTO
# ============================================================

def set_leverage(leverage):

    leverage = max(
        MIN_LEVERAGE,
        min(MAX_LEVERAGE, leverage)
    )

    result = signed_request(
        "POST",
        "/fapi/v1/leverage",
        {
            "symbol": SYMBOL,
            "leverage": leverage
        }
    )

    log(
        f"Apalancamiento configurado: "
        f"{leverage}x"
    )

    return leverage


# ============================================================
# INDICADORES
# ============================================================

def calculate_indicators(df):

    df["ema9"] = df["close"].ewm(
        span=9,
        adjust=False
    ).mean()

    df["ema21"] = df["close"].ewm(
        span=21,
        adjust=False
    ).mean()

    delta = df["close"].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()

    rs = avg_gain / avg_loss.replace(
        0,
        1e-10
    )

    df["rsi"] = 100 - (
        100 / (1 + rs)
    )

    df["volume_ma"] = df["volume"].rolling(
        20
    ).mean()

    high_low = df["high"] - df["low"]

    high_close = abs(
        df["high"] - df["close"].shift()
    )

    low_close = abs(
        df["low"] - df["close"].shift()
    )

    tr = pd.concat(
        [
            high_low,
            high_close,
            low_close
        ],
        axis=1
    ).max(axis=1)

    df["atr"] = tr.rolling(
        14
    ).mean()

    return df


# ============================================================
# SEÑAL - MODO ACTIVO 2 PUNTOS
# ============================================================

def analyze_market():

    if len(candles) < 50:
        return "NEUTRAL", 0

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

        df[col] = df[col].astype(float)

    df = calculate_indicators(df)

    last = df.iloc[-1]

    score_long = 0
    score_short = 0

    # --------------------------------------------------------
    # TENDENCIA
    # --------------------------------------------------------

    if last["ema9"] > last["ema21"]:
        score_long += 2

    if last["ema9"] < last["ema21"]:
        score_short += 2

    # --------------------------------------------------------
    # RSI
    # --------------------------------------------------------

    if 52 <= last["rsi"] <= 68:
        score_long += 2

    if 32 <= last["rsi"] <= 48:
        score_short += 2

    # --------------------------------------------------------
    # VOLUMEN
    # --------------------------------------------------------

    if last["volume"] > last["volume_ma"]:

        if last["close"] > last["open"]:
            score_long += 1

        elif last["close"] < last["open"]:
            score_short += 1

    # --------------------------------------------------------
    # PRECIO VS EMA9
    # --------------------------------------------------------

    if last["close"] > last["ema9"]:
        score_long += 1

    if last["close"] < last["ema9"]:
        score_short += 1

    log(
        f"Precio={last['close']:.8f} "
        f"EMA9={last['ema9']:.8f} "
        f"EMA21={last['ema21']:.8f} "
        f"RSI={last['rsi']:.2f} "
        f"L={score_long} "
        f"S={score_short}"
    )

    # --------------------------------------------------------
    # ENTRADA ACTIVA: 2 PUNTOS
    # --------------------------------------------------------

    if (
        score_long >= 2
        and score_long >= score_short + 1
    ):

        return "LONG", score_long

    if (
        score_short >= 2
        and score_short >= score_long + 1
    ):

        return "SHORT", score_short

    return "NEUTRAL", max(
        score_long,
        score_short
    )


# ============================================================
# CANTIDAD
# ============================================================

def calculate_quantity(price, leverage):

    balance = get_usdt_balance()

    if balance <= 0:
        raise Exception(
            "No hay balance USDT disponible"
        )

    # Máximo 3 USDT de margen
    margin = min(
        MARGIN_PER_TRADE_USDT,
        balance * 0.25
    )

    notional = margin * leverage

    quantity = notional / price

    quantity = floor_step(
        quantity,
        qty_step
    )

    if quantity < min_qty:

        raise Exception(
            f"Cantidad calculada {quantity} "
            f"menor al mínimo {min_qty}"
        )

    log(
        f"Balance={balance:.4f} USDT | "
        f"Margen={margin:.4f} USDT | "
        f"Nocional={notional:.4f} USDT | "
        f"Cantidad={quantity}"
    )

    return quantity


# ============================================================
# ABRIR POSICIÓN
# ============================================================

def open_position(side):

    global last_trade_time
    global position_side
    global entry_price
    global highest_price
    global lowest_price

    now = time.time()

    if (
        now - last_trade_time
        < COOLDOWN_SECONDS
    ):
        return

    current_position, amount, current_entry = (
        get_current_position()
    )

    if current_position is not None:

        log(
            f"Ya existe posición "
            f"{current_position}. "
            f"No se abre otra."
        )

        return

    price = get_price()

    leverage = 6

    if side == "LONG":
        order_side = "BUY"
    else:
        order_side = "SELL"

    leverage = set_leverage(
        leverage
    )

    quantity = calculate_quantity(
        price,
        leverage
    )

    log(
        f"SEÑAL {side} | "
        f"Precio {price} | "
        f"Leverage {leverage}x | "
        f"Cantidad {quantity}"
    )

    if not LIVE_TRADING:

        log(
            "MODO PRUEBA: "
            "NO SE ENVÍA LA ORDEN."
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

def close_position(reason):

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
        f"CERRANDO {current_position} | "
        f"Razón: {reason}"
    )

    if not LIVE_TRADING:

        log(
            "MODO PRUEBA: "
            "NO SE ENVÍA CIERRE."
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

def manage_position(price):

    global highest_price
    global lowest_price

    current_position, amount, current_entry = (
        get_current_position()
    )

    if current_position is None:
        return

    if current_position == "LONG":

        if highest_price == 0:
            highest_price = current_entry

        if price > highest_price:
            highest_price = price

        stop_price = current_entry * (
            1 - STOP_LOSS_PCT
        )

        trailing_price = highest_price * (
            1 - TRAILING_DROP_PCT
        )

        take_profit = current_entry * (
            1 + TAKE_PROFIT_PCT
        )

        if price <= stop_price:

            close_position(
                "STOP LOSS"
            )

            return

        if (
            price <= trailing_price
            and price > current_entry
        ):

            close_position(
                "TRAILING STOP"
            )

            return

        if price >= take_profit:

            close_position(
                "TAKE PROFIT"
            )

            return

    elif current_position == "SHORT":

        if lowest_price == 0:
            lowest_price = current_entry

        if price < lowest_price:
            lowest_price = price

        stop_price = current_entry * (
            1 + STOP_LOSS_PCT
        )

        trailing_price = lowest_price * (
            1 + TRAILING_DROP_PCT
        )

        take_profit = current_entry * (
            1 - TAKE_PROFIT_PCT
        )

        if price >= stop_price:

            close_position(
                "STOP LOSS"
            )

            return

        if (
            price >= trailing_price
            and price < current_entry
        ):

            close_position(
                "TRAILING STOP"
            )

            return

        if price <= take_profit:

            close_position(
                "TAKE PROFIT"
            )

            return


# ============================================================
# VELAS
# ============================================================

def load_initial_candles():

    url = BASE_URL + "/fapi/v1/klines"

    response = requests.get(
        url,
        params={
            "symbol": SYMBOL,
            "interval": "1m",
            "limit": 100
        },
        timeout=10
    )

    if response.status_code != 200:

        raise Exception(
            f"Error descargando velas: "
            f"{response.text}"
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

def on_message(ws, message):

    try:

        data = json.loads(message)

        if data.get("e") != "kline":
            return

        k = data["k"]

        price = float(k["c"])

        # Gestionar posición en tiempo real
        manage_position(price)

        # Analizar solamente al cierre de vela
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
                and candles[-1][0] == k["t"]
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
                f"SEÑAL ACTUAL: "
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
            f"ERROR procesando mensaje: "
            f"{e}"
        )


def on_error(ws, error):

    log(
        f"WebSocket ERROR: {error}"
    )


def on_close(
    ws,
    close_status_code,
    close_msg
):

    log(
        f"WebSocket cerrado: "
        f"{close_status_code} "
        f"{close_msg}"
    )


def on_open(ws):

    log(
        "WebSocket conectado correctamente."
    )


# ============================================================
# MAIN
# ============================================================

def main():

    log(
        "===================================="
    )

    log(
        "BOT ONGUSDT FUTURES - 2 PUNTOS"
    )

    log(
        "===================================="
    )

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan las variables "
            "BINANCE_API_KEY y "
            "BINANCE_API_SECRET en Render."
        )

    log(
        f"LIVE_TRADING = "
        f"{LIVE_TRADING}"
    )

    log(
        "Modo de señal: ACTIVO - 2 PUNTOS"
    )

    load_exchange_info()

    load_initial_candles()

    balance = get_usdt_balance()

    log(
        f"Balance Futures disponible: "
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

    log(
        "Conectando al stream de velas..."
    )

    websocket_url = (
        "wss://fstream.binance.com/ws/"
        f"{SYMBOL.lower()}@kline_1m"
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
                f"WebSocket exception: {e}"
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
