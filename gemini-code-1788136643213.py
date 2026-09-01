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

MARGIN_PER_TRADE_USDT = float(
    os.getenv("MARGIN_PER_TRADE_USDT", "2.5")
)

LEVERAGE = 6

STOP_LOSS_PCT = 0.020
TAKE_PROFIT_PCT = 0.030
TRAILING_DROP_PCT = 0.012

COOLDOWN_SECONDS = 60

POSITION_CHECK_SECONDS = 30

# Datos aproximados.
# Se evita exchangeInfo al arrancar para no generar
# otra petición que pueda provocar un bloqueo -1003.
QTY_STEP = 1.0
MIN_QTY = 1.0


# ============================================================
# VARIABLES
# ============================================================

last_trade_time = 0

position_side = None
entry_price = 0.0

highest_price = 0.0
lowest_price = 0.0

candles = []

last_position_check = 0
cached_position = (None, 0.0, 0.0)

rest_lock = threading.Lock()


# ============================================================
# SERVIDOR HTTP PARA RENDER
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
            b"BOT ONGUSDT OK"
        )

    def log_message(self, format, *args):
        return


def start_http_server():

    port = int(
        os.getenv("PORT", "10000")
    )

    server = HTTPServer(
        ("0.0.0.0", port),
        HealthHandler
    )

    log(
        f"Servidor HTTP iniciado "
        f"en puerto {port}"
    )

    server.serve_forever()


# ============================================================
# UTILIDADES
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


def floor_step(value, step):

    if step <= 0:
        return value

    return math.floor(
        value / step
    ) * step


def format_number(value, decimals=8):

    return (
        f"{value:.{decimals}f}"
        .rstrip("0")
        .rstrip(".")
    )


# ============================================================
# REQUEST BINANCE FIRMADO
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

    params = dict(params)

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

    with rest_lock:

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
# PRECIO
# ============================================================

def get_price():

    url = (
        BASE_URL +
        "/fapi/v1/ticker/price"
    )

    with rest_lock:

        response = requests.get(
            url,
            params={
                "symbol": SYMBOL
            },
            timeout=10
        )

    if response.status_code != 200:

        raise Exception(
            "Error obteniendo precio: "
            + response.text
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

def get_current_position(force=False):

    global cached_position
    global last_position_check

    now = time.time()

    if (
        not force
        and
        now - last_position_check
        < POSITION_CHECK_SECONDS
    ):

        return cached_position

    data = signed_request(
        "GET",
        "/fapi/v3/positionRisk",
        {
            "symbol": SYMBOL
        }
    )

    result = (
        None,
        0.0,
        0.0
    )

    for p in data:

        if p["symbol"] != SYMBOL:
            continue

        amount = float(
            p["positionAmt"]
        )

        if amount > 0:

            result = (
                "LONG",
                amount,
                float(
                    p["entryPrice"]
                )
            )

            break

        if amount < 0:

            result = (
                "SHORT",
                abs(amount),
                float(
                    p["entryPrice"]
                )
            )

            break

    cached_position = result
    last_position_check = now

    return result


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

    return int(
        result.get(
            "leverage",
            LEVERAGE
        )
    )


# ============================================================
# INDICADORES
# ============================================================

def calculate_indicators(df):

    df["ema9"] = (
        df["close"]
        .ewm(
            span=9,
            adjust=False
        )
        .mean()
    )

    df["ema21"] = (
        df["close"]
        .ewm(
            span=21,
            adjust=False
        )
        .mean()
    )

    delta = df["close"].diff()

    gain = delta.clip(
        lower=0
    )

    loss = -delta.clip(
        upper=0
    )

    avg_gain = (
        gain
        .rolling(14)
        .mean()
    )

    avg_loss = (
        loss
        .rolling(14)
        .mean()
    )

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

    df["volume_ma"] = (
        df["volume"]
        .rolling(20)
        .mean()
    )

    return df


# ============================================================
# ANÁLISIS
# ============================================================

def analyze_market():

    if len(candles) < 50:

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

    for col in [
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]:

        df[col] = (
            df[col]
            .astype(float)
        )

    df = calculate_indicators(df)

    last = df.iloc[-1]

    score_long = 0
    score_short = 0

    # TENDENCIA

    if last["ema9"] > last["ema21"]:
        score_long += 2

    if last["ema9"] < last["ema21"]:
        score_short += 2

    # RSI

    if 50 <= last["rsi"] <= 70:
        score_long += 2

    if 30 <= last["rsi"] <= 50:
        score_short += 2

    # VOLUMEN

    if last["volume"] > last["volume_ma"]:

        if last["close"] > last["open"]:
            score_long += 1

        elif last["close"] < last["open"]:
            score_short += 1

    # PRECIO VS EMA

    if last["close"] > last["ema9"]:
        score_long += 1

    if last["close"] < last["ema9"]:
        score_short += 1

    # MOMENTUM

    candle_range = (
        last["high"] -
        last["low"]
    )

    if candle_range > 0:

        body = abs(
            last["close"] -
            last["open"]
        )

        body_ratio = (
            body /
            candle_range
        )

        if body_ratio >= 0.45:

            if last["close"] > last["open"]:
                score_long += 1

            elif last["close"] < last["open"]:
                score_short += 1

    log(
        f"Precio={last['close']:.8f} "
        f"EMA9={last['ema9']:.8f} "
        f"EMA21={last['ema21']:.8f} "
        f"RSI={last['rsi']:.2f} "
        f"L={score_long} "
        f"S={score_short}"
    )

    if (
        score_long >= 4
        and
        score_long >= score_short + 2
    ):

        return (
            "LONG",
            score_long
        )

    if (
        score_short >= 4
        and
        score_short >= score_long + 2
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
    price,
    leverage
):

    balance = get_usdt_balance()

    if balance <= 0:

        raise Exception(
            "No hay balance USDT disponible"
        )

    margin = min(
        MARGIN_PER_TRADE_USDT,
        balance * 0.25
    )

    notional = (
        margin *
        leverage
    )

    quantity = (
        notional /
        price
    )

    quantity = floor_step(
        quantity,
        QTY_STEP
    )

    if quantity < MIN_QTY:

        raise Exception(
            f"Cantidad calculada "
            f"{quantity} menor al "
            f"mínimo configurado "
            f"{MIN_QTY}"
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

        log("Cooldown activo.")
        return

    current_position, amount, current_entry = (
        get_current_position(force=True)
    )

    if current_position is not None:

        log(
            f"Ya existe posición "
            f"{current_position}."
        )

        return

    price = get_price()

    leverage = LEVERAGE

    quantity = calculate_quantity(
        price,
        leverage
    )

    log(
        f"ENTRADA {side} | "
        f"Precio={price} | "
        f"Leverage={leverage}x | "
        f"Cantidad={quantity}"
    )

    if not LIVE_TRADING:

        log(
            "MODO PRUEBA: "
            "NO SE ENVÍA ORDEN."
        )

        return

    # Solo configuramos leverage cuando realmente
    # vamos a operar.
    set_leverage()

    order_side = (
        "BUY"
        if side == "LONG"
        else "SELL"
    )

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

    get_current_position(force=True)


# ============================================================
# CERRAR POSICIÓN
# ============================================================

def close_position(reason):

    global position_side
    global entry_price
    global highest_price
    global lowest_price

    current_position, amount, current_entry = (
        get_current_position(force=True)
    )

    if current_position is None:

        position_side = None
        entry_price = 0.0
        return

    order_side = (
        "SELL"
        if current_position == "LONG"
        else "BUY"
    )

    log(
        f"CERRANDO {current_position} | "
        f"Razón={reason}"
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

    get_current_position(force=True)


# ============================================================
# GESTIÓN DE POSICIÓN
# ============================================================

def manage_position(price):

    global highest_price
    global lowest_price
    global entry_price
    global position_side

    current_position = position_side
    current_entry = entry_price

    if current_position is None:

        pos, amount, entry = (
            get_current_position()
        )

        if pos is None:
            return

        position_side = pos
        entry_price = entry

        current_position = pos
        current_entry = entry

    if current_entry <= 0:
        return

    if current_position == "LONG":

        if highest_price == 0:
            highest_price = current_entry

        if price > highest_price:
            highest_price = price

        stop_price = (
            current_entry *
            (1 - STOP_LOSS_PCT)
        )

        take_profit = (
            current_entry *
            (1 + TAKE_PROFIT_PCT)
        )

        trailing_price = (
            highest_price *
            (1 - TRAILING_DROP_PCT)
        )

        if price <= stop_price:

            close_position("STOP LOSS")
            return

        if price >= take_profit:

            close_position("TAKE PROFIT")
            return

        if (
            price <= trailing_price
            and
            price > current_entry
        ):

            close_position("TRAILING STOP")
            return

    elif current_position == "SHORT":

        if lowest_price == 0:
            lowest_price = current_entry

        if price < lowest_price:
            lowest_price = price

        stop_price = (
            current_entry *
            (1 + STOP_LOSS_PCT)
        )

        take_profit = (
            current_entry *
            (1 - TAKE_PROFIT_PCT)
        )

        trailing_price = (
            lowest_price *
            (1 + TRAILING_DROP_PCT)
        )

        if price >= stop_price:

            close_position("STOP LOSS")
            return

        if price <= take_profit:

            close_position("TAKE PROFIT")
            return

        if (
            price >= trailing_price
            and
            price < current_entry
        ):

            close_position("TRAILING STOP")
            return


# ============================================================
# VELAS MEDIANTE WEBSOCKET
# ============================================================

def update_candle_from_websocket(k):

    candle = [
        k["t"],
        float(k["o"]),
        float(k["h"]),
        float(k["l"]),
        float(k["c"]),
        float(k["v"])
    ]

    if candles:

        if candles[-1][0] == k["t"]:

            candles[-1] = candle

        else:

            candles.append(candle)

    else:

        candles.append(candle)

    if len(candles) > 200:

        candles.pop(0)


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

        update_candle_from_websocket(k)

        manage_position(price)

        if k["x"]:

            if len(candles) < 50:

                log(
                    f"Recolectando velas "
                    f"{len(candles)}/50..."
                )

                return

            signal, score = (
                analyze_market()
            )

            log(
                f"SEÑAL ACTUAL: "
                f"{signal} "
                f"(score {score})"
            )

            if signal == "LONG":

                open_position("LONG")

            elif signal == "SHORT":

                open_position("SHORT")

    except Exception as e:

        log(
            f"ERROR procesando mensaje: {e}"
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
        "WebSocket conectado "
        "correctamente."
    )


# ============================================================
# MAIN
# ============================================================

def main():

    log(
        "===================================="
    )

    log(
        "BOT ONGUSDT FUTURES"
    )

    log(
        "===================================="
    )

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan las variables "
            "BINANCE_API_KEY y "
            "BINANCE_API_SECRET "
            "en Render."
        )

    log(
        f"LIVE_TRADING = {LIVE_TRADING}"
    )

    log(
        f"MARGEN POR OPERACIÓN = "
        f"{MARGIN_PER_TRADE_USDT} USDT"
    )

    log(
        "Modo seguro: NO se consulta "
        "exchangeInfo al iniciar."
    )

    log(
        "Las velas se cargarán "
        "mediante WebSocket."
    )

    # ========================================================
    # Solo consultamos balance/posición.
    # No hacemos exchangeInfo.
    # ========================================================

    if LIVE_TRADING:

        balance = get_usdt_balance()

        log(
            f"Balance Futures disponible: "
            f"{balance:.4f} USDT"
        )

        current_position, amount, entry = (
            get_current_position(force=True)
        )

        if current_position:

            log(
                f"POSICIÓN EXISTENTE: "
                f"{current_position} "
                f"cantidad={amount} "
                f"entrada={entry}"
            )

            global position_side
            global entry_price

            position_side = current_position
            entry_price = entry

    else:

        log(
            "MODO PRUEBA ACTIVADO."
        )

        log(
            "No se enviarán órdenes "
            "reales a Binance."
        )

    log(
        "Esperando 50 velas "
        "cerradas antes de analizar..."
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
            "Reconectando en "
            "30 segundos..."
        )

        time.sleep(30)


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
