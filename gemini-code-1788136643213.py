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

# ============================================================
# TRADING
# ============================================================

LIVE_TRADING = True

MARGIN_PER_TRADE_USDT = 2.5
LEVERAGE = 6

STOP_LOSS_PCT = 0.020
TAKE_PROFIT_PCT = 0.030
TRAILING_DROP_PCT = 0.012

COOLDOWN_SECONDS = 60

# Estrategia MÁS ACTIVA
MIN_SCORE = 3
MIN_SCORE_GAP = 1

# ============================================================
# BINANCE
# ============================================================

BASE_URL = "https://fapi.binance.com"

MARKET_WS = "wss://fstream.binance.com/ws/ongusdt@kline_1m"

# ============================================================
# VARIABLES GLOBALES
# ============================================================

candles = []

current_price = None

position_side = None
position_qty = 0.0
entry_price = 0.0

highest_price = 0.0
lowest_price = 0.0

last_trade_time = 0

rest_pause_until = 0

state_lock = threading.Lock()


# ============================================================
# LOG
# ============================================================

def log(msg):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] {msg}", flush=True)


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

    log(f"REST pausado durante {seconds}s")


# ============================================================
# FIRMA BINANCE
# ============================================================

def signed_request(method, endpoint, params=None):

    if not API_KEY or not API_SECRET:
        raise Exception("Faltan BINANCE_API_KEY o BINANCE_API_SECRET")

    if not rest_allowed():
        raise Exception("REST temporalmente pausado")

    if params is None:
        params = {}

    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000

    query = urlencode(params)

    signature = hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()

    query += "&signature=" + signature

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    url = BASE_URL + endpoint

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
        raise Exception("Método HTTP no soportado")

    if response.status_code in (418, 429):

        retry_after = response.headers.get("Retry-After")

        try:
            wait = int(retry_after)
        except:
            wait = 120

        log(
            f"BINANCE {response.status_code}. "
            f"No se vuelve a insistir. Esperando {wait}s."
        )

        pause_rest(wait)

        raise Exception(
            f"Binance rate limit {response.status_code}"
        )

    if response.status_code >= 400:

        raise Exception(
            f"Binance HTTP {response.status_code}: "
            f"{response.text}"
        )

    return response.json()


# ============================================================
# BALANCE
# SOLO SE USA CUANDO REALMENTE VAMOS A ABRIR
# ============================================================

def get_usdt_balance():

    data = signed_request(
        "GET",
        "/fapi/v2/balance"
    )

    for item in data:

        if item["asset"] == "USDT":

            return float(item["availableBalance"])

    return 0.0


# ============================================================
# LEVERAGE
# SOLO UNA VEZ
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

        log(f"No se pudo configurar leverage: {e}")

        return False


# ============================================================
# CANTIDAD
#
# ONGUSDT se trabaja con cantidad entera en esta versión.
# ============================================================

QTY_STEP = 1.0
MIN_QTY = 1.0


def calculate_quantity(price):

    balance = get_usdt_balance()

    if balance <= 0:
        raise Exception("No hay balance USDT disponible")

    margin = min(
        MARGIN_PER_TRADE_USDT,
        balance
    )

    notional = margin * LEVERAGE

    quantity = notional / price

    quantity = math.floor(
        quantity / QTY_STEP
    ) * QTY_STEP

    if quantity < MIN_QTY:
        quantity = MIN_QTY

    quantity = float(
        f"{quantity:.8f}"
    )

    return quantity


# ============================================================
# ABRIR POSICIÓN
# ============================================================

def open_position(side):

    global last_trade_time

    global position_side
    global position_qty
    global entry_price

    global highest_price
    global lowest_price

    now = time.time()

    if now - last_trade_time < COOLDOWN_SECONDS:

        log("Cooldown activo")
        return

    with state_lock:

        if position_side is not None:

            log(
                f"Ya existe posición "
                f"{position_side}"
            )

            return

    if current_price is None:

        log("Sin precio todavía")
        return

    price = current_price

    log(
        f"SEÑAL {side} | "
        f"Precio={price:.6f}"
    )

    if not LIVE_TRADING:

        log(
            "LIVE_TRADING=False -> "
            "no se ejecuta orden"
        )

        return

    try:

        quantity = calculate_quantity(price)

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
                "quantity": quantity
            }
        )

        log(
            f"ORDEN EJECUTADA: "
            f"{side} "
            f"qty={quantity}"
        )

        # El precio real de entrada será actualizado
        # por ACCOUNT_UPDATE / ORDER_TRADE_UPDATE.
        with state_lock:

            position_side = side
            position_qty = quantity
            entry_price = price

            highest_price = price
            lowest_price = price

        last_trade_time = time.time()

    except Exception as e:

        log(f"ERROR ABRIENDO POSICIÓN: {e}")


# ============================================================
# CERRAR POSICIÓN
# ============================================================

def close_position(reason="signal"):

    global position_side
    global position_qty
    global entry_price

    global highest_price
    global lowest_price

    global last_trade_time

    with state_lock:

        side = position_side
        qty = position_qty

    if side is None or qty <= 0:

        return

    log(
        f"CERRANDO {side} | "
        f"Motivo: {reason}"
    )

    if not LIVE_TRADING:

        log(
            "LIVE_TRADING=False -> "
            "no se ejecuta cierre"
        )

        return

    try:

        close_side = (
            "SELL"
            if side == "LONG"
            else "BUY"
        )

        result = signed_request(
            "POST",
            "/fapi/v1/order",
            {
                "symbol": SYMBOL,
                "side": close_side,
                "type": "MARKET",
                "quantity": qty,
                "reduceOnly": "true"
            }
        )

        log("POSICIÓN CERRADA")

        with state_lock:

            position_side = None
            position_qty = 0.0
            entry_price = 0.0

            highest_price = 0.0
            lowest_price = 0.0

        last_trade_time = time.time()

    except Exception as e:

        log(f"ERROR CERRANDO POSICIÓN: {e}")


# ============================================================
# INDICADORES
# ============================================================

def calculate_signal(df):

    if len(df) < 30:
        return None

    df = df.copy()

    df["ema9"] = (
        df["close"]
        .ewm(span=9, adjust=False)
        .mean()
    )

    df["ema21"] = (
        df["close"]
        .ewm(span=21, adjust=False)
        .mean()
    )

    delta = df["close"].diff()

    gain = delta.clip(lower=0)

    loss = -delta.clip(upper=0)

    avg_gain = (
        gain.rolling(14)
        .mean()
    )

    avg_loss = (
        loss.rolling(14)
        .mean()
    )

    rs = avg_gain / avg_loss.replace(
        0,
        1e-10
    )

    df["rsi"] = 100 - (
        100 / (1 + rs)
    )

    df["volume_ma"] = (
        df["volume"]
        .rolling(20)
        .mean()
    )

    last = df.iloc[-1]

    previous = df.iloc[-2]

    close = float(last["close"])

    ema9 = float(last["ema9"])

    ema21 = float(last["ema21"])

    rsi = float(last["rsi"])

    volume = float(last["volume"])

    volume_ma = float(last["volume_ma"])

    open_price = float(last["open"])

    body = close - open_price

    previous_close = float(
        previous["close"]
    )

    # ========================================================
    # LONG
    # ========================================================

    long_score = 0

    if ema9 > ema21:
        long_score += 1

    if close > ema9:
        long_score += 1

    if 50 < rsi < 72:
        long_score += 1

    if volume > volume_ma:
        long_score += 1

    if body > 0:
        long_score += 1

    if close > previous_close:
        long_score += 1

    # ========================================================
    # SHORT
    # ========================================================

    short_score = 0

    if ema9 < ema21:
        short_score += 1

    if close < ema9:
        short_score += 1

    if 28 < rsi < 50:
        short_score += 1

    if volume > volume_ma:
        short_score += 1

    if body < 0:
        short_score += 1

    if close < previous_close:
        short_score += 1

    log(
        f"SIGNAL | "
        f"price={close:.6f} | "
        f"EMA9={ema9:.6f} | "
        f"EMA21={ema21:.6f} | "
        f"RSI={rsi:.1f} | "
        f"LONG={long_score} | "
        f"SHORT={short_score}"
    )

    # ========================================================
    # ENTRADA MÁS ACTIVA
    # ========================================================

    if (
        long_score >= MIN_SCORE
        and long_score - short_score >= MIN_SCORE_GAP
    ):

        return "LONG"

    if (
        short_score >= MIN_SCORE
        and short_score - long_score >= MIN_SCORE_GAP
    ):

        return "SHORT"

    return None


# ============================================================
# PROCESAR VELA CERRADA
# ============================================================

def process_candle():

    if len(candles) < 30:
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

    df["open"] = pd.to_numeric(
        df["open"]
    )

    df["high"] = pd.to_numeric(
        df["high"]
    )

    df["low"] = pd.to_numeric(
        df["low"]
    )

    df["close"] = pd.to_numeric(
        df["close"]
    )

    df["volume"] = pd.to_numeric(
        df["volume"]
    )

    signal = calculate_signal(df)

    if signal is None:
        return

    with state_lock:

        current_side = position_side

    # ========================================================
    # SIN POSICIÓN
    # ========================================================

    if current_side is None:

        open_position(signal)

        return

    # ========================================================
    # CAMBIO DE DIRECCIÓN
    # ========================================================

    if current_side == "LONG" and signal == "SHORT":

        close_position(
            "señal contraria"
        )

        time.sleep(1)

        open_position("SHORT")

    elif current_side == "SHORT" and signal == "LONG":

        close_position(
            "señal contraria"
        )

        time.sleep(1)

        open_position("LONG")


# ============================================================
# MANEJAR POSICIÓN
# ============================================================

def manage_position():

    global highest_price
    global lowest_price

    with state_lock:

        side = position_side
        entry = entry_price

    if side is None or entry <= 0:
        return

    price = current_price

    if price is None:
        return

    # ========================================================
    # LONG
    # ========================================================

    if side == "LONG":

        if highest_price == 0:
            highest_price = price

        highest_price = max(
            highest_price,
            price
        )

        stop_price = (
            entry *
            (1 - STOP_LOSS_PCT)
        )

        take_price = (
            entry *
            (1 + TAKE_PROFIT_PCT)
        )

        trailing_price = (
            highest_price *
            (1 - TRAILING_DROP_PCT)
        )

        if price <= stop_price:

            close_position(
                "STOP LOSS"
            )

        elif price >= take_price:

            close_position(
                "TAKE PROFIT"
            )

        elif (
            highest_price > entry
            and price <= trailing_price
        ):

            close_position(
                "TRAILING STOP"
            )

    # ========================================================
    # SHORT
    # ========================================================

    elif side == "SHORT":

        if lowest_price == 0:
            lowest_price = price

        lowest_price = min(
            lowest_price,
            price
        )

        stop_price = (
            entry *
            (1 + STOP_LOSS_PCT)
        )

        take_price = (
            entry *
            (1 - TAKE_PROFIT_PCT)
        )

        trailing_price = (
            lowest_price *
            (1 + TRAILING_DROP_PCT)
        )

        if price >= stop_price:

            close_position(
                "STOP LOSS"
            )

        elif price <= take_price:

            close_position(
                "TAKE PROFIT"
            )

        elif (
            lowest_price < entry
            and price >= trailing_price
        ):

            close_position(
                "TRAILING STOP"
            )


# ============================================================
# WEBSOCKET DE MERCADO
# ============================================================

def on_market_message(ws, message):

    global current_price

    try:

        data = json.loads(message)

        kline = data.get("k")

        if not kline:
            return

        close_price = float(
            kline["c"]
        )

        current_price = close_price

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

                if candles[-1][0] == candle[0]:

                    candles[-1] = candle

                else:

                    candles.append(candle)

            else:

                candles.append(candle)

            if len(candles) > 300:

                del candles[:-300]

            process_candle()

    except Exception as e:

        log(
            f"Error procesando market WS: {e}"
        )


def on_market_error(ws, error):

    log(
        f"Market WS error: {error}"
    )


def on_market_close(ws, close_status_code, close_msg):

    log(
        f"Market WS cerrado: "
        f"{close_status_code} {close_msg}"
    )


def on_market_open(ws):

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
            "Reconexión Market WS en 60 segundos..."
        )

        time.sleep(60)


# ============================================================
# USER DATA STREAM
# ============================================================

def create_listen_key():

    if not rest_allowed():

        raise Exception(
            "REST pausado"
        )

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    response = requests.post(
        BASE_URL + "/fapi/v1/listenKey",
        headers=headers,
        timeout=10
    )

    if response.status_code in (418, 429):

        retry_after = response.headers.get(
            "Retry-After"
        )

        try:
            wait = int(retry_after)
        except:
            wait = 120

        pause_rest(wait)

        raise Exception(
            f"User stream rate limit "
            f"{response.status_code}"
        )

    if response.status_code >= 400:

        raise Exception(
            response.text
        )

    return response.json()["listenKey"]


def keepalive_listen_key(listen_key):

    if not rest_allowed():
        return

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    try:

        response = requests.put(
            BASE_URL + "/fapi/v1/listenKey",
            headers=headers,
            timeout=10
        )

        if response.status_code in (418, 429):

            retry_after = response.headers.get(
                "Retry-After"
            )

            try:
                wait = int(retry_after)
            except:
                wait = 120

            pause_rest(wait)

            log(
                f"User stream keepalive: "
                f"Binance {response.status_code}"
            )

    except Exception as e:

        log(
            f"Keepalive error: {e}"
        )


def user_stream_keepalive_loop(listen_key):

    while True:

        time.sleep(45 * 60)

        keepalive_listen_key(
            listen_key
        )


# ============================================================
# USER DATA EVENTS
# ============================================================

def process_account_update(data):

    global position_side
    global position_qty
    global entry_price

    account = data.get("a", {})

    positions = account.get(
        "P",
        []
    )

    for p in positions:

        if p.get("s") != SYMBOL:
            continue

        amt = float(
            p.get("pa", 0)
        )

        entry = float(
            p.get("ep", 0)
        )

        if amt > 0:

            position_side = "LONG"
            position_qty = amt
            entry_price = entry

            log(
                f"ACCOUNT UPDATE -> "
                f"LONG qty={amt} "
                f"entry={entry}"
            )

        elif amt < 0:

            position_side = "SHORT"
            position_qty = abs(amt)
            entry_price = entry

            log(
                f"ACCOUNT UPDATE -> "
                f"SHORT qty={abs(amt)} "
                f"entry={entry}"
            )

        else:

            position_side = None
            position_qty = 0.0
            entry_price = 0.0

            highest_price = 0.0
            lowest_price = 0.0

            log(
                "ACCOUNT UPDATE -> "
                "posición cerrada"
            )


def process_order_update(data):

    order = data.get("o", {})

    symbol = order.get("s")

    if symbol != SYMBOL:
        return

    status = order.get("X")

    side = order.get("S")

    executed_qty = order.get(
        "z",
        "0"
    )

    avg_price = order.get(
        "ap",
        "0"
    )

    if status == "FILLED":

        log(
            f"ORDER FILLED | "
            f"side={side} | "
            f"qty={executed_qty} | "
            f"avg={avg_price}"
        )


def on_user_message(ws, message):

    try:

        data = json.loads(message)

        event_type = data.get("e")

        if event_type == "ACCOUNT_UPDATE":

            process_account_update(
                data
            )

        elif event_type == "ORDER_TRADE_UPDATE":

            process_order_update(
                data
            )

        elif event_type == "listenKeyExpired":

            log(
                "ListenKey expirado"
            )

    except Exception as e:

        log(
            f"Error User WS: {e}"
        )


def on_user_error(ws, error):

    log(
        f"User WS error: {error}"
    )


def on_user_close(ws, code, msg):

    log(
        f"User WS cerrado: {code} {msg}"
    )


def on_user_open(ws):

    log(
        "USER DATA WEBSOCKET CONECTADO"
    )


def user_websocket_loop():

    while True:

        listen_key = None

        try:

            log(
                "Creando User Data Stream..."
            )

            listen_key = create_listen_key()

            ws_url = (
                "wss://fstream.binance.com/ws/"
                + listen_key
            )

            ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_user_open,
                on_message=on_user_message,
                on_error=on_user_error,
                on_close=on_user_close
            )

            ws.run_forever(
                ping_interval=60,
                ping_timeout=20
            )

        except Exception as e:

            log(
                f"User WS exception: {e}"
            )

        log(
            "Reconexión User WS en 60 segundos..."
        )

        time.sleep(60)


# ============================================================
# HILO PARA CONTROLAR POSICIÓN
# ============================================================

def position_manager_loop():

    while True:

        try:

            manage_position()

        except Exception as e:

            log(
                f"Position manager error: {e}"
            )

        time.sleep(1)


# ============================================================
# HEALTH CHECK RENDER
# ============================================================

class HealthHandler(
    BaseHTTPRequestHandler
):

    def do_GET(self):

        self.send_response(200)

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
        ("0.0.0.0", port),
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
        "===================================="
    )

    log(
        "      ONGUSDT FUTURES BOT"
    )

    log(
        "===================================="
    )

    log(
        f"LIVE_TRADING = {LIVE_TRADING}"
    )

    log(
        f"MARGIN = {MARGIN_PER_TRADE_USDT} USDT"
    )

    log(
        f"LEVERAGE = {LEVERAGE}x"
    )

    log(
        f"MIN_SCORE = {MIN_SCORE}"
    )

    log(
        f"MIN_SCORE_GAP = {MIN_SCORE_GAP}"
    )

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan las variables "
            "BINANCE_API_KEY y "
            "BINANCE_API_SECRET"
        )

    # ========================================================
    # LEVERAGE
    # ========================================================

    if LIVE_TRADING:

        log(
            "Configurando leverage..."
        )

        set_leverage()

    # ========================================================
    # HEALTH
    # ========================================================

    threading.Thread(
        target=health_server,
        daemon=True
    ).start()

    # ========================================================
    # MARKET WEBSOCKET
    # ========================================================

    threading.Thread(
        target=market_websocket_loop,
        daemon=True
    ).start()

    # ========================================================
    # USER DATA WEBSOCKET
    # ========================================================

    threading.Thread(
        target=user_websocket_loop,
        daemon=True
    ).start()

    # ========================================================
    # POSITION MANAGER
    # ========================================================

    threading.Thread(
        target=position_manager_loop,
        daemon=True
    ).start()

    log(
        "BOT INICIADO CORRECTAMENTE"
    )

    log(
        "Esperando datos de mercado..."
    )

    while True:

        time.sleep(60)

        log(
            f"STATUS | "
            f"price={current_price} | "
            f"position={position_side} | "
            f"qty={position_qty}"
        )


# ============================================================
# ARRANQUE
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except Exception as e:

        log(
            f"FATAL ERROR: {e}"
        )

        time.sleep(30)
