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
import uuid
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
# MODO: cambia entre real y testnet con una sola variable
# En Railway podes setear USE_TESTNET=true como variable de entorno
# ============================================================

USE_TESTNET = os.getenv("USE_TESTNET", "true").lower() == "true"

if USE_TESTNET:
    BASE_URL = "https://testnet.binancefuture.com"
    MARKET_WS = "wss://stream.binancefuture.com/ws/ongusdt@kline_1m"
    WS_API_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"
else:
    BASE_URL = "https://fapi.binance.com"
    MARKET_WS = "wss://fstream.binance.com/ws/ongusdt@kline_1m"
    WS_API_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"

# ============================================================
# TRADING
# ============================================================

# Se ejecutan ordenes reales solo si esto es True.
# En Testnet no importa (es dinero ficticio), pero en real
# arrancalo en False un rato para ver logs antes de operar.
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"

MARGIN_PER_TRADE_USDT = 2.5
MARGIN_MIN_USDT = 1.0        # margen minimo si la volatilidad esta muy alta
MARGIN_MAX_USDT = 4.0        # margen maximo si la volatilidad esta muy baja
LEVERAGE = 6
MARGIN_TYPE = "ISOLATED"  # ISOLATED o CROSSED

# Stop/Take/Trailing ahora se calculan en base al ATR (volatilidad reciente)
# en vez de un porcentaje fijo. Estos multiplicadores son lo que se ajusta.
ATR_PERIOD = 14
ATR_STOP_MULT = 1.5
ATR_TAKE_MULT = 2.5
ATR_TRAILING_MULT = 1.0

# Si el ATR relativo (ATR/precio) esta por debajo de esto, el mercado
# esta demasiado plano y el bot no opera (evita ruido / señales falsas)
MIN_ATR_PCT = 0.003  # 0.3%

COOLDOWN_SECONDS = 60

MIN_SCORE = 3
MIN_SCORE_GAP = 1

# ============================================================
# VARIABLES
# ============================================================

candles = []

current_price = None

position_side = None
position_qty = 0.0
entry_price = 0.0

highest_price = 0.0
lowest_price = 0.0

# ATR actual, se recalcula en cada vela cerrada
current_atr = None

last_trade_time = 0

rest_pause_until = 0

state_lock = threading.Lock()

user_stream_control = None
user_stream_control_lock = threading.Lock()

listen_key = None

# Reglas reales del simbolo, se completan al arrancar
QTY_STEP = 1.0
MIN_QTY = 1.0
MIN_NOTIONAL = 5.0


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
# REST BINANCE
# ============================================================

def signed_request(method, endpoint, params=None):

    if not API_KEY or not API_SECRET:
        raise Exception("Faltan BINANCE_API_KEY o BINANCE_API_SECRET")

    if not rest_allowed():
        raise Exception("REST temporalmente pausado")

    if params is None:
        params = {}

    params = dict(params)
    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000

    query = urlencode(params)
    signature = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    query += "&signature=" + signature

    headers = {"X-MBX-APIKEY": API_KEY}
    url = BASE_URL + endpoint

    try:
        if method == "GET":
            response = requests.get(url, headers=headers, params=query, timeout=10)
        elif method == "POST":
            response = requests.post(url, headers=headers, params=query, timeout=10)
        elif method == "DELETE":
            response = requests.delete(url, headers=headers, params=query, timeout=10)
        else:
            raise Exception("Método HTTP no soportado")
    except requests.RequestException as e:
        raise Exception(f"Error de conexión REST: {e}")

    if response.status_code in (418, 429):
        retry_after = response.headers.get("Retry-After")
        try:
            wait = int(retry_after)
        except:
            wait = 120
        log(f"BINANCE {response.status_code}. NO se vuelve a insistir. Esperando {wait}s.")
        pause_rest(wait)
        raise Exception(f"Binance rate limit {response.status_code}")

    if response.status_code >= 400:
        raise Exception(f"Binance HTTP {response.status_code}: {response.text}")

    return response.json()


def public_request(endpoint, params=None):
    """Request publico, sin firma (para exchangeInfo)."""
    url = BASE_URL + endpoint
    response = requests.get(url, params=params, timeout=10)
    if response.status_code >= 400:
        raise Exception(f"Binance HTTP {response.status_code}: {response.text}")
    return response.json()


# ============================================================
# REGLAS DEL SIMBOLO (exchangeInfo)
# ============================================================

def load_symbol_rules():
    """Trae step size, minQty y minNotional reales desde Binance."""

    global QTY_STEP, MIN_QTY, MIN_NOTIONAL

    try:
        data = public_request("/fapi/v1/exchangeInfo")

        for s in data.get("symbols", []):

            if s.get("symbol") != SYMBOL:
                continue

            for f in s.get("filters", []):

                if f.get("filterType") == "LOT_SIZE":
                    QTY_STEP = float(f.get("stepSize", QTY_STEP))
                    MIN_QTY = float(f.get("minQty", MIN_QTY))

                if f.get("filterType") == "MIN_NOTIONAL":
                    MIN_NOTIONAL = float(f.get("notional", MIN_NOTIONAL))

            log(
                f"Reglas {SYMBOL} cargadas | "
                f"step={QTY_STEP} min_qty={MIN_QTY} "
                f"min_notional={MIN_NOTIONAL}"
            )
            return

        log(f"No se encontraron reglas para {SYMBOL}, se usan valores por defecto")

    except Exception as e:
        log(f"No se pudo cargar exchangeInfo, se usan valores por defecto: {e}")


# ============================================================
# BALANCE
# ============================================================

def get_usdt_balance():
    data = signed_request("GET", "/fapi/v2/balance")
    for item in data:
        if item["asset"] == "USDT":
            return float(item["availableBalance"])
    return 0.0


# ============================================================
# LEVERAGE Y MARGIN TYPE
# ============================================================

def set_leverage():
    try:
        result = signed_request("POST", "/fapi/v1/leverage", {"symbol": SYMBOL, "leverage": LEVERAGE})
        log(f"Leverage configurado: {result.get('leverage', LEVERAGE)}x")
        return True
    except Exception as e:
        log(f"No se pudo configurar leverage: {e}")
        return False


def set_margin_type():
    try:
        signed_request("POST", "/fapi/v1/marginType", {"symbol": SYMBOL, "marginType": MARGIN_TYPE})
        log(f"Margin type configurado: {MARGIN_TYPE}")
    except Exception as e:
        # Binance tira error si ya estaba seteado en ese modo; no es fatal
        log(f"Margin type (puede que ya estuviera seteado): {e}")


# ============================================================
# ATR (Average True Range) - mide volatilidad reciente
# ============================================================

def calculate_atr(df, period=ATR_PERIOD):

    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)

    atr = tr.rolling(period).mean()

    return float(atr.iloc[-1]) if not atr.empty else None


# ============================================================
# CANTIDAD (ajustada por volatilidad)
# ============================================================

def calculate_margin_by_volatility(price):
    """
    Mas volatilidad relativa -> menos margen por operacion.
    Menos volatilidad relativa -> mas margen (dentro de los limites).
    """

    if current_atr is None or price <= 0:
        return MARGIN_PER_TRADE_USDT

    atr_pct = current_atr / price

    # A mayor atr_pct, factor mas chico. Referencia: 0.3% = base, 1.5% = minimo
    if atr_pct <= MIN_ATR_PCT:
        factor = 1.0
    else:
        factor = max(0.3, MIN_ATR_PCT / atr_pct)

    margin = MARGIN_PER_TRADE_USDT * factor

    margin = max(MARGIN_MIN_USDT, min(MARGIN_MAX_USDT, margin))

    return margin


def calculate_quantity(price):

    balance = get_usdt_balance()

    if balance <= 0:
        raise Exception("No hay balance USDT disponible")

    dynamic_margin = calculate_margin_by_volatility(price)

    margin = min(dynamic_margin, balance)
    notional = margin * LEVERAGE

    if notional < MIN_NOTIONAL:
        raise Exception(
            f"Notional {notional:.2f} USDT por debajo "
            f"del minimo permitido ({MIN_NOTIONAL} USDT)"
        )

    quantity = notional / price
    quantity = math.floor(quantity / QTY_STEP) * QTY_STEP

    if quantity < MIN_QTY:
        quantity = MIN_QTY

    return float(f"{quantity:.8f}")


# ============================================================
# ABRIR / CERRAR POSICIÓN
# ============================================================

def open_position(side):

    global last_trade_time, position_side, position_qty, entry_price
    global highest_price, lowest_price

    now = time.time()
    if now - last_trade_time < COOLDOWN_SECONDS:
        log("Cooldown activo")
        return

    with state_lock:
        if position_side is not None:
            log(f"Ya existe posición {position_side}")
            return

    if current_price is None:
        log("Sin precio todavía")
        return

    price = current_price
    log(f"SEÑAL {side} | Precio={price:.6f}")

    if not LIVE_TRADING:
        log("LIVE_TRADING=False -> no se ejecuta orden")
        return

    try:
        quantity = calculate_quantity(price)
        order_side = "BUY" if side == "LONG" else "SELL"

        signed_request("POST", "/fapi/v1/order", {
            "symbol": SYMBOL,
            "side": order_side,
            "type": "MARKET",
            "quantity": quantity
        })

        log(f"ORDEN EJECUTADA: {side} qty={quantity}")

        with state_lock:
            position_side = side
            position_qty = quantity
            entry_price = price
            highest_price = price
            lowest_price = price

        last_trade_time = time.time()

    except Exception as e:
        log(f"ERROR ABRIENDO POSICIÓN: {e}")


def close_position(reason="signal"):

    global position_side, position_qty, entry_price
    global highest_price, lowest_price, last_trade_time

    with state_lock:
        side = position_side
        qty = position_qty

    if side is None or qty <= 0:
        return

    log(f"CERRANDO {side} | Motivo: {reason}")

    if not LIVE_TRADING:
        log("LIVE_TRADING=False -> no se ejecuta cierre")
        return

    try:
        close_side = "SELL" if side == "LONG" else "BUY"

        signed_request("POST", "/fapi/v1/order", {
            "symbol": SYMBOL,
            "side": close_side,
            "type": "MARKET",
            "quantity": qty,
            "reduceOnly": "true"
        })

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
    df["ema9"] = df["close"].ewm(span=9, adjust=False).mean()
    df["ema21"] = df["close"].ewm(span=21, adjust=False).mean()

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss.replace(0, 1e-10)
    df["rsi"] = 100 - (100 / (1 + rs))

    df["volume_ma"] = df["volume"].rolling(20).mean()

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
    previous_close = float(previous["close"])

    long_score = 0
    if ema9 > ema21: long_score += 1
    if close > ema9: long_score += 1
    if 50 < rsi < 72: long_score += 1
    if volume > volume_ma: long_score += 1
    if body > 0: long_score += 1
    if close > previous_close: long_score += 1

    short_score = 0
    if ema9 < ema21: short_score += 1
    if close < ema9: short_score += 1
    if 28 < rsi < 50: short_score += 1
    if volume > volume_ma: short_score += 1
    if body < 0: short_score += 1
    if close < previous_close: short_score += 1

    log(
        f"SIGNAL | price={close:.6f} | EMA9={ema9:.6f} | "
        f"EMA21={ema21:.6f} | RSI={rsi:.1f} | "
        f"LONG={long_score} | SHORT={short_score}"
    )

    if long_score >= MIN_SCORE and long_score - short_score >= MIN_SCORE_GAP:
        return "LONG"

    if short_score >= MIN_SCORE and short_score - long_score >= MIN_SCORE_GAP:
        return "SHORT"

    return None


# ============================================================
# PROCESAR VELA
# ============================================================

def process_candle():

    global current_atr

    if len(candles) < 30:
        return

    df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col])

    current_atr = calculate_atr(df)

    if current_price and current_atr:
        atr_pct = current_atr / current_price
        if atr_pct < MIN_ATR_PCT:
            log(f"Volatilidad muy baja (ATR={atr_pct*100:.2f}%), no se opera")
            return

    signal = calculate_signal(df)

    if signal is None:
        return

    with state_lock:
        current_side = position_side

    if current_side is None:
        open_position(signal)
        return

    if current_side == "LONG" and signal == "SHORT":
        close_position("señal contraria")
        time.sleep(1)
        open_position("SHORT")

    elif current_side == "SHORT" and signal == "LONG":
        close_position("señal contraria")
        time.sleep(1)
        open_position("LONG")


# ============================================================
# CONTROL DE POSICIÓN
# ============================================================

def manage_position():

    global highest_price, lowest_price

    with state_lock:
        side = position_side
        entry = entry_price

    if side is None or entry <= 0:
        return

    price = current_price
    if price is None:
        return

    # Si por algun motivo todavia no hay ATR calculado, usa un fallback
    # chico en % para no dejar la posicion sin proteccion
    atr = current_atr if current_atr else entry * 0.01

    stop_distance = atr * ATR_STOP_MULT
    take_distance = atr * ATR_TAKE_MULT
    trailing_distance = atr * ATR_TRAILING_MULT

    if side == "LONG":

        if highest_price == 0:
            highest_price = price
        highest_price = max(highest_price, price)

        stop_price = entry - stop_distance
        take_price = entry + take_distance
        trailing_price = highest_price - trailing_distance

        if price <= stop_price:
            close_position("STOP LOSS (ATR)")
        elif price >= take_price:
            close_position("TAKE PROFIT (ATR)")
        elif highest_price > entry and price <= trailing_price:
            close_position("TRAILING STOP (ATR)")

    elif side == "SHORT":

        if lowest_price == 0:
            lowest_price = price
        lowest_price = min(lowest_price, price)

        stop_price = entry + stop_distance
        take_price = entry - take_distance
        trailing_price = lowest_price + trailing_distance

        if price >= stop_price:
            close_position("STOP LOSS (ATR)")
        elif price <= take_price:
            close_position("TAKE PROFIT (ATR)")
        elif lowest_price < entry and price >= trailing_price:
            close_position("TRAILING STOP (ATR)")


# ============================================================
# MARKET WEBSOCKET
# ============================================================

def on_market_message(ws, message):

    global current_price

    try:
        data = json.loads(message)
        kline = data.get("k")
        if not kline:
            return

        current_price = float(kline["c"])

        if kline["x"]:
            candle = [
                int(kline["t"]), float(kline["o"]), float(kline["h"]),
                float(kline["l"]), float(kline["c"]), float(kline["v"])
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
        log(f"Error market WS: {e}")


def on_market_error(ws, error):
    log(f"Market WS error: {error}")


def on_market_close(ws, code, msg):
    log(f"Market WS cerrado: {code} {msg}")


def on_market_open(ws):
    log("MARKET WEBSOCKET CONECTADO")


def market_websocket_loop():

    while True:
        try:
            log("Conectando Market WebSocket...")
            ws = websocket.WebSocketApp(
                MARKET_WS,
                on_open=on_market_open,
                on_message=on_market_message,
                on_error=on_market_error,
                on_close=on_market_close
            )
            ws.run_forever(ping_interval=60, ping_timeout=20)

        except Exception as e:
            log(f"Market WS exception: {e}")

        log("Reconexión Market WS en 60 segundos...")
        time.sleep(60)


# ============================================================
# USER DATA STREAM
# ============================================================

def start_user_data_stream():

    global user_stream_control, listen_key

    log("Abriendo conexión WS API para User Data Stream...")

    ws = websocket.create_connection(WS_API_URL, timeout=15)

    request_id = str(uuid.uuid4())
    request = {"id": request_id, "method": "userDataStream.start", "params": {"apiKey": API_KEY}}

    ws.send(json.dumps(request))
    response = json.loads(ws.recv())

    if response.get("status") != 200:
        ws.close()
        raise Exception(f"UserDataStream.start rechazado: {response}")

    key = response.get("result", {}).get("listenKey")

    if not key:
        ws.close()
        raise Exception("Binance no devolvió listenKey")

    with user_stream_control_lock:
        user_stream_control = ws

    listen_key = key

    log("USER DATA STREAM CREADO POR WS API")
    log("ListenKey recibido correctamente")

    return listen_key


def user_stream_keepalive_loop():

    global user_stream_control, listen_key

    while True:

        time.sleep(45 * 60)

        try:
            with user_stream_control_lock:
                ws = user_stream_control

            if ws is None:
                log("Keepalive: no hay conexión WS API")
                continue

            request_id = str(uuid.uuid4())
            request = {"id": request_id, "method": "userDataStream.ping", "params": {"apiKey": API_KEY}}

            with user_stream_control_lock:
                ws.send(json.dumps(request))
                ws.settimeout(15)
                response = json.loads(ws.recv())

            if response.get("status") == 200:
                new_key = response.get("result", {}).get("listenKey")
                if new_key:
                    listen_key = new_key  # ahora si actualiza la variable global
                log("USER DATA STREAM KEEPALIVE OK")
            else:
                log(f"USER DATA KEEPALIVE RESPUESTA: {response}")

        except Exception as e:
            log(f"User Data keepalive error: {e}")
            with user_stream_control_lock:
                try:
                    if user_stream_control:
                        user_stream_control.close()
                except:
                    pass
                user_stream_control = None


def process_account_update(data):

    global position_side, position_qty, entry_price, highest_price, lowest_price

    account = data.get("a", {})
    positions = account.get("P", [])

    for p in positions:

        if p.get("s") != SYMBOL:
            continue

        amt = float(p.get("pa", 0))
        entry = float(p.get("ep", 0))

        with state_lock:

            if amt > 0:
                position_side = "LONG"
                position_qty = amt
                entry_price = entry
                if highest_price == 0: highest_price = entry
                if lowest_price == 0: lowest_price = entry
                log(f"ACCOUNT UPDATE -> LONG qty={amt} entry={entry}")

            elif amt < 0:
                position_side = "SHORT"
                position_qty = abs(amt)
                entry_price = entry
                if highest_price == 0: highest_price = entry
                if lowest_price == 0: lowest_price = entry
                log(f"ACCOUNT UPDATE -> SHORT qty={abs(amt)} entry={entry}")

            else:
                position_side = None
                position_qty = 0.0
                entry_price = 0.0
                highest_price = 0.0
                lowest_price = 0.0
                log("ACCOUNT UPDATE -> posición cerrada")


def process_order_update(data):

    order = data.get("o", {})
    if order.get("s") != SYMBOL:
        return

    status = order.get("X")
    side = order.get("S")
    executed_qty = order.get("z", "0")
    avg_price = order.get("ap", "0")

    if status == "FILLED":
        log(f"ORDER FILLED | side={side} | qty={executed_qty} | avg={avg_price}")


def on_user_message(ws, message):
    try:
        data = json.loads(message)
        event_type = data.get("e")

        if event_type == "ACCOUNT_UPDATE":
            process_account_update(data)
        elif event_type == "ORDER_TRADE_UPDATE":
            process_order_update(data)
        elif event_type == "listenKeyExpired":
            log("ListenKey expirado")

    except Exception as e:
        log(f"Error User WS: {e}")


def on_user_error(ws, error):
    log(f"User WS error: {error}")


def on_user_close(ws, code, msg):
    log(f"User WS cerrado: {code} {msg}")


def on_user_open(ws):
    log("USER DATA WEBSOCKET CONECTADO")


def user_websocket_loop():

    global listen_key, user_stream_control

    while True:

        stream_ws = None

        try:
            key = start_user_data_stream()
            ws_url = "wss://fstream.binance.com/ws/" + key

            if USE_TESTNET:
                ws_url = "wss://stream.binancefuture.com/ws/" + key

            log("Conectando User Data WebSocket...")

            stream_ws = websocket.WebSocketApp(
                ws_url,
                on_open=on_user_open,
                on_message=on_user_message,
                on_error=on_user_error,
                on_close=on_user_close
            )

            stream_ws.run_forever(ping_interval=60, ping_timeout=20)

        except Exception as e:
            log(f"User WS exception: {e}")

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

        log("Reconexión User Data en 60 segundos...")
        time.sleep(60)


# ============================================================
# POSITION MANAGER
# ============================================================

def position_manager_loop():
    while True:
        try:
            manage_position()
        except Exception as e:
            log(f"Position manager error: {e}")
        time.sleep(1)


# ============================================================
# HEALTH SERVER
# ============================================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"BOT ONLINE")

    def log_message(self, format, *args):
        return


def health_server():
    port = int(os.getenv("PORT", "10000"))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    log(f"Health server escuchando en puerto {port}")
    server.serve_forever()


# ============================================================
# MAIN
# ============================================================

def main():

    log("====================================")
    log("      ONGUSDT FUTURES BOT")
    log("====================================")
    log(f"USE_TESTNET = {USE_TESTNET}")
    log(f"LIVE_TRADING = {LIVE_TRADING}")
    log(f"MARGIN base = {MARGIN_PER_TRADE_USDT} USDT (rango {MARGIN_MIN_USDT}-{MARGIN_MAX_USDT})")
    log(f"LEVERAGE = {LEVERAGE}x")
    log(f"ATR_STOP_MULT = {ATR_STOP_MULT} | ATR_TAKE_MULT = {ATR_TAKE_MULT}")
    log(f"MIN_ATR_PCT = {MIN_ATR_PCT*100:.2f}%")
    log(f"MIN_SCORE = {MIN_SCORE}")
    log(f"MIN_SCORE_GAP = {MIN_SCORE_GAP}")

    if not API_KEY or not API_SECRET:
        raise Exception("Faltan las variables BINANCE_API_KEY y BINANCE_API_SECRET")

    log("Cargando reglas del símbolo (exchangeInfo)...")
    load_symbol_rules()

    if LIVE_TRADING:
        log("Configurando leverage y margin type...")
        set_leverage()
        set_margin_type()

    threading.Thread(target=health_server, daemon=True).start()
    threading.Thread(target=market_websocket_loop, daemon=True).start()
    threading.Thread(target=user_websocket_loop, daemon=True).start()
    threading.Thread(target=user_stream_keepalive_loop, daemon=True).start()
    threading.Thread(target=position_manager_loop, daemon=True).start()

    log("BOT INICIADO CORRECTAMENTE")
    log("Esperando datos de mercado...")

    while True:
        time.sleep(60)
        log(f"STATUS | price={current_price} | position={position_side} | qty={position_qty}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL ERROR: {e}")
        time.sleep(30)
