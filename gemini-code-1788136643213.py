# ============================================================
# ONGUSDT - BREAKOUT BOT
# Estrategia adaptada del bot de breakout de Binance Futures
# Conexión adaptada a la conexión funcional del bot ONG actual
# ============================================================

import os
import time
import json
import uuid
import hmac
import hashlib
import threading
from collections import deque
from urllib.parse import urlencode

import requests
import websocket


# ============================================================
# CONFIGURACION
# ============================================================

SYMBOL = "ONGUSDT"

LIVE_TRADING = True
USE_TESTNET = False

API_KEY = os.getenv("BINANCE_API_KEY", "")
API_SECRET = os.getenv("BINANCE_API_SECRET", "")

REST_URL = "https://fapi.binance.com"

# Conexion de mercado que ya funciona en nuestro bot
MARKET_WS_URL = (
    "wss://fstream.binance.com/market/ws/ongusdt@kline_1m"
)

# Conexion WS API que ya usamos
WS_API_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"

# Conexion User Data que ya usamos
PRIVATE_WS_BASE = "wss://fstream.binance.com/private/ws"

LEVERAGE = 6

LOOKBACK = 20
ATR_PERIOD = 14

# Filtro de ruptura
ATR_FILTER = 0.60

# Stop del sistema original:
# para ONG el ATR normalmente queda por debajo de 100,
# por lo tanto utiliza 3 ATR.
ATR_STOP_MULTIPLIER = 3.0

# Targets
TP1_R = 1.5
TP2_R = 2.0
TP3_R = 3.0

# Distribucion
TP1_PCT = 0.40
TP2_PCT = 0.30
TP3_PCT = 0.30

# Capital de riesgo por operación
RISK_USDT = 2.50

# Margen máximo aproximado
MAX_MARGIN_USDT = 4.0

# Tiempo entre reconexiones
RECONNECT_SECONDS = 60


# ============================================================
# ESTADO
# ============================================================

candles = deque(maxlen=200)

current_price = None

position = None

position_lock = threading.Lock()

symbol_info = {}

market_ws = None
user_ws = None

listen_key = None

stop_event = threading.Event()


# ============================================================
# LOG
# ============================================================

def log(msg):
    print(
        f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())} UTC] "
        f"{msg}",
        flush=True
    )


# ============================================================
# REST FIRMADO
# ============================================================

def sign_params(params):
    query = urlencode(params)

    signature = hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()

    return query + "&signature=" + signature


def signed_request(method, endpoint, params=None):
    if params is None:
        params = {}

    params["timestamp"] = int(time.time() * 1000)

    query = sign_params(params)

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    url = REST_URL + endpoint

    try:

        if method == "GET":
            response = requests.get(
                url + "?" + query,
                headers=headers,
                timeout=10
            )

        elif method == "POST":
            response = requests.post(
                url + "?" + query,
                headers=headers,
                timeout=10
            )

        elif method == "DELETE":
            response = requests.delete(
                url + "?" + query,
                headers=headers,
                timeout=10
            )

        else:
            raise ValueError("Metodo HTTP no soportado")

        if response.status_code in (418, 429):

            log(
                f"RATE LIMIT Binance HTTP {response.status_code}. "
                f"Esperando 60 segundos..."
            )

            time.sleep(60)

            return None

        if response.status_code >= 400:

            log(
                f"ERROR Binance {response.status_code}: "
                f"{response.text}"
            )

            return None

        return response.json()

    except Exception as e:

        log(f"ERROR REST: {e}")

        return None


# ============================================================
# REST PUBLICO
# ============================================================

def public_get(endpoint, params=None):

    try:

        response = requests.get(
            REST_URL + endpoint,
            params=params or {},
            timeout=10
        )

        if response.status_code in (418, 429):

            log(
                f"RATE LIMIT publico HTTP {response.status_code}"
            )

            time.sleep(60)

            return None

        if response.status_code >= 400:

            log(
                f"ERROR publico {response.status_code}: "
                f"{response.text}"
            )

            return None

        return response.json()

    except Exception as e:

        log(f"ERROR REST publico: {e}")

        return None


# ============================================================
# REGLAS DEL SIMBOLO
# ============================================================

def load_symbol_info():

    global symbol_info

    data = public_get(
        "/fapi/v1/exchangeInfo"
    )

    if not data:
        return False

    for s in data.get("symbols", []):

        if s["symbol"] == SYMBOL:

            symbol_info = s

            log(
                f"Symbol rules cargadas para {SYMBOL}"
            )

            return True

    log("No se encontraron reglas del simbolo")

    return False


def get_filter(filter_type):

    for f in symbol_info.get("filters", []):

        if f["filterType"] == filter_type:
            return f

    return {}


def floor_step(value, step):

    if step <= 0:
        return value

    decimals = 0

    step_str = f"{step:.16f}".rstrip("0")

    if "." in step_str:
        decimals = len(step_str.split(".")[1])

    result = int(value / step) * step

    return round(result, decimals)


def round_quantity(qty):

    lot = get_filter("LOT_SIZE")

    step = float(lot.get("stepSize", "0.001"))

    return floor_step(qty, step)


def round_price(price):

    pf = get_filter("PRICE_FILTER")

    tick = float(pf.get("tickSize", "0.0001"))

    return floor_step(price, tick)


# ============================================================
# LEVERAGE / MARGIN
# ============================================================

def configure_symbol():

    result = signed_request(
        "POST",
        "/fapi/v1/marginType",
        {
            "symbol": SYMBOL,
            "marginType": "ISOLATED"
        }
    )

    # Binance devuelve error si ya estaba aislado.
    # Eso no es un problema.

    result = signed_request(
        "POST",
        "/fapi/v1/leverage",
        {
            "symbol": SYMBOL,
            "leverage": LEVERAGE
        }
    )

    if result:

        log(
            f"Leverage configurado: {LEVERAGE}x"
        )

    return True


# ============================================================
# HISTORICO PARA ARRANCAR EL INDICADOR
# ============================================================

def load_initial_candles():

    data = public_get(
        "/fapi/v1/klines",
        {
            "symbol": SYMBOL,
            "interval": "1m",
            "limit": 200
        }
    )

    if not data:
        return False

    candles.clear()

    for k in data:

        candles.append(
            {
                "open_time": int(k[0]),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "closed": True
            }
        )

    if candles:

        global current_price

        current_price = candles[-1]["close"]

    log(
        f"Velas cargadas: {len(candles)}"
    )

    return True


# ============================================================
# ATR
# ============================================================

def calculate_atr(period=ATR_PERIOD):

    if len(candles) < period + 1:
        return None

    data = list(candles)

    true_ranges = []

    for i in range(1, len(data)):

        high = data[i]["high"]
        low = data[i]["low"]
        previous_close = data[i - 1]["close"]

        tr = max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close)
        )

        true_ranges.append(tr)

    if len(true_ranges) < period:
        return None

    return sum(true_ranges[-period:]) / period


# ============================================================
# BREAKOUT
# ============================================================

def get_breakout_signal():

    if len(candles) < LOOKBACK + ATR_PERIOD + 2:
        return None, None, None

    data = list(candles)

    last = data[-1]

    # Canal construido sobre las velas anteriores.
    channel = data[-LOOKBACK-1:-1]

    resistance = max(
        c["high"] for c in channel
    )

    support = min(
        c["low"] for c in channel
    )

    atr = calculate_atr()

    if atr is None or atr <= 0:
        return None, None, None

    channel_width = resistance - support

    # Filtro de volatilidad
    if channel_width < atr * ATR_FILTER:

        return None, atr, channel_width

    close = last["close"]

    if close > resistance:

        return "BUY", atr, channel_width

    if close < support:

        return "SELL", atr, channel_width

    return None, atr, channel_width


# ============================================================
# TAMAÑO DE POSICION
# ============================================================

def calculate_position_size(
    entry_price,
    stop_price
):

    risk_distance = abs(
        entry_price - stop_price
    )

    if risk_distance <= 0:
        return 0

    qty = RISK_USDT / risk_distance

    qty = round_quantity(qty)

    lot = get_filter("LOT_SIZE")

    min_qty = float(
        lot.get("minQty", "0")
    )

    if qty < min_qty:

        qty = min_qty

    # Control de margen
    notional = qty * entry_price

    margin = notional / LEVERAGE

    if margin > MAX_MARGIN_USDT:

        max_notional = MAX_MARGIN_USDT * LEVERAGE

        qty = max_notional / entry_price

        qty = round_quantity(qty)

    return qty


# ============================================================
# ORDEN DE MERCADO
# ============================================================

def market_order(side, quantity, reduce_only=False):

    quantity = round_quantity(quantity)

    if quantity <= 0:
        return None

    params = {
        "symbol": SYMBOL,
        "side": side,
        "type": "MARKET",
        "quantity": quantity
    }

    if reduce_only:
        params["reduceOnly"] = "true"

    result = signed_request(
        "POST",
        "/fapi/v1/order",
        params
    )

    return result


# ============================================================
# STOP MARKET
# ============================================================

def place_stop(quantity, side, stop_price):

    quantity = round_quantity(quantity)
    stop_price = round_price(stop_price)

    if quantity <= 0:
        return None

    params = {
        "symbol": SYMBOL,
        "side": side,
        "type": "STOP_MARKET",
        "quantity": quantity,
        "stopPrice": stop_price,
        "reduceOnly": "true",
        "workingType": "CONTRACT_PRICE"
    }

    result = signed_request(
        "POST",
        "/fapi/v1/order",
        params
    )

    if result:

        log(
            f"STOP colocado | side={side} "
            f"qty={quantity} stop={stop_price}"
        )

    return result


# ============================================================
# CANCELAR ORDENES ABIERTAS
# ============================================================

def cancel_all_open_orders():

    result = signed_request(
        "DELETE",
        "/fapi/v1/allOpenOrders",
        {
            "symbol": SYMBOL
        }
    )

    return result


# ============================================================
# POSICION ACTUAL
# ============================================================

def get_position():

    data = signed_request(
        "GET",
        "/fapi/v2/positionRisk",
        {
            "symbol": SYMBOL
        }
    )

    if not data:
        return None

    for p in data:

        if p["symbol"] != SYMBOL:
            continue

        qty = float(p["positionAmt"])

        if abs(qty) <= 0:
            return None

        entry = float(p["entryPrice"])

        if qty > 0:
            side = "LONG"
        else:
            side = "SHORT"

        return {
            "side": side,
            "qty": abs(qty),
            "entry": entry
        }

    return None


# ============================================================
# ENTRADA
# ============================================================

def enter_trade(signal, atr):

    global position

    if position is not None:
        return

    price = current_price

    if price is None or atr is None:
        return

    # Para ONG el ATR estará normalmente muy por debajo
    # de 100, por lo que el multiplicador será 3.
    if atr > 100:
        k = 4.0
    else:
        k = 3.0

    if signal == "BUY":

        stop_price = price - (k * atr)

        order_side = "BUY"
        stop_side = "SELL"

    else:

        stop_price = price + (k * atr)

        order_side = "SELL"
        stop_side = "BUY"

    risk_distance = abs(
        price - stop_price
    )

    tp1 = (
        price + risk_distance * TP1_R
        if signal == "BUY"
        else price - risk_distance * TP1_R
    )

    tp2 = (
        price + risk_distance * TP2_R
        if signal == "BUY"
        else price - risk_distance * TP2_R
    )

    tp3 = (
        price + risk_distance * TP3_R
        if signal == "BUY"
        else price - risk_distance * TP3_R
    )

    qty = calculate_position_size(
        price,
        stop_price
    )

    if qty <= 0:

        log("Cantidad calculada invalida")

        return

    log(
        f"BREAKOUT {signal} | "
        f"entry={price:.8f} | "
        f"ATR={atr:.8f} | "
        f"SL={stop_price:.8f} | "
        f"R={risk_distance:.8f}"
    )

    if not LIVE_TRADING:

        log(
            f"TEST | qty={qty} "
            f"TP1={tp1} TP2={tp2} TP3={tp3}"
        )

        return

    order = market_order(
        order_side,
        qty
    )

    if not order:

        log("ERROR: no se pudo abrir posicion")

        return

    # Intentamos obtener precio real de ejecucion
    executed_qty = float(
        order.get("executedQty", qty)
    )

    avg_price = float(
        order.get("avgPrice", 0)
    )

    if avg_price <= 0:

        avg_price = price

    risk_distance = abs(
        avg_price - stop_price
    )

    if signal == "BUY":

        stop_price = avg_price - k * atr

        tp1 = avg_price + risk_distance * TP1_R
        tp2 = avg_price + risk_distance * TP2_R
        tp3 = avg_price + risk_distance * TP3_R

    else:

        stop_price = avg_price + k * atr

        tp1 = avg_price - risk_distance * TP1_R
        tp2 = avg_price - risk_distance * TP2_R
        tp3 = avg_price - risk_distance * TP3_R

    position = {
        "side": signal,
        "qty": executed_qty,
        "entry": avg_price,
        "atr": atr,
        "risk": risk_distance,
        "stop": stop_price,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "tp1_done": False,
        "tp2_done": False,
        "tp3_done": False,
        "remaining": executed_qty,
        "stop_order": None
    }

    log(
        f"POSICION ABIERTA | "
        f"{signal} | qty={executed_qty} | "
        f"entry={avg_price:.8f}"
    )

    stop_order = place_stop(
        executed_qty,
        stop_side,
        stop_price
    )

    if stop_order:

        position["stop_order"] = stop_order.get(
            "orderId"
        )


# ============================================================
# CERRAR PARTE
# ============================================================

def close_partial(percent):

    global position

    if not position:
        return

    remaining = position["remaining"]

    qty = round_quantity(
        remaining * percent
    )

    if qty <= 0:
        return

    if position["side"] == "LONG":
        side = "SELL"
    else:
        side = "BUY"

    order = market_order(
        side,
        qty,
        reduce_only=True
    )

    if not order:
        return

    executed = float(
        order.get("executedQty", qty)
    )

    position["remaining"] = max(
        0,
        position["remaining"] - executed
    )

    log(
        f"CIERRE PARCIAL | "
        f"{percent * 100:.0f}% | "
        f"qty={executed}"
    )


# ============================================================
# MOVER STOP
# ============================================================

def move_stop(new_stop):

    global position

    if not position:
        return

    # Primero eliminamos el stop anterior
    cancel_all_open_orders()

    position["stop"] = new_stop

    if position["side"] == "LONG":
        stop_side = "SELL"
    else:
        stop_side = "BUY"

    if position["remaining"] <= 0:
        return

    result = place_stop(
        position["remaining"],
        stop_side,
        new_stop
    )

    if result:

        position["stop_order"] = result.get(
            "orderId"
        )

        log(
            f"STOP ACTUALIZADO -> "
            f"{new_stop:.8f}"
        )


# ============================================================
# CERRAR TODO
# ============================================================

def close_position():

    global position

    if not position:
        return

    cancel_all_open_orders()

    qty = round_quantity(
        position["remaining"]
    )

    if qty <= 0:

        position = None

        return

    if position["side"] == "LONG":
        side = "SELL"
    else:
        side = "BUY"

    result = market_order(
        side,
        qty,
        reduce_only=True
    )

    if result:

        log(
            f"POSICION CERRADA | qty={qty}"
        )

    position = None


# ============================================================
# GESTION DE TP
# ============================================================

def manage_position(price):

    global position

    if not position:
        return

    side = position["side"]

    entry = position["entry"]
    risk = position["risk"]

    tp1 = position["tp1"]
    tp2 = position["tp2"]
    tp3 = position["tp3"]

    # --------------------------------------------------------
    # LONG
    # --------------------------------------------------------

    if side == "LONG":

        if not position["tp1_done"] and price >= tp1:

            close_partial(TP1_PCT)

            if position:

                position["tp1_done"] = True

                # BE
                move_stop(entry)

                log(
                    "TP1 alcanzado -> STOP a BREAK EVEN"
                )

        if position and not position["tp2_done"]:

            if price >= tp2:

                close_partial(TP2_PCT)

                if position:

                    position["tp2_done"] = True

                    # STOP al TP1
                    move_stop(tp1)

                    log(
                        "TP2 alcanzado -> "
                        "STOP protegido en TP1"
                    )

        if position and not position["tp3_done"]:

            if price >= tp3:

                position["tp3_done"] = True

                close_position()

                log(
                    "TP3 alcanzado -> "
                    "OPERACION COMPLETADA"
                )

    # --------------------------------------------------------
    # SHORT
    # --------------------------------------------------------

    else:

        if not position["tp1_done"] and price <= tp1:

            close_partial(TP1_PCT)

            if position:

                position["tp1_done"] = True

                move_stop(entry)

                log(
                    "TP1 alcanzado -> STOP a BREAK EVEN"
                )

        if position and not position["tp2_done"]:

            if price <= tp2:

                close_partial(TP2_PCT)

                if position:

                    position["tp2_done"] = True

                    move_stop(tp1)

                    log(
                        "TP2 alcanzado -> "
                        "STOP protegido en TP1"
                    )

        if position and not position["tp3_done"]:

            if price <= tp3:

                position["tp3_done"] = True

                close_position()

                log(
                    "TP3 alcanzado -> "
                    "OPERACION COMPLETADA"
                )


# ============================================================
# PROCESAR VELA CERRADA
# ============================================================

def process_closed_candle(candle):

    candles.append(candle)

    signal, atr, width = get_breakout_signal()

    if signal:

        log(
            f"SEÑAL BREAKOUT {signal} | "
            f"close={candle['close']:.8f} | "
            f"ATR={atr:.8f} | "
            f"channel={width:.8f}"
        )

    if position is None and signal:

        enter_trade(
            signal,
            atr
        )


# ============================================================
# WEBSOCKET DE MERCADO
# ============================================================

def market_on_message(ws, message):

    global current_price

    try:

        data = json.loads(message)

        k = data.get("k")

        if not k:
            return

        current_price = float(
            k["c"]
        )

        # Gestionamos la posicion en tiempo real
        if position is not None:

            manage_position(
                current_price
            )

        # Solo generamos señales cuando cierra
        # la vela de 1 minuto
        if k["x"]:

            candle = {
                "open_time": int(k["t"]),
                "open": float(k["o"]),
                "high": float(k["h"]),
                "low": float(k["l"]),
                "close": float(k["c"]),
                "volume": float(k["v"]),
                "closed": True
            }

            process_closed_candle(
                candle
            )

    except Exception as e:

        log(
            f"ERROR procesando MARKET WS: {e}"
        )


def market_on_error(ws, error):

    log(
        f"MARKET WS ERROR: {error}"
    )


def market_on_close(ws, code, msg):

    log(
        f"MARKET WS CERRADO: {code} {msg}"
    )


def market_on_open(ws):

    log(
        "MARKET WEBSOCKET CONECTADO"
    )


def market_loop():

    global market_ws

    while not stop_event.is_set():

        try:

            log(
                "Conectando MARKET WebSocket..."
            )

            market_ws = websocket.WebSocketApp(
                MARKET_WS_URL,
                on_open=market_on_open,
                on_message=market_on_message,
                on_error=market_on_error,
                on_close=market_on_close
            )

            market_ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as e:

            log(
                f"ERROR MARKET LOOP: {e}"
            )

        if not stop_event.is_set():

            log(
                f"Reconectando MARKET en "
                f"{RECONNECT_SECONDS} segundos..."
            )

            time.sleep(
                RECONNECT_SECONDS
            )


# ============================================================
# USER DATA STREAM
# ============================================================

def start_user_stream():

    global listen_key

    try:

        ws = websocket.create_connection(
            WS_API_URL,
            timeout=15
        )

        request = {
            "id": str(uuid.uuid4()),
            "method": "userDataStream.start",
            "params": {
                "apiKey": API_KEY
            }
        }

        ws.send(
            json.dumps(request)
        )

        response = json.loads(
            ws.recv()
        )

        ws.close()

        result = response.get(
            "result",
            {}
        )

        listen_key = result.get(
            "listenKey"
        )

        if listen_key:

            log(
                "USER DATA STREAM iniciado"
            )

            return listen_key

        log(
            f"ERROR USER DATA: {response}"
        )

    except Exception as e:

        log(
            f"ERROR iniciando USER DATA: {e}"
        )

    return None


def keepalive_user_stream():

    while not stop_event.is_set():

        time.sleep(
            45 * 60
        )

        if stop_event.is_set():
            break

        try:

            ws = websocket.create_connection(
                WS_API_URL,
                timeout=15
            )

            request = {
                "id": str(uuid.uuid4()),
                "method": "userDataStream.ping",
                "params": {
                    "apiKey": API_KEY,
                    "listenKey": listen_key
                }
            }

            ws.send(
                json.dumps(request)
            )

            ws.recv()

            ws.close()

            log(
                "USER DATA keepalive OK"
            )

        except Exception as e:

            log(
                f"ERROR keepalive USER DATA: {e}"
            )


def user_on_message(ws, message):

    global position

    try:

        data = json.loads(message)

        event = data.get("e")

        if event == "ORDER_TRADE_UPDATE":

            order = data.get(
                "o",
                {}
            )

            order_type = order.get(
                "o"
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

            log(
                f"ORDER UPDATE | "
                f"type={order_type} "
                f"side={side} "
                f"status={status} "
                f"executed={executed}"
            )

        elif event == "ACCOUNT_UPDATE":

            log(
                "ACCOUNT UPDATE recibido"
            )

    except Exception as e:

        log(
            f"ERROR USER DATA: {e}"
        )


def user_loop():

    global user_ws
    global listen_key

    while not stop_event.is_set():

        try:

            listen_key = start_user_stream()

            if not listen_key:

                time.sleep(
                    RECONNECT_SECONDS
                )

                continue

            url = (
                PRIVATE_WS_BASE
                + "?listenKey="
                + listen_key
                + "&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE"
            )

            log(
                "Conectando USER DATA..."
            )

            user_ws = websocket.WebSocketApp(
                url,
                on_message=user_on_message,
                on_error=lambda ws, err:
                    log(
                        f"USER DATA ERROR: {err}"
                    ),
                on_close=lambda ws, code, msg:
                    log(
                        f"USER DATA CLOSED: {code} {msg}"
                    )
            )

            user_ws.run_forever(
                ping_interval=20,
                ping_timeout=10
            )

        except Exception as e:

            log(
                f"ERROR USER DATA LOOP: {e}"
            )

        if not stop_event.is_set():

            log(
                f"Reconexión User Data en "
                f"{RECONNECT_SECONDS} segundos..."
            )

            time.sleep(
                RECONNECT_SECONDS
            )


# ============================================================
# SINCRONIZAR POSICION AL ARRANCAR
# ============================================================

def sync_position():

    global position

    p = get_position()

    if p:

        log(
            f"POSICION EXISTENTE | "
            f"{p['side']} "
            f"qty={p['qty']} "
            f"entry={p['entry']}"
        )

        position = {
            "side": p["side"],
            "qty": p["qty"],
            "entry": p["entry"],
            "remaining": p["qty"],
            "risk": 0,
            "stop": 0,
            "tp1": 0,
            "tp2": 0,
            "tp3": 0,
            "tp1_done": False,
            "tp2_done": False,
            "tp3_done": False,
            "atr": 0,
            "stop_order": None
        }

    else:

        position = None

        log(
            "Sin posicion abierta"
        )


# ============================================================
# STATUS
# ============================================================

def status_loop():

    while not stop_event.is_set():

        try:

            p = position

            if p:

                log(
                    f"STATUS | "
                    f"price={current_price} | "
                    f"position={p['side']} | "
                    f"qty={p['remaining']} | "
                    f"entry={p['entry']}"
                )

            else:

                log(
                    f"STATUS | "
                    f"price={current_price} | "
                    f"position=None | qty=0"
                )

        except Exception as e:

            log(
                f"STATUS ERROR: {e}"
            )

        time.sleep(60)


# ============================================================
# MAIN
# ============================================================

def main():

    log(
        "=================================================="
    )

    log(
        "ONGUSDT BREAKOUT BOT INICIANDO"
    )

    log(
        f"LIVE_TRADING={LIVE_TRADING}"
    )

    log(
        f"LEVERAGE={LEVERAGE}x"
    )

    log(
        f"LOOKBACK={LOOKBACK}"
    )

    log(
        f"ATR_PERIOD={ATR_PERIOD}"
    )

    log(
        "=================================================="
    )

    if not API_KEY or not API_SECRET:

        log(
            "ERROR: faltan BINANCE_API_KEY "
            "o BINANCE_API_SECRET"
        )

        return

    # --------------------------------------------------------
    # Cargar reglas
    # --------------------------------------------------------

    if not load_symbol_info():

        log(
            "No se pudieron cargar reglas."
        )

        return

    # --------------------------------------------------------
    # Configurar cuenta
    # --------------------------------------------------------

    if LIVE_TRADING:

        configure_symbol()

    # --------------------------------------------------------
    # Cargar histórico
    # --------------------------------------------------------

    if not load_initial_candles():

        log(
            "No se pudo cargar histórico."
        )

        return

    # --------------------------------------------------------
    # Sincronizar posicion
    # --------------------------------------------------------

    if LIVE_TRADING:

        sync_position()

    # --------------------------------------------------------
    # USER DATA
    # --------------------------------------------------------

    user_thread = threading.Thread(
        target=user_loop,
        daemon=True
    )

    user_thread.start()

    keepalive_thread = threading.Thread(
        target=keepalive_user_stream,
        daemon=True
    )

    keepalive_thread.start()

    # --------------------------------------------------------
    # MARKET WS
    # --------------------------------------------------------

    market_thread = threading.Thread(
        target=market_loop,
        daemon=True
    )

    market_thread.start()

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    status_thread = threading.Thread(
        target=status_loop,
        daemon=True
    )

    status_thread.start()

    # --------------------------------------------------------
    # MAIN
    # --------------------------------------------------------

    try:

        while True:

            time.sleep(1)

    except KeyboardInterrupt:

        log(
            "Deteniendo bot..."
        )

        stop_event.set()

        try:

            if market_ws:
                market_ws.close()

        except Exception:
            pass

        try:

            if user_ws:
                user_ws.close()

        except Exception:
            pass


if __name__ == "__main__":
    main()
