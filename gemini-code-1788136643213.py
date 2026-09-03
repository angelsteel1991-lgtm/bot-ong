import os
import sys
import time
import json
import uuid
import hmac
import hashlib
import requests
import threading
import websocket
import urllib.parse

# ============================================================
# CONFIGURACIÓN Y VARIABLES DE ENTORNO
# ============================================================
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
USE_TESTNET = os.getenv("USE_TESTNET", "true").lower() == "true"
SYMBOL = os.getenv("SYMBOL", "DOGEUSDT").upper()

# URLs según el entorno
if USE_TESTNET:
    REST_URL = "https://testnet.binancefuture.com"
    WS_BASE_URL = "wss://stream.binancefuture.com"
    WS_API_URL = "wss://testnet.binancefuture.com/ws-fapi/v1"
else:
    REST_URL = "https://fapi.binance.com"
    WS_BASE_URL = "wss://fstream.binance.com"
    WS_API_URL = "wss://ws-fapi.binance.com/ws-fapi/v1"

# Variables de estado global
latest_price = None
current_position = None
current_qty = 0.0
listen_key = None
user_stream_control_lock = threading.Lock()

def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}] {msg}", flush=True)

def log_public_ip():
    try:
        ip = requests.get("https://api.ipify.org", timeout=5).text
        log(f"IP PUBLICA DE SALIDA DEL BOT: {ip}")
    except Exception as e:
        log(f"No se pudo obtener IP pública: {e}")

# ============================================================
# FIRMA REST DE BINANCE
# ============================================================
def signed_request(method, endpoint, params=None):
    if params is None:
        params = {}
    
    params["timestamp"] = int(time.time() * 1000)
    query_string = urllib.parse.urlencode(params)
    signature = hmac.new(
        BINANCE_API_SECRET.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    
    url = f"{REST_URL}{endpoint}?{query_string}&signature={signature}"
    headers = {"X-MBX-APIKEY": BINANCE_API_KEY}
    
    response = requests.request(method, url, headers=headers, timeout=10)
    return response.json()

# ============================================================
# USER DATA STREAM (CORREGIDO VÍA REST HTTP)
# ============================================================
def start_user_data_stream():
    global listen_key
    log("Solicitando listenKey vía REST para User Data Stream...")
    
    data = signed_request("POST", "/fapi/v1/listenKey")
    key = data.get("listenKey")
    
    if not key:
        raise Exception(f"Error al obtener listenKey de Binance: {data}")
        
    with user_stream_control_lock:
        listen_key = key
        
    log("ListenKey recibido correctamente vía REST")
    return listen_key

def user_stream_keepalive_loop():
    global listen_key
    while True:
        time.sleep(30 * 60)  # Renueva el token cada 30 minutos
        try:
            if listen_key:
                signed_request("PUT", "/fapi/v1/listenKey")
                log("USER DATA STREAM KEEPALIVE OK (vía REST)")
        except Exception as e:
            log(f"User Data keepalive error: {e}")

def user_websocket_loop():
    while True:
        try:
            key = start_user_data_stream()
            ws_url = f"{WS_BASE_URL}/ws/{key}"
            log(f"Conectando a WS User Stream...")
            
            ws = websocket.create_connection(ws_url, timeout=15)
            log("Conexión User Data Stream WebSocket establecida.")
            
            while True:
                msg = ws.recv()
                if msg:
                    data = json.loads(msg)
                    event_type = data.get("e")
                    if event_type == "ACCOUNT_UPDATE":
                        log("Actualización de Cuenta/Posición recibida.")
                    elif event_type == "ORDER_TRADE_UPDATE":
                        log(f"Actualización de Orden: {data.get('o')}")
                        
        except Exception as e:
            log(f"User WS exception: {e}")
            log("Reconexión User Data en 60 segundos...")
            time.sleep(60)

# ============================================================
# MARKET DATA STREAM (PRECIO TICKER)
# ============================================================
def market_websocket_loop():
    global latest_price
    stream_name = f"{SYMBOL.lower()}@ticker"
    ws_url = f"{WS_BASE_URL}/ws/{stream_name}"
    
    while True:
        try:
            log(f"Conectando a WS Market Stream ({SYMBOL})...")
            ws = websocket.create_connection(ws_url, timeout=15)
            log("Conexión Market Data Stream establecida.")
            
            while True:
                msg = ws.recv()
                if msg:
                    data = json.loads(msg)
                    latest_price = float(data.get("c", 0.0))
        except Exception as e:
            log(f"Market WS exception: {e}")
            log("Reconexión Market Data en 10 segundos...")
            time.sleep(10)

# ============================================================
# LOGICA PRINCIPAL / IMPRESIÓN DE ESTADO
# ============================================================
def status_loop():
    while True:
        time.sleep(30)
        log(f"STATUS | price={latest_price} | position={current_position} | qty={current_qty}")

# ============================================================
# PUNTO DE ENTRADA
# ============================================================
if __name__ == "__main__":
    log(f"Iniciando Bot de Trading. Testnet={USE_TESTNET} | Símbolo={SYMBOL}")
    log_public_ip()
    
    # Hilos secundarios
    threading.Thread(target=market_websocket_loop, daemon=True).start()
    threading.Thread(target=user_websocket_loop, daemon=True).start()
    threading.Thread(target=user_stream_keepalive_loop, daemon=True).start()
    
    # Hilo principal de monitoreo
    status_loop()
