import os
import time
import json
import websocket

# Configuración del par
SYMBOL = 'ongusdt'  # WebSockets de Binance requiere el símbolo en minúsculas

# Variables de control
in_position = False
buy_price = 0.0
highest_price = 0.0

STOP_LOSS_PCT = 0.025    # 2.5% pérdida máxima
TRAILING_DROP_PCT = 0.02  # 2.0% caída desde el máximo

def on_message(ws, message):
    global in_position, buy_price, highest_price

    data = json.loads(message)
    current_price = float(data['c'])  # 'c' es el precio de cierre actual en el stream

    if not in_position:
        print(f"Precio actual de {SYMBOL.upper()}: {current_price}. Esperando señal...")
    else:
        if current_price > highest_price:
            highest_price = current_price
            print(f"Nuevo máximo: {highest_price}")

        trailing_stop_price = highest_price * (1 - TRAILING_DROP_PCT)
        hard_stop_price = buy_price * (1 - STOP_LOSS_PCT)

        if current_price <= trailing_stop_price and current_price > buy_price:
            print(f"¡Ejecutando Trailing Stop-Loss a {current_price}!")
            in_position = False

        elif current_price <= hard_stop_price:
            print(f"¡Ejecutando Stop-Loss Fijo a {current_price}!")
            in_position = False

def on_error(ws, error):
    print(f"Error en WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Conexión cerrada. Reconectando en 5 segundos...")
    time.sleep(5)
    start_websocket()

def start_websocket():
    # Stream público de ticker individual de Binance (No consume límites de API HTTP)
    socket_url = f"wss://stream.binance.com:9443/ws/{SYMBOL}@ticker"
    
    ws = websocket.WebSocketApp(
        socket_url,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    print("Iniciando Bot con WebSocket Stream (sin consumo de API REST)...")
    start_websocket()
