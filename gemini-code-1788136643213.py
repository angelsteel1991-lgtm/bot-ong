import os
import time
import ccxt
import numpy as np
import pandas as pd
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# Servidor básico para responder al puerto de Render
class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Bot activo")

def run_http_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), SimpleHTTPRequestHandler)
    server.serve_forever()

# Iniciar servidor en segundo plano
threading.Thread(target=run_http_server, daemon=True).start()

# Llaves API desde Render
API_KEY = os.environ.get('BINANCE_API_KEY')
SECRET_KEY = os.environ.get('BINANCE_SECRET_KEY')

SYMBOL = 'ONG/USDT'
MONTO_POR_ORDEN_USDT = 10
LEVERAGE = 2

# Conexión a Binance Futures
exchange = ccxt.binanceusdm({
    'apiKey': API_KEY,
    'secret': SECRET_KEY,
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

try:
    exchange.set_leverage(LEVERAGE, SYMBOL)
    print(f"✅ Apalancamiento configurado a {LEVERAGE}x")
except Exception as e:
    print(f"⚠️ Apalancamiento: {e}")

def obtener_datos():
    ohlcv = exchange.fetch_ohlcv(SYMBOL, timeframe='15m', limit=30)
    return pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

def ejecutar_orden(lado):
    try:
        ticker = exchange.fetch_ticker(SYMBOL)
        precio = ticker['last']
        cantidad = round(MONTO_POR_ORDEN_USDT / precio, 1)

        if lado == "BUY":
            print(f"🚀 EJECUTANDO LONG: {cantidad} ONG")
            return exchange.create_market_buy_order(SYMBOL, cantidad)
        elif lado == "SELL":
            print(f"⚠️ EJECUTANDO SHORT: {cantidad} ONG")
            return exchange.create_market_sell_order(SYMBOL, cantidad)
    except Exception as e:
        print(f"❌ Error al enviar orden: {e}")

def bot_24_7():
    print("🤖 Bot 24/7 iniciado en la nube para ONG...")
    posicion = "NINGUNA"

    while True:
        try:
            df = obtener_datos()
            precios = df['close'].values
            precio_actual = precios[-1]

            sma = np.mean(precios[-20:])
            std = np.std(precios[-20:])
            techo = sma + (std * 2.0)
            suelo = sma - (std * 2.0)

            movimiento = abs(precio_actual - precios[-2])
            vol_prom = np.mean(np.abs(np.diff(precios[-10:])))

            hora = time.strftime('%H:%M:%S')
            print(f"[{hora}] ONG: {precio_actual:.5f} | Suelo: {suelo:.5f} | Techo: {techo:.5f} | Posición: {posicion}")

            if precio_actual < suelo and movimiento > (vol_prom * 2):
                if posicion != "SHORT":
                    ejecutar_orden("SELL")
                    posicion = "SHORT"

            elif precio_actual > techo and movimiento > (vol_prom * 2):
                if posicion != "LONG":
                    ejecutar_orden("BUY")
                    posicion = "LONG"

            else:
                if posicion == "NINGUNA":
                    posicion = "GRID"

            time.sleep(30)
        except Exception as e:
            print(f"❌ Error temporal: {e}")
            time.sleep(15)

if __name__ == "__main__":
    bot_24_7()
