import os
import time
from binance.client import Client
from binance.exceptions import BinanceAPIException

API_KEY = os.environ.get('BINANCE_API_KEY')
API_SECRET = os.environ.get('BINANCE_API_SECRET')

# Conexión al servidor alternativo de Binance para evitar el bloqueo de IP
client = Client(API_KEY, API_SECRET)
client.API_URL = 'https://api1.binance.com/api'

SYMBOL = 'ONGUSDT'
QTY = 100               # Cantidad de ONG a operar por orden
STOP_LOSS_PCT = 0.025   # 2.5% de pérdida máxima fija
TRAILING_DROP_PCT = 0.02 # Caída del 2% desde el máximo para asegurar ganancia

in_position = False
buy_price = 0.0
highest_price = 0.0

print("Bot iniciado con servidor alternativo api1, protección de Rate Limit y Trailing Stop...")

while True:
    try:
        ticker = client.get_symbol_ticker(symbol=SYMBOL)
        current_price = float(ticker['price'])

        if not in_position:
            print(f"Precio actual de {SYMBOL}: {current_price}. Buscando punto de compra...")
        else:
            if current_price > highest_price:
                highest_price = current_price
                print(f"Nuevo máximo alcanzado: {highest_price}")

            trailing_stop_price = highest_price * (1 - TRAILING_DROP_PCT)
            hard_stop_price = buy_price * (1 - STOP_LOSS_PCT)

            if current_price <= trailing_stop_price and current_price > buy_price:
                print(f"¡Ejecutando Trailing Stop-Loss! Ganancia asegurada a {current_price}")
                in_position = False

            elif current_price <= hard_stop_price:
                print(f"¡Ejecutando Stop-Loss Fijo! Protección de capital a {current_price}")
                in_position = False

        # Pausa de 30 segundos entre consultas para no saturar la API
        time.sleep(30)

    except BinanceAPIException as e:
        if e.code == -1003:
            print("Límite de API alcanzado. Esperando 5 minutos para reintentar...")
            time.sleep(300)
        else:
            print(f"Error de Binance API: {e}")
            time.sleep(60)

    except Exception as e:
        print(f"Error inesperado: {e}")
        time.sleep(60)
