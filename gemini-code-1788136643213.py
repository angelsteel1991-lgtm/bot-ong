import os
import time
from binance.client import Client
from binance.exceptions import BinanceAPIException

# Cargar credenciales desde variables de entorno de Render
API_KEY = os.environ.get('BINANCE_API_KEY')
API_SECRET = os.environ.get('BINANCE_API_SECRET')

client = Client(API_KEY, API_SECRET)

SYMBOL = 'ONGUSDT'
QTY = 100  # Cantidad de ONG a operar por orden (Ajustar según tu saldo)
STOP_LOSS_PCT = 0.025      # 2.5% de pérdida máxima fija
TRAILING_DROP_PCT = 0.02   # Caída del 2% desde el máximo alcanzado para asegurar ganancia

in_position = False
buy_price = 0.0
highest_price = 0.0

print("Bot iniciado con Trailing Stop-Loss para ONG/USDT...")

while True:
    try:
        ticker = client.get_symbol_ticker(symbol=SYMBOL)
        current_price = float(ticker['price'])

        if not in_position:
            # Lógica de entrada (Compra de prueba / Estrategia)
            print(f"Precio actual de {SYMBOL}: {current_price}. Buscando punto de compra...")
            # Aquí se ejecuta la compra según tus condiciones
            # Simulamos o ejecutamos orden de compra market:
            # order = client.create_order(symbol=SYMBOL, side='BUY', type='MARKET', quantity=QTY)
            # buy_price = current_price
            # highest_price = current_price
            # in_position = True

        else:
            # Registrar el precio más alto alcanzado desde la compra
            if current_price > highest_price:
                highest_price = current_price
                print(f"Nuevo máximo alcanzado: {highest_price}")

            # 1. Trailing Stop-Loss: Venta por caída desde el pico máximo
            trailing_stop_price = highest_price * (1 - TRAILING_DROP_PCT)
            
            # 2. Stop-Loss Fijo: Venta si cae por debajo del costo inicial
            hard_stop_price = buy_price * (1 - STOP_LOSS_PCT)

            if current_price <= trailing_stop_price and current_price > buy_price:
                print(f"¡Ejecutando Trailing Stop-Loss! Ganancia asegurada. Precio: {current_price}")
                # client.create_order(symbol=SYMBOL, side='SELL', type='MARKET', quantity=QTY)
                in_position = False

            elif current_price <= hard_stop_price:
                print(f"¡Ejecutando Stop-Loss Fijo! Protección de capital. Precio: {current_price}")
                # client.create_order(symbol=SYMBOL, side='SELL', type='MARKET', quantity=QTY)
                in_position = False

    except BinanceAPIException as e:
        print(f"Error de Binance API: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")

    time.sleep(10)
