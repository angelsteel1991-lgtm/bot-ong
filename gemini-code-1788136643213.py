import os
import time
import math
import json
import uuid
import hmac
import hashlib
import threading
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import requests
import pandas as pd
import websocket

from datetime import datetime, timezone


# ============================================================
# UNIVERSAL BINANCE FUTURES BOT V4 TEST
#
# BASE:
# - MISMA CONEXION DEL V3 DEL USUARIO
# - MARKET WEBSOCKET
# - WS API BINANCE
# - USER DATA STREAM
# - REST SOLO PARA METADATA / KLINES / LEVERAGE
#
# ESTRATEGIA:
# - FReinforcedStrategy
# - EMA SHORT / EMA LONG
# - SMA 50 EN TIMEFRAME SUPERIOR
# - LONG + SHORT
#
# TEST:
# - NO ENVIA ORDENES REALES
# - SIMULA BALANCE
# - SIMULA COMISION
# - SIMULA SLIPPAGE
# - RESPETA MIN QTY / STEP / MIN NOTIONAL
#
# APALANCAMIENTO:
# - x6
#
# IMPORTANTE:
# LIVE_TRADING DEBE QUEDAR EN FALSE
# HASTA TERMINAR LAS PRUEBAS.
# ============================================================


# ============================================================
# CONFIGURACION
# ============================================================

LIVE_TRADING = False

USE_TESTNET = False

INTERVAL = "5m"

LEVERAGE = 6

# ------------------------------------------------------------
# CAPITAL DE TEST
#
# CAMBIALO AL CAPITAL QUE QUIERAS SIMULAR.
#
# EJEMPLO:
# 10 USDT = aproximadamente una cuenta muy pequeña.
# ------------------------------------------------------------

START_BALANCE = 10.0

# ------------------------------------------------------------
# RIESGO
#
# 1% del balance por operacion.
# ------------------------------------------------------------

RISK_PER_TRADE = 0.01

# ------------------------------------------------------------
# MARGEN MAXIMO
# ------------------------------------------------------------

MAX_MARGIN_USAGE = 0.80

# ------------------------------------------------------------
# POSICIONES
#
# Para capital pequeño:
# una sola posicion.
# ------------------------------------------------------------

MAX_POSITIONS = 1

# ------------------------------------------------------------
# STOP
#
# La estrategia FReinforced NO trae un ATR stop.
# Para este bot usamos ATR para proteccion de riesgo.
# ------------------------------------------------------------

ATR_PERIOD = 14

ATR_STOP_MULT = 1.70

# ------------------------------------------------------------
# CANTIDAD DE VELAS
#
# FReinforced usa timeframe superior:
# 5m x 12 = 60m
#
# SMA 50 de 60m necesita aproximadamente
# 600 velas de 5m.
# ------------------------------------------------------------

MIN_CANDLES = 650

INITIAL_CANDLES = 750

# ------------------------------------------------------------
# RECONEXION
# ------------------------------------------------------------

RECONNECT_SECONDS = 10

REST_TIMEOUT = 15


# ============================================================
# SIMULACION
# ============================================================

# ------------------------------------------------------------
# COMISION ESTIMADA
#
# Se aplica tanto entrada como salida.
#
# Esto es una aproximacion de TEST.
# ------------------------------------------------------------

TAKER_FEE = 0.0005

# ------------------------------------------------------------
# SLIPPAGE
#
# 0.05% por ejecucion.
# ------------------------------------------------------------

SLIPPAGE = 0.0005


# ============================================================
# PROTECCION DE GANANCIA
# ============================================================

PROTECTION_LEVELS = [
    (1.0, 0.15),
    (1.5, 0.35),
    (2.0, 0.70),
    (2.5, 1.15),
    (3.0, 1.75),
    (4.0, 2.50),
    (5.0, 3.25),
    (6.0, 4.25),
    (8.0, 5.50),
    (10.0, 7.00),
]


TRAILING_LEVELS = [
    (4.0, 2.0),
    (6.0, 2.5),
    (8.0, 3.0),
    (10.0, 3.5),
]


TIME_STOP_CANDLES = 36

TIME_STOP_MIN_MFE_R = 0.50


# ============================================================
# API
# ============================================================

API_KEY = os.getenv(
    "BINANCE_API_KEY"
)

API_SECRET = os.getenv(
    "BINANCE_API_SECRET"
)


if USE_TESTNET:

    REST_BASE = (
        "https://testnet.binancefuture.com"
    )

    MARKET_WS_BASE = (
        "wss://stream.binancefuture.com/market/ws/"
    )

    WS_API_URL = (
        "wss://testnet.binancefuture.com/"
        "ws-fapi/v1"
    )

else:

    REST_BASE = (
        "https://fapi.binance.com"
    )

    MARKET_WS_BASE = (
        "wss://fstream.binance.com/market/ws/"
    )

    WS_API_URL = (
        "wss://ws-fapi.binance.com/"
        "ws-fapi/v1"
    )


# ============================================================
# SIMBOLOS
# ============================================================

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "SUIUSDT",
    "LTCUSDT",
    "BCHUSDT",
    "ETCUSDT",
    "FILUSDT",
    "APTUSDT",
    "ARBUSDT",
    "OPUSDT",
    "INJUSDT",
    "NEARUSDT",
    "ATOMUSDT",
    "DOTUSDT",
    "TRXUSDT",
    "UNIUSDT",
    "AAVEUSDT",
    "HBARUSDT",
    "TIAUSDT",
    "SEIUSDT",
    "WIFUSDT",
    "PEPEUSDT",
    "TONUSDT",
]


# ============================================================
# ESTADO GLOBAL
# ============================================================

market_data = {}

symbol_rules = {}

positions = {}

trade_history = []

paper_balance = START_BALANCE

paper_initial_balance = START_BALANCE

state_lock = threading.RLock()

order_lock = threading.Lock()

ws_request_lock = threading.Lock()

user_stream_control_lock = threading.RLock()

user_stream_control = None

listen_key = None


# ============================================================
# ESTADO DE SIMBOLOS
# ============================================================

def create_symbol_state():

    return {
        "candles": [],
        "price": None,
        "atr": None,
        "last_signal": None,
        "last_candle_time": None,
    }


for symbol in SYMBOLS:

    market_data[symbol] = (
        create_symbol_state()
    )


# ============================================================
# LOG
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


# ============================================================
# REST SIGNATURE
# ============================================================

def rest_signature(params):

    query = urlencode(
        params,
        doseq=True
    )

    return hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()


# ============================================================
# REST REQUEST
# ============================================================

def rest_request(
    method,
    path,
    params=None,
    signed=False
):

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan BINANCE_API_KEY "
            "o BINANCE_API_SECRET"
        )

    params = dict(
        params or {}
    )

    if signed:

        params["timestamp"] = int(
            time.time() * 1000
        )

        params["recvWindow"] = 5000

        params["signature"] = (
            rest_signature(params)
        )

    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    url = REST_BASE + path

    if method == "GET":

        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=REST_TIMEOUT
        )

    elif method == "POST":

        response = requests.post(
            url,
            params=params,
            headers=headers,
            timeout=REST_TIMEOUT
        )

    elif method == "DELETE":

        response = requests.delete(
            url,
            params=params,
            headers=headers,
            timeout=REST_TIMEOUT
        )

    else:

        raise Exception(
            f"Metodo REST no soportado: "
            f"{method}"
        )

    if response.status_code >= 400:

        raise Exception(
            f"REST {method} {path} "
            f"{response.status_code}: "
            f"{response.text}"
        )

    return response.json()


# ============================================================
# REGLAS BINANCE
# ============================================================

def load_symbol_rules():

    global symbol_rules

    log(
        "Cargando reglas reales de Binance..."
    )

    data = rest_request(
        "GET",
        "/fapi/v1/exchangeInfo"
    )

    rules = {}

    for item in data.get(
        "symbols",
        []
    ):

        symbol = item.get(
            "symbol"
        )

        if symbol not in SYMBOLS:

            continue

        if item.get(
            "status"
        ) != "TRADING":

            log(
                f"{symbol} | "
                f"NO TRADING - omitido"
            )

            continue

        filters = {
            x.get("filterType"): x
            for x in item.get(
                "filters",
                []
            )
        }

        lot = filters.get(
            "LOT_SIZE",
            {}
        )

        market_lot = filters.get(
            "MARKET_LOT_SIZE",
            {}
        )

        price_filter = filters.get(
            "PRICE_FILTER",
            {}
        )

        min_notional_filter = (
            filters.get(
                "MIN_NOTIONAL",
                filters.get(
                    "NOTIONAL",
                    {}
                )
            )
        )

        step = float(
            market_lot.get(
                "stepSize",
                lot.get(
                    "stepSize",
                    0
                )
            )
        )

        min_qty = float(
            market_lot.get(
                "minQty",
                lot.get(
                    "minQty",
                    0
                )
            )
        )

        max_qty = float(
            market_lot.get(
                "maxQty",
                lot.get(
                    "maxQty",
                    0
                )
            )
        )

        min_notional = float(
            min_notional_filter.get(
                "notional",
                min_notional_filter.get(
                    "minNotional",
                    0
                )
            )
        )

        tick_size = float(
            price_filter.get(
                "tickSize",
                0
            )
        )

        if step <= 0:

            log(
                f"{symbol} | "
                f"ERROR stepSize invalido"
            )

            continue

        rules[symbol] = {
            "step": step,
            "min_qty": min_qty,
            "max_qty": max_qty,
            "min_notional": min_notional,
            "tick_size": tick_size,
        }

        log(
            f"RULES | {symbol} | "
            f"step={step} | "
            f"minQty={min_qty} | "
            f"maxQty={max_qty} | "
            f"minNotional={min_notional} | "
            f"tickSize={tick_size}"
        )

    symbol_rules = rules

    log(
        f"REGLAS CARGADAS: "
        f"{len(symbol_rules)}/{len(SYMBOLS)}"
    )


# ============================================================
# REDONDEO CANTIDAD
# ============================================================

def floor_to_step(
    value,
    step
):

    if step <= 0:

        return 0.0

    value_dec = Decimal(
        str(value)
    )

    step_dec = Decimal(
        str(step)
    )

    units = (
        value_dec / step_dec
    ).to_integral_value(
        rounding=ROUND_DOWN
    )

    return float(
        units * step_dec
    )


# ============================================================
# REDONDEO PRECIO
# ============================================================

def floor_price(
    price,
    tick_size
):

    if tick_size <= 0:

        return float(price)

    value_dec = Decimal(
        str(price)
    )

    tick_dec = Decimal(
        str(tick_size)
    )

    units = (
        value_dec / tick_dec
    ).to_integral_value(
        rounding=ROUND_DOWN
    )

    return float(
        units * tick_dec
    )


# ============================================================
# NORMALIZAR CANTIDAD
# ============================================================

def normalize_quantity(
    symbol,
    quantity,
    price
):

    rule = symbol_rules.get(
        symbol
    )

    if not rule:

        return 0.0

    step = rule["step"]

    min_qty = rule["min_qty"]

    max_qty = rule["max_qty"]

    min_notional = rule[
        "min_notional"
    ]

    quantity = floor_to_step(
        quantity,
        step
    )

    if quantity <= 0:

        return 0.0

    if max_qty > 0:

        quantity = min(
            quantity,
            max_qty
        )

        quantity = floor_to_step(
            quantity,
            step
        )

    if quantity < min_qty:

        return 0.0

    if (
        min_notional > 0
        and quantity * price
        < min_notional
    ):

        return 0.0

    return quantity


# ============================================================
# LEVERAGE
# ============================================================

def set_symbol_leverage(
    symbol
):

    if not LIVE_TRADING:

        log(
            f"{symbol} | "
            f"TEST | leverage simulado = "
            f"x{LEVERAGE}"
        )

        return True

    try:

        result = rest_request(
            "POST",
            "/fapi/v1/leverage",
            {
                "symbol": symbol,
                "leverage": LEVERAGE,
            },
            signed=True
        )

        log(
            f"{symbol} | "
            f"LEVERAGE = "
            f"{result.get('leverage')}"
        )

        return True

    except Exception as e:

        log(
            f"{symbol} | "
            f"ERROR LEVERAGE: {e}"
        )

        return False


def configure_all_leverage():

    log(
        f"Configurando leverage x{LEVERAGE}..."
    )

    for symbol in SYMBOLS:

        if symbol not in symbol_rules:

            continue

        set_symbol_leverage(
            symbol
        )

        time.sleep(
            0.08
        )


# ============================================================
# ATR
# ============================================================

def calculate_atr(
    df,
    period=ATR_PERIOD
):

    high = df["high"]

    low = df["low"]

    close = df["close"]

    previous_close = (
        close.shift(1)
    )

    tr = pd.concat(
        [
            high - low,
            (
                high - previous_close
            ).abs(),
            (
                low - previous_close
            ).abs(),
        ],
        axis=1
    ).max(
        axis=1
    )

    atr = tr.rolling(
        period
    ).mean()

    value = atr.iloc[-1]

    if pd.isna(value):

        return None

    return float(value)


# ============================================================
# INDICADORES FReinforced
#
# Basado en la estrategia publicada por
# freqtrade/freqtrade-strategies.
#
# Timeframe base: 5m
# Timeframe superior: 60m
#
# EMA SHORT = 8
# EMA LONG  = 21
# SMA       = 50 en 60m
# ============================================================

def calculate_freinforced_indicators(
    df
):

    df = df.copy()

    # --------------------------------------------------------
    # EMA 8
    # --------------------------------------------------------

    df["ema_short"] = (
        df["close"]
        .ewm(
            span=8,
            adjust=False
        )
        .mean()
    )

    # --------------------------------------------------------
    # EMA 21
    # --------------------------------------------------------

    df["ema_long"] = (
        df["close"]
        .ewm(
            span=21,
            adjust=False
        )
        .mean()
    )

    # --------------------------------------------------------
    # TIMEFRAME SUPERIOR
    #
    # 12 velas de 5m = 1 hora.
    # Usamos el cierre de cada bloque horario.
    # --------------------------------------------------------

    temp = df[
        [
            "timestamp",
            "close"
        ]
    ].copy()

    temp["datetime"] = pd.to_datetime(
        temp["timestamp"],
        unit="ms",
        utc=True
    )

    temp = temp.set_index(
        "datetime"
    )

    hourly = (
        temp["close"]
        .resample("1h")
        .last()
        .dropna()
    )

    # --------------------------------------------------------
    # SMA 50 DEL 1H
    # --------------------------------------------------------

    hourly_sma = (
        hourly
        .rolling(50)
        .mean()
    )

    # --------------------------------------------------------
    # VOLVER A 5M
    #
    # El valor de SMA se aplica a las velas de ese periodo.
    # --------------------------------------------------------

    sma_df = (
        hourly_sma
        .rename("htf_sma")
        .reset_index()
    )

    sma_df["datetime"] = (
        sma_df["datetime"]
        + pd.Timedelta(hours=1)
    )

    sma_df = sma_df.set_index(
        "datetime"
    )

    df = df.set_index(
        "datetime"
    )

    df = df.join(
        sma_df[
            ["htf_sma"]
        ],
        how="left"
    )

    df["htf_sma"] = (
        df["htf_sma"]
        .ffill()
    )

    df = df.reset_index()

    # --------------------------------------------------------
    # ATR PARA GESTION DE RIESGO
    # --------------------------------------------------------

    df["atr"] = calculate_atr(
        df
    )

    return df


# ============================================================
# CRUCE ALCISTA
# ============================================================

def crossed_above(
    series_a,
    series_b
):

    if len(series_a) < 2:

        return False

    current_a = (
        series_a.iloc[-1]
    )

    previous_a = (
        series_a.iloc[-2]
    )

    current_b = (
        series_b.iloc[-1]
    )

    previous_b = (
        series_b.iloc[-2]
    )

    if any(
        pd.isna(x)
        for x in [
            current_a,
            previous_a,
            current_b,
            previous_b
        ]
    ):

        return False

    return (
        previous_a
        <= previous_b
        and current_a
        > current_b
    )


# ============================================================
# CRUCE BAJISTA
# ============================================================

def crossed_below(
    series_a,
    series_b
):

    if len(series_a) < 2:

        return False

    current_a = (
        series_a.iloc[-1]
    )

    previous_a = (
        series_a.iloc[-2]
    )

    current_b = (
        series_b.iloc[-1]
    )

    previous_b = (
        series_b.iloc[-2]
    )

    if any(
        pd.isna(x)
        for x in [
            current_a,
            previous_a,
            current_b,
            previous_b
        ]
    ):

        return False

    return (
        previous_a
        >= previous_b
        and current_a
        < current_b
    )


# ============================================================
# SEÑAL FReinforced
#
# LONG:
# close > SMA 50 de 1H
# + cruce EMA8 sobre EMA21
#
# SHORT:
# close < SMA 50 de 1H
# + cruce EMA8 bajo EMA21
# ============================================================

def calculate_signal(
    symbol
):

    with state_lock:

        candles = list(
            market_data[
                symbol
            ]["candles"]
        )

    if len(candles) < MIN_CANDLES:

        return None

    df = pd.DataFrame(
        candles,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    )

    df = (
        calculate_freinforced_indicators(
            df
        )
    )

    last = df.iloc[-1]

    required = [
        last["ema_short"],
        last["ema_long"],
        last["htf_sma"],
        last["atr"],
    ]

    if any(
        pd.isna(x)
        for x in required
    ):

        return None

    long_cross = (
        crossed_above(
            df["ema_short"],
            df["ema_long"]
        )
    )

    short_cross = (
        crossed_below(
            df["ema_short"],
            df["ema_long"]
        )

    close = float(
        last["close"]
    )

    ema_short = float(
        last["ema_short"]
    )

    ema_long = float(
        last["ema_long"]
    )

    htf_sma = float(
        last["htf_sma"]
    )

    atr = float(
        last["atr"]
    )

    signal = None

    if (
        close > htf_sma
        and long_cross
    ):

        signal = "LONG"

    elif (
        close < htf_sma
        and short_cross
    ):

        signal = "SHORT"

    log(
        f"{symbol} | "
        f"FREINFORCED | "
        f"price={close:.8f} | "
        f"EMA8={ema_short:.8f} | "
        f"EMA21={ema_long:.8f} | "
        f"SMA1H50={htf_sma:.8f} | "
        f"LONG_CROSS={long_cross} | "
        f"SHORT_CROSS={short_cross} | "
        f"signal={signal}"
    )

    return signal


# ============================================================
# WS SIGNATURE
# ============================================================

def _ws_signature(params):

    query = urlencode(
        sorted(
            params.items()
        ),
        doseq=True
    )

    return hmac.new(
        API_SECRET.encode(),
        query.encode(),
        hashlib.sha256
    ).hexdigest()


# ============================================================
# WS API REQUEST
# ============================================================

def ws_api_request(
    method,
    params=None,
    signed=True,
    timeout=15
):

    global user_stream_control

    if not API_KEY or not API_SECRET:

        raise Exception(
            "Faltan API KEY / SECRET"
        )

    with ws_request_lock:

        with user_stream_control_lock:

            ws = user_stream_control

        if ws is None:

            raise Exception(
                "WS API no conectada"
            )

        request_params = dict(
            params or {}
        )

        if signed:

            request_params[
                "apiKey"
            ] = API_KEY

            request_params[
                "timestamp"
            ] = int(
                time.time() * 1000
            )

            request_params[
                "recvWindow"
            ] = 5000

            request_params[
                "signature"
            ] = _ws_signature(
                request_params
            )

        request_id = str(
            uuid.uuid4()
        )

        request = {
            "id": request_id,
            "method": method,
            "params": request_params,
        }

        ws.settimeout(
            timeout
        )

        ws.send(
            json.dumps(request)
        )

        deadline = (
            time.time()
            + timeout
        )

        response = None

        while time.time() < deadline:

            raw = ws.recv()

            if not raw:

                continue

            response = json.loads(
                raw
            )

            if response.get(
                "id"
            ) == request_id:

                break

        if response is None:

            raise Exception(
                f"Timeout esperando "
                f"{method}"
            )

        if response.get(
            "id"
        ) != request_id:

            raise Exception(
                f"Respuesta WS incorrecta: "
                f"{response}"
            )

    if response.get(
        "status"
    ) != 200:

        raise Exception(
            f"{method} rechazado: "
            f"{response}"
        )

    return response.get(
        "result"
    )


# ============================================================
# BALANCE
# ============================================================

def get_usdt_balance():

    if not LIVE_TRADING:

        with state_lock:

            return paper_balance

    result = ws_api_request(
        "account.balance",
        {},
        signed=True
    )

    for item in (
        result or []
    ):

        if item.get(
            "asset"
        ) == "USDT":

            return float(
                item.get(
                    "availableBalance",
                    item.get(
                        "balance",
                        0
                    )
                )
            )

    return 0.0


# ============================================================
# MAXIMA CANTIDAD POR MARGEN
# ============================================================

def calculate_max_quantity_by_margin(
    symbol,
    price,
    available_balance
):

    if (
        price <= 0
        or available_balance <= 0
    ):

        return 0.0

    max_margin = (
        available_balance
        * MAX_MARGIN_USAGE
    )

    max_notional = (
        max_margin
        * LEVERAGE
    )

    return (
        max_notional
        / price
    )


# ============================================================
# CALCULAR CANTIDAD
# ============================================================

def calculate_position_size(
    symbol,
    entry,
    stop_distance
):

    if (
        entry <= 0
        or stop_distance <= 0
    ):

        return 0.0

    balance = get_usdt_balance()

    if balance <= 0:

        return 0.0

    risk_money = (
        balance
        * RISK_PER_TRADE
    )

    # --------------------------------------------------------
    # TAMAÑO POR RIESGO
    # --------------------------------------------------------

    risk_quantity = (
        risk_money
        / stop_distance
    )

    # --------------------------------------------------------
    # TAMAÑO POR MARGEN
    # --------------------------------------------------------

    margin_quantity = (
        calculate_max_quantity_by_margin(
            symbol,
            entry,
            balance
        )
    )

    raw_quantity = min(
        risk_quantity,
        margin_quantity
    )

    quantity = normalize_quantity(
        symbol,
        raw_quantity,
        entry
    )

    if quantity <= 0:

        rule = symbol_rules.get(
            symbol,
            {}
        )

        min_qty = rule.get(
            "min_qty",
            0
        )

        min_notional = rule.get(
            "min_notional",
            0
        )

        log(
            f"{symbol} | "
            f"NO TRADE | "
            f"cantidad calculada menor "
            f"que las reglas Binance | "
            f"balance={balance:.6f} | "
            f"risk=${risk_money:.6f} | "
            f"minQty={min_qty} | "
            f"minNotional={min_notional}"
        )

        return 0.0

    theoretical_risk = (
        quantity
        * stop_distance
    )

    max_allowed_risk = (
        risk_money
    )

    if theoretical_risk > (
        max_allowed_risk
        + 1e-12
    ):

        log(
            f"{symbol} | "
            f"NO TRADE | "
            f"cantidad supera riesgo | "
            f"risk=${theoretical_risk:.6f} | "
            f"max=${max_allowed_risk:.6f}"
        )

        return 0.0

    notional = (
        quantity
        * entry
    )

    margin_required = (
        notional
        / LEVERAGE
    )

    if margin_required > (
        balance
        * MAX_MARGIN_USAGE
    ):

        log(
            f"{symbol} | "
            f"NO TRADE | "
            f"margen insuficiente"
        )

        return 0.0

    log(
        f"{symbol} | "
        f"SIZE | "
        f"balance={balance:.6f} | "
        f"risk=${risk_money:.6f} | "
        f"qty={quantity:.12f} | "
        f"notional=${notional:.6f} | "
        f"margin=${margin_required:.6f} | "
        f"x{LEVERAGE} | "
        f"risk=${theoretical_risk:.6f}"
    )

    return quantity


# ============================================================
# NORMALIZAR STOP
# ============================================================

def normalize_stop_price(
    symbol,
    price,
    side=None
):

    rule = symbol_rules.get(
        symbol
    )

    if not rule:

        return price

    tick = rule.get(
        "tick_size",
        0
    )

    if tick <= 0:

        return price

    # --------------------------------------------------------
    # LONG STOP:
    # debe quedar por debajo.
    #
    # SHORT STOP:
    # debe quedar por encima.
    #
    # Para SHORT usamos ceil.
    # --------------------------------------------------------

    if side == "SHORT":

        value_dec = Decimal(
            str(price)
        )

        tick_dec = Decimal(
            str(tick)
        )

        units = (
            value_dec / tick_dec
        ).to_integral_value(
            rounding="ROUND_CEILING"
        )

        return float(
            units * tick_dec
        )

    return floor_price(
        price,
        tick
    )


# ============================================================
# STOP REAL
# ============================================================

def place_exchange_stop(
    symbol,
    side,
    stop_price
):

    close_side = (
        "SELL"
        if side == "LONG"
        else "BUY"
    )

    stop_price = normalize_stop_price(
        symbol,
        stop_price,
        side
    )

    params = {
        "algoType": "CONDITIONAL",
        "symbol": symbol,
        "side": close_side,
        "type": "STOP_MARKET",
        "triggerPrice": str(
            stop_price
        ),
        "closePosition": "true",
        "workingType": "MARK_PRICE",
        "priceProtect": "FALSE",
    }

    return ws_api_request(
        "algoOrder.place",
        params,
        signed=True
    )


# ============================================================
# CANCELAR STOP
# ============================================================

def cancel_order(
    symbol,
    order_id
):

    if not order_id:

        return True

    try:

        ws_api_request(
            "algoOrder.cancel",
            {
                "symbol": symbol,
                "algoId": int(
                    order_id
                ),
            },
            signed=True
        )

        log(
            f"{symbol} | "
            f"STOP CANCELADO | "
            f"algoId={order_id}"
        )

        return True

    except Exception as e:

        log(
            f"{symbol} | "
            f"ERROR CANCELANDO STOP: "
            f"{e}"
        )

        return False


# ============================================================
# ACTUALIZAR STOP REAL
# ============================================================

def update_exchange_stop(
    symbol,
    position
):

    stop = float(
        position["stop"]
    )

    stop = normalize_stop_price(
        symbol,
        stop,
        position["side"]
    )

    old_id = position.get(
        "stop_order_id"
    )

    old_stop = position.get(
        "exchange_stop_price"
    )

    if (
        old_id
        and old_stop is not None
        and abs(
            old_stop - stop
        )
        <= max(
            abs(stop) * 0.000001,
            1e-12
        )
    ):

        return True

    try:

        result = place_exchange_stop(
            symbol,
            position["side"],
            stop
        )

        result = result or {}

        algo_id = result.get(
            "algoId"
        )

        if not algo_id:

            raise Exception(
                f"Binance no devolvio "
                f"algoId: {result}"
            )

        position[
            "stop_order_id"
        ] = algo_id

        position[
            "exchange_stop_price"
        ] = stop

        log(
            f"{symbol} | "
            f"STOP REAL OK | "
            f"stop={stop:.8f} | "
            f"algoId={algo_id}"
        )

        if old_id:

            cancel_order(
                symbol,
                old_id
            )

        return True

    except Exception as e:

        log(
            f"{symbol} | "
            f"ERROR STOP REAL: {e}"
        )

        if not old_id:

            position[
                "stop_order_id"
            ] = None

            position[
                "exchange_stop_price"
            ] = None

        return False


# ============================================================
# SIMULAR PRECIO DE ENTRADA
# ============================================================

def simulate_entry_price(
    price,
    side
):

    if side == "LONG":

        return price * (
            1 + SLIPPAGE
        )

    return price * (
        1 - SLIPPAGE
    )


# ============================================================
# SIMULAR PRECIO DE SALIDA
# ============================================================

def simulate_exit_price(
    price,
    side
):

    if side == "LONG":

        return price * (
            1 - SLIPPAGE
        )

    return price * (
        1 + SLIPPAGE
    )


# ============================================================
# ABRIR POSICION
# ============================================================

def open_position(
    symbol,
    side
):

    global paper_balance

    with order_lock:

        with state_lock:

            if symbol in positions:

                return

            if len(positions) >= MAX_POSITIONS:

                return

            price = market_data[
                symbol
            ]["price"]

            atr = market_data[
                symbol
            ]["atr"]

        if (
            price is None
            or atr is None
            or atr <= 0
        ):

            return

        stop_distance = (
            atr
            * ATR_STOP_MULT
        )

        quantity = calculate_position_size(
            symbol,
            price,
            stop_distance
        )

        if quantity <= 0:

            return

        # ----------------------------------------------------
        # TEST
        # ----------------------------------------------------

        if not LIVE_TRADING:

            avg_price = (
                simulate_entry_price(
                    price,
                    side
                )
            )

            executed_qty = quantity

            notional = (
                avg_price
                * executed_qty
            )

            margin = (
                notional
                / LEVERAGE
            )

            fee = (
                notional
                * TAKER_FEE
            )

            with state_lock:

                if fee > paper_balance:

                    log(
                        f"{symbol} | "
                        f"TEST | "
                        f"NO HAY BALANCE PARA FEE"
                    )

                    return

                paper_balance -= fee

            log(
                f"TEST OPEN | "
                f"{symbol} | "
                f"{side} | "
                f"entry={avg_price:.8f} | "
                f"qty={executed_qty:.12f} | "
                f"notional=${notional:.6f} | "
                f"margin=${margin:.6f} | "
                f"fee=${fee:.6f} | "
                f"balance=${paper_balance:.6f}"
            )

        # ----------------------------------------------------
        # LIVE
        # ----------------------------------------------------

        else:

            order_side = (
                "BUY"
                if side == "LONG"
                else "SELL"
            )

            try:

                result = ws_api_request(
                    "order.place",
                    {
                        "symbol": symbol,
                        "side": order_side,
                        "type": "MARKET",
                        "quantity": quantity,
                        "newOrderRespType": "RESULT",
                    },
                    signed=True
                )

                result = result or {}

                executed_qty = float(
                    result.get(
                        "executedQty",
                        0
                    )
                )

                avg_price = float(
                    result.get(
                        "avgPrice",
                        0
                    )
                    or 0
                )

                if executed_qty <= 0:

                    raise Exception(
                        f"Orden sin ejecucion: "
                        f"{result}"
                    )

                if avg_price <= 0:

                    avg_price = price

                log(
                    f"LIVE ENTRY EXECUTED | "
                    f"{symbol} | "
                    f"{side} | "
                    f"qty={executed_qty:.12f} | "
                    f"entry={avg_price:.8f}"
                )

            except Exception as e:

                log(
                    f"{symbol} | "
                    f"ERROR ENTRADA: {e}"
                )

                return

        # ----------------------------------------------------
        # STOP
        # ----------------------------------------------------

        initial_stop = (
            avg_price - stop_distance
            if side == "LONG"
            else avg_price + stop_distance
        )

        position = {
            "symbol": symbol,
            "side": side,
            "entry": avg_price,
            "quantity": executed_qty,
            "initial_risk": stop_distance,
            "stop": initial_stop,
            "highest": avg_price,
            "lowest": avg_price,
            "mfe_r": 0.0,
            "bars": 0,
            "opened_at": time.time(),
            "stop_order_id": None,
            "exchange_stop_price": None,
        }

        with state_lock:

            if symbol in positions:

                log(
                    f"{symbol} | "
                    f"YA EXISTE POSICION"
                )

                return

            positions[
                symbol
            ] = position

        # ----------------------------------------------------
        # STOP REAL
        # ----------------------------------------------------

        if LIVE_TRADING:

            stop_ok = update_exchange_stop(
                symbol,
                position
            )

            if not stop_ok:

                log(
                    f"{symbol} | "
                    f"ALERTA CRITICA | "
                    f"STOP NO COLOCADO"
                )

                try:

                    close_position(
                        symbol,
                        avg_price,
                        "STOP NO COLOCADO"
                    )

                except Exception as e:

                    log(
                        f"{symbol} | "
                        f"CIERRE EMERGENCIA "
                        f"FALLO: {e}"
                    )

                return

        log(
            f"POSITION ACTIVE | "
            f"{symbol} | "
            f"{side} | "
            f"entry={avg_price:.8f} | "
            f"qty={executed_qty:.12f} | "
            f"stop={initial_stop:.8f}"
        )


# ============================================================
# CERRAR POSICION
# ============================================================

def close_position(
    symbol,
    price,
    reason
):

    global paper_balance

    with order_lock:

        with state_lock:

            position = positions.get(
                symbol
            )

            if position is None:

                return

            side = position[
                "side"
            ]

            entry = position[
                "entry"
            ]

            quantity = position[
                "quantity"
            ]

            stop_order_id = position.get(
                "stop_order_id"
            )

        # ----------------------------------------------------
        # TEST
        # ----------------------------------------------------

        if not LIVE_TRADING:

            exit_price = (
                simulate_exit_price(
                    price,
                    side
                )
            )

            executed_qty = quantity

        # ----------------------------------------------------
        # LIVE
        # ----------------------------------------------------

        else:

            close_side = (
                "SELL"
                if side == "LONG"
                else "BUY"
            )

            try:

                result = ws_api_request(
                    "order.place",
                    {
                        "symbol": symbol,
                        "side": close_side,
                        "type": "MARKET",
                        "quantity": quantity,
                        "reduceOnly": "true",
                        "newOrderRespType": "RESULT",
                    },
                    signed=True
                )

                result = result or {}

                executed_qty = float(
                    result.get(
                        "executedQty",
                        0
                    )
                )

                exit_price = float(
                    result.get(
                        "avgPrice",
                        0
                    )
                    or 0
                )

                if executed_qty <= 0:

                    raise Exception(
                        f"Cierre sin ejecucion: "
                        f"{result}"
                    )

                if exit_price <= 0:

                    exit_price = price

            except Exception as e:

                log(
                    f"{symbol} | "
                    f"ERROR CERRANDO: {e}"
                )

                return

        # ----------------------------------------------------
        # PNL BRUTO
        # ----------------------------------------------------

        if side == "LONG":

            pnl = (
                exit_price
                - entry
            ) * executed_qty

        else:

            pnl = (
                entry
                - exit_price
            ) * executed_qty

        # ----------------------------------------------------
        # COMISION DE SALIDA
        # ----------------------------------------------------

        exit_notional = (
            exit_price
            * executed_qty
        )

        exit_fee = (
            exit_notional
            * TAKER_FEE
        )

        # ----------------------------------------------------
        # R
        # ----------------------------------------------------

        initial_risk = position[
            "initial_risk"
        ]

        if initial_risk > 0:

            if side == "LONG":

                r_multiple = (
                    exit_price
                    - entry
                ) / initial_risk

            else:

                r_multiple = (
                    entry
                    - exit_price
                ) / initial_risk

        else:

            r_multiple = 0.0

        # ----------------------------------------------------
        # BALANCE TEST
        # ----------------------------------------------------

        net_pnl = (
            pnl
            - exit_fee
        )

        if not LIVE_TRADING:

            with state_lock:

                paper_balance += net_pnl

        # ----------------------------------------------------
        # CANCELAR STOP DESPUES DEL CIERRE
        # ----------------------------------------------------

        if LIVE_TRADING and stop_order_id:

            cancel_order(
                symbol,
                stop_order_id
            )

        # ----------------------------------------------------
        # HISTORIAL
        # ----------------------------------------------------

        trade = {
            "symbol": symbol,
            "side": side,
            "entry": entry,
            "exit": exit_price,
            "quantity": executed_qty,
            "gross_pnl": pnl,
            "exit_fee": exit_fee,
            "net_pnl": net_pnl,
            "r": r_multiple,
            "reason": reason,
            "time": time.time(),
        }

        with state_lock:

            positions.pop(
                symbol,
                None
            )

            trade_history.append(
                trade
            )

            balance_now = (
                paper_balance
            )

        log(
            f"CLOSE | "
            f"{symbol} | "
            f"{side} | "
            f"entry={entry:.8f} | "
            f"exit={exit_price:.8f} | "
            f"qty={executed_qty:.12f} | "
            f"gross={pnl:+.6f} USDT | "
            f"fee={exit_fee:.6f} | "
            f"net={net_pnl:+.6f} | "
            f"R={r_multiple:+.2f} | "
            f"reason={reason} | "
            f"TEST_BAL=${balance_now:.6f}"
        )


# ============================================================
# GESTION POSICION
# ============================================================

def manage_position(
    symbol
):

    with state_lock:

        position = positions.get(
            symbol
        )

        if position is None:

            return

        price = market_data[
            symbol
        ]["price"]

    if price is None:

        return

    side = position[
        "side"
    ]

    entry = position[
        "entry"
    ]

    initial_risk = position[
        "initial_risk"
    ]

    if initial_risk <= 0:

        return

    with state_lock:

        if side == "LONG":

            position[
                "highest"
            ] = max(
                position["highest"],
                price
            )

            mfe = (
                position["highest"]
                - entry
            ) / initial_risk

        else:

            position[
                "lowest"
            ] = min(
                position["lowest"],
                price
            )

            mfe = (
                entry
                - position["lowest"]
            ) / initial_risk

        position[
            "mfe_r"
        ] = max(
            position["mfe_r"],
            mfe
        )

    new_stop = position[
        "stop"
    ]

    # --------------------------------------------------------
    # PROTECCION
    # --------------------------------------------------------

    for trigger_r, protect_r in (
        PROTECTION_LEVELS
    ):

        if mfe >= trigger_r:

            if side == "LONG":

                candidate = (
                    entry
                    + protect_r
                    * initial_risk
                )

                if candidate > new_stop:

                    new_stop = candidate

            else:

                candidate = (
                    entry
                    - protect_r
                    * initial_risk
                )

                if candidate < new_stop:

                    new_stop = candidate

    # --------------------------------------------------------
    # TRAILING
    # --------------------------------------------------------

    for trigger_r, gap_r in (
        TRAILING_LEVELS
    ):

        if mfe >= trigger_r:

            if side == "LONG":

                candidate = (
                    position["highest"]
                    - gap_r
                    * initial_risk
                )

                if candidate > new_stop:

                    new_stop = candidate

            else:

                candidate = (
                    position["lowest"]
                    + gap_r
                    * initial_risk
                )

                if candidate < new_stop:

                    new_stop = candidate

    old_stop = position[
        "stop"
    ]

    with state_lock:

        if side == "LONG":

            position[
                "stop"
            ] = max(
                position["stop"],
                new_stop
            )

        else:

            position[
                "stop"
            ] = min(
                position["stop"],
                new_stop
            )

        stop = position[
            "stop"
        ]

        position[
            "bars"
        ] += 1

        bars = position[
            "bars"
        ]

        mfe_r = position[
            "mfe_r"
        ]

    # --------------------------------------------------------
    # ACTUALIZAR STOP REAL
    # --------------------------------------------------------

    if abs(
        stop - old_stop
    ) > max(
        abs(stop) * 0.000001,
        1e-12
    ):

        if LIVE_TRADING:

            update_exchange_stop(
                symbol,
                position
            )

    # --------------------------------------------------------
    # STOP LOCAL
    # --------------------------------------------------------

    if (
        side == "LONG"
        and price <= stop
    ):

        close_position(
            symbol,
            price,
            f"STOP | MFE={mfe_r:.2f}R"
        )

        return

    if (
        side == "SHORT"
        and price >= stop
    ):

        close_position(
            symbol,
            price,
            f"STOP | MFE={mfe_r:.2f}R"
        )

        return

    # --------------------------------------------------------
    # TIME STOP
    # --------------------------------------------------------

    if (
        bars >= TIME_STOP_CANDLES
        and mfe_r < TIME_STOP_MIN_MFE_R
    ):

        close_position(
            symbol,
            price,
            f"TIME STOP | MFE={mfe_r:.2f}R"
        )


# ============================================================
# PROCESAR VELA CERRADA
# ============================================================

def process_candle(
    symbol
):

    with state_lock:

        candles = list(
            market_data[
                symbol
            ]["candles"]
        )

    if len(candles) < MIN_CANDLES:

        return

    df = pd.DataFrame(
        candles,
        columns=[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
    )

    df = (
        calculate_freinforced_indicators(
            df
        )
    )

    atr = df[
        "atr"
    ].iloc[-1]

    if pd.isna(atr):

        return

    with state_lock:

        market_data[
            symbol
        ]["atr"] = float(
            atr
        )

    signal = calculate_signal(
        symbol
    )

    if signal is None:

        return

    with state_lock:

        existing = positions.get(
            symbol
        )

        price = market_data[
            symbol
        ]["price"]

        total_positions = len(
            positions
        )

    # --------------------------------------------------------
    # POSICION EXISTENTE
    # --------------------------------------------------------

    if existing is not None:

        if (
            existing["side"]
            != signal
        ):

            if price is not None:

                close_position(
                    symbol,
                    price,
                    "SEÑAL CONTRARIA"
                )

        return

    # --------------------------------------------------------
    # LIMITE
    # --------------------------------------------------------

    if total_positions >= MAX_POSITIONS:

        return

    # --------------------------------------------------------
    # ENTRADA
    # --------------------------------------------------------

    open_position(
        symbol,
        signal
    )


# ============================================================
# CARGAR VELAS
# ============================================================

def load_initial_candles():

    log(
        "Cargando historial inicial..."
    )

    for symbol in SYMBOLS:

        try:

            data = rest_request(
                "GET",
                "/fapi/v1/klines",
                {
                    "symbol": symbol,
                    "interval": INTERVAL,
                    "limit": INITIAL_CANDLES,
                }
            )

            candles = []

            for row in data:

                candles.append(
                    [
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5]),
                    ]
                )

            if candles:

                with state_lock:

                    # ultima vela abierta fuera
                    market_data[
                        symbol
                    ]["candles"] = (
                        candles[:-1]
                    )

                    market_data[
                        symbol
                    ]["price"] = (
                        candles[-1][4]
                    )

                log(
                    f"{symbol} | "
                    f"VELAS={len(candles[:-1])}"
                )

        except Exception as e:

            log(
                f"{symbol} | "
                f"ERROR HISTORIAL: {e}"
            )

        time.sleep(
            0.08
        )


# ============================================================
# MARKET WS MESSAGE
# ============================================================

def on_market_message(
    symbol,
    ws,
    message
):

    try:

        data = json.loads(
            message
        )

        data = data.get(
            "data",
            data
        )

        kline = data.get(
            "k"
        )

        if not kline:

            return

        price = float(
            kline["c"]
        )

        with state_lock:

            market_data[
                symbol
            ]["price"] = price

        # ----------------------------------------------------
        # VELA ABIERTA
        # ----------------------------------------------------

        if not kline["x"]:

            return

        # ----------------------------------------------------
        # VELA CERRADA
        # ----------------------------------------------------

        candle = [
            int(kline["t"]),
            float(kline["o"]),
            float(kline["h"]),
            float(kline["l"]),
            float(kline["c"]),
            float(kline["v"]),
        ]

        with state_lock:

            candles = market_data[
                symbol
            ]["candles"]

            if candles:

                if (
                    candles[-1][0]
                    == candle[0]
                ):

                    candles[-1] = candle

                else:

                    candles.append(
                        candle
                    )

            else:

                candles.append(
                    candle
                )

            if len(candles) > 800:

                del candles[:-800]

            market_data[
                symbol
            ]["last_candle_time"] = (
                candle[0]
            )

        process_candle(
            symbol
        )

    except Exception as e:

        log(
            f"{symbol} | "
            f"MARKET WS ERROR: {e}"
        )


# ============================================================
# MARKET WS CALLBACKS
# ============================================================

def make_market_open(
    symbol
):

    def callback(ws):

        log(
            f"{symbol} | "
            f"MARKET WS CONECTADO"
        )

    return callback


def make_market_error(
    symbol
):

    def callback(
        ws,
        error
    ):

        log(
            f"{symbol} | "
            f"MARKET WS ERROR: {error}"
        )

    return callback


def make_market_close(
    symbol
):

    def callback(
        ws,
        code,
        msg
    ):

        log(
            f"{symbol} | "
            f"MARKET WS CERRADO: "
            f"{code} {msg}"
        )

    return callback


# ============================================================
# MARKET WS LOOP
# ============================================================

def market_websocket_loop(
    symbol
):

    market_ws = (
        MARKET_WS_BASE
        + symbol.lower()
        + "@kline_"
        + INTERVAL
    )

    while True:

        try:

            ws = websocket.WebSocketApp(
                market_ws,

                on_open=make_market_open(
                    symbol
                ),

                on_message=lambda ws, message:
                    on_market_message(
                        symbol,
                        ws,
                        message
                    ),

                on_error=make_market_error(
                    symbol
                ),

                on_close=make_market_close(
                    symbol
                ),
            )

            ws.run_forever(
                ping_interval=30,
                ping_timeout=10
            )

        except Exception as e:

            log(
                f"{symbol} | "
                f"MARKET EXCEPTION: {e}"
            )

        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# USER DATA STREAM
# ============================================================

def start_user_data_stream():

    global user_stream_control
    global listen_key

    log(
        "Abriendo WS API..."
    )

    ws = websocket.create_connection(
        WS_API_URL,
        timeout=15
    )

    request_id = str(
        uuid.uuid4()
    )

    request = {
        "id": request_id,
        "method": "userDataStream.start",
        "params": {
            "apiKey": API_KEY
        },
    }

    ws.send(
        json.dumps(request)
    )

    deadline = (
        time.time()
        + 15
    )

    response = None

    while time.time() < deadline:

        response = json.loads(
            ws.recv()
        )

        if response.get(
            "id"
        ) == request_id:

            break

    if (
        response is None
        or response.get("id")
        != request_id
    ):

        ws.close()

        raise Exception(
            "Timeout userDataStream.start"
        )

    if response.get(
        "status"
    ) != 200:

        ws.close()

        raise Exception(
            f"userDataStream.start "
            f"rechazado: {response}"
        )

    key = (
        response
        .get(
            "result",
            {}
        )
        .get(
            "listenKey"
        )
    )

    if not key:

        ws.close()

        raise Exception(
            "Binance no devolvio listenKey"
        )

    with user_stream_control_lock:

        user_stream_control = ws

    listen_key = key

    log(
        "WS API CONECTADA"
    )

    return listen_key


# ============================================================
# USER CALLBACKS
# ============================================================

def on_user_open(ws):

    log(
        "USER DATA WS CONECTADO"
    )


def on_user_error(
    ws,
    error
):

    log(
        f"USER WS ERROR: {error}"
    )


def on_user_close(
    ws,
    code,
    msg
):

    log(
        f"USER WS CERRADO: "
        f"{code} {msg}"
    )


# ============================================================
# SINCRONIZAR POSICIONES
# ============================================================

def sync_account_positions(
    data
):

    account = data.get(
        "a",
        {}
    )

    for item in account.get(
        "P",
        []
    ):

        symbol = item.get(
            "s"
        )

        if symbol not in SYMBOLS:

            continue

        try:

            amount = float(
                item.get(
                    "pa",
                    0
                )
            )

            entry = float(
                item.get(
                    "ep",
                    0
                )
            )

        except Exception:

            continue

        if amount == 0:

            with state_lock:

                positions.pop(
                    symbol,
                    None
                )

            log(
                f"ACCOUNT SYNC | "
                f"{symbol} | "
                f"CERRADA"
            )

            continue

        side = (
            "LONG"
            if amount > 0
            else "SHORT"
        )

        qty = abs(
            amount
        )

        with state_lock:

            old = positions.get(
                symbol
            )

            atr = market_data[
                symbol
            ].get(
                "atr"
            )

            if (
                atr is None
                or atr <= 0
            ):

                risk = (
                    entry
                    * 0.01
                )

            else:

                risk = (
                    atr
                    * ATR_STOP_MULT
                )

            if (
                old
                and old.get("side")
                == side
            ):

                stop = old.get(
                    "stop",
                    (
                        entry - risk
                        if side == "LONG"
                        else entry + risk
                    )
                )

                mfe_r = old.get(
                    "mfe_r",
                    0.0
                )

                bars = old.get(
                    "bars",
                    0
                )

                opened_at = old.get(
                    "opened_at",
                    time.time()
                )

                stop_id = old.get(
                    "stop_order_id"
                )

                exchange_stop = old.get(
                    "exchange_stop_price"
                )

            else:

                stop = (
                    entry - risk
                    if side == "LONG"
                    else entry + risk
                )

                mfe_r = 0.0

                bars = 0

                opened_at = time.time()

                stop_id = None

                exchange_stop = None

            current_price = (
                market_data[
                    symbol
                ].get(
                    "price"
                )
                or entry
            )

            positions[
                symbol
            ] = {
                "symbol": symbol,
                "side": side,
                "entry": entry,
                "quantity": qty,
                "initial_risk": risk,
                "stop": stop,
                "highest": max(
                    entry,
                    current_price
                ),
                "lowest": min(
                    entry,
                    current_price
                ),
                "mfe_r": mfe_r,
                "bars": bars,
                "opened_at": opened_at,
                "stop_order_id": stop_id,
                "exchange_stop_price": (
                    exchange_stop
                ),
            }

        log(
            f"ACCOUNT SYNC | "
            f"{symbol} | "
            f"{side} | "
            f"qty={qty:.12f} | "
            f"entry={entry:.8f}"
        )


# ============================================================
# USER MESSAGE
# ============================================================

def on_user_message(
    ws,
    message
):

    try:

        data = json.loads(
            message
        )

        event_type = data.get(
            "e"
        )

        if event_type == (
            "listenKeyExpired"
        ):

            log(
                "LISTEN KEY EXPIRADO"
            )

            try:

                ws.close()

            except Exception:

                pass

        elif event_type == (
            "ACCOUNT_UPDATE"
        ):

            sync_account_positions(
                data
            )

            log(
                "ACCOUNT_UPDATE"
            )

        elif event_type == (
            "ORDER_TRADE_UPDATE"
        ):

            order = data.get(
                "o",
                {}
            )

            log(
                f"ORDER UPDATE | "
                f"symbol={order.get('s')} | "
                f"side={order.get('S')} | "
                f"status={order.get('X')} | "
                f"exec={order.get('x')}"
            )

        elif event_type == (
            "ALGO_UPDATE"
        ):

            log(
                f"ALGO UPDATE | "
                f"{data}"
            )

    except Exception as e:

        log(
            f"USER MESSAGE ERROR: {e}"
        )


# ============================================================
# USER WS LOOP
# ============================================================

def user_websocket_loop():

    global user_stream_control
    global listen_key

    while True:

        stream_ws = None

        try:

            key = (
                start_user_data_stream()
            )

            if USE_TESTNET:

                ws_url = (
                    "wss://stream.binancefuture.com/"
                    "private/ws/"
                    + key
                )

            else:

                ws_url = (
                    "wss://fstream.binance.com/"
                    "private/ws/"
                    + key
                )

            log(
                "Conectando User Data..."
            )

            stream_ws = websocket.WebSocketApp(
                ws_url,

                on_open=on_user_open,

                on_message=on_user_message,

                on_error=on_user_error,

                on_close=on_user_close,
            )

            stream_ws.run_forever(
                ping_interval=30,
                ping_timeout=10
            )

        except Exception as e:

            log(
                f"USER WS EXCEPTION: {e}"
            )

        finally:

            try:

                if stream_ws:

                    stream_ws.close()

            except Exception:

                pass

            with user_stream_control_lock:

                try:

                    if user_stream_control:

                        user_stream_control.close()

                except Exception:

                    pass

                user_stream_control = None

                listen_key = None

        log(
            "Reconectando User Data en "
            f"{RECONNECT_SECONDS}s..."
        )

        time.sleep(
            RECONNECT_SECONDS
        )


# ============================================================
# KEEPALIVE
# ============================================================

def user_stream_keepalive_loop():

    global user_stream_control
    global listen_key

    while True:

        time.sleep(
            30 * 60
        )

        try:

            with user_stream_control_lock:

                ws = user_stream_control

            if ws is None:

                continue

            request_id = str(
                uuid.uuid4()
            )

            request = {
                "id": request_id,
                "method": "userDataStream.ping",
                "params": {
                    "apiKey": API_KEY
                },
            }

            with ws_request_lock:

                with user_stream_control_lock:

                    ws = (
                        user_stream_control
                    )

                    if ws is None:

                        continue

                    ws.send(
                        json.dumps(
                            request
                        )
                    )

                    ws.settimeout(
                        15
                    )

                    deadline = (
                        time.time()
                        + 15
                    )

                    response = None

                    while (
                        time.time()
                        < deadline
                    ):

                        response = (
                            json.loads(
                                ws.recv()
                            )
                        )

                        if response.get(
                            "id"
                        ) == request_id:

                            break

            if (
                response
                and response.get(
                    "status"
                ) == 200
            ):

                new_key = (
                    response
                    .get(
                        "result",
                        {}
                    )
                    .get(
                        "listenKey"
                    )
                )

                if new_key:

                    listen_key = new_key

                log(
                    "USER DATA KEEPALIVE OK"
                )

            else:

                log(
                    f"KEEPALIVE ERROR: "
                    f"{response}"
                )

        except Exception as e:

            log(
                f"KEEPALIVE ERROR: {e}"
            )

            with user_stream_control_lock:

                try:

                    if user_stream_control:

                        user_stream_control.close()

                except Exception:

                    pass

                user_stream_control = None

                listen_key = None


# ============================================================
# POSITION MANAGER
# ============================================================

def position_manager_loop():

    while True:

        try:

            with state_lock:

                symbols = list(
                    positions.keys()
                )

            for symbol in symbols:

                manage_position(
                    symbol
                )

        except Exception as e:

            log(
                f"POSITION MANAGER ERROR: {e}"
            )

        time.sleep(
            1
        )


# ============================================================
# ESTADISTICAS TEST
# ============================================================

def calculate_test_statistics():

    with state_lock:

        trades = list(
            trade_history
        )

        balance = paper_balance

    total = len(
        trades
    )

    if total == 0:

        return {
            "total": 0,
            "wins": 0,
            "losses": 0,
            "winrate": 0.0,
            "pnl": 0.0,
            "balance": balance,
        }

    wins = sum(
        1
        for x in trades
        if x["net_pnl"] > 0
    )

    losses = (
        total - wins
    )

    pnl = sum(
        x["net_pnl"]
        for x in trades
    )

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "winrate": (
            wins / total
        ) * 100,
        "pnl": pnl,
        "balance": balance,
    }


# ============================================================
# STATUS
# ============================================================

def status_loop():

    while True:

        try:

            with state_lock:

                active = len(
                    positions
                )

                balance_test = (
                    paper_balance
                )

                trades = len(
                    trade_history
                )

            if LIVE_TRADING:

                try:

                    balance = (
                        get_usdt_balance()
                    )

                except Exception as e:

                    balance = None

                    log(
                        f"STATUS BALANCE ERROR: "
                        f"{e}"
                    )

            else:

                balance = balance_test

            stats = (
                calculate_test_statistics()
            )

            log(
                f"STATUS | "
                f"balance="
                f"{balance if balance is not None else 'N/A'} | "
                f"positions="
                f"{active}/{MAX_POSITIONS} | "
                f"trades={trades} | "
                f"wins={stats['wins']} | "
                f"losses={stats['losses']} | "
                f"winrate={stats['winrate']:.1f}% | "
                f"PnL={stats['pnl']:+.6f} | "
                f"x{LEVERAGE}"
            )

        except Exception as e:

            log(
                f"STATUS ERROR: {e}"
            )

        time.sleep(
            60
        )


# ============================================================
# HEALTH SERVER
# ============================================================

def health_server():

    try:

        from http.server import (
            HTTPServer,
            BaseHTTPRequestHandler
        )

        class HealthHandler(
            BaseHTTPRequestHandler
        ):

            def do_GET(self):

                self.send_response(
                    200
                )

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

        port = int(
            os.getenv(
                "PORT",
                "10000"
            )
        )

        server = HTTPServer(
            (
                "0.0.0.0",
                port
            ),
            HealthHandler
        )

        log(
            f"Health server puerto {port}"
        )

        server.serve_forever()

    except Exception as e:

        log(
            f"HEALTH SERVER ERROR: {e}"
        )


# ============================================================
# MAIN
# ============================================================

def main():

    log(
        "================================================"
    )

    log(
        "     UNIVERSAL BINANCE FUTURES BOT V4"
    )

    log(
        "     FREINFORCED - TEST MODE"
    )

    log(
        "================================================"
    )

    log(
        f"LIVE_TRADING      = {LIVE_TRADING}"
    )

    log(
        f"USE_TESTNET       = {USE_TESTNET}"
    )

    log(
        f"LEVERAGE          = x{LEVERAGE}"
    )

    log(
        f"START_BALANCE     = {START_BALANCE} USDT"
    )

    log(
        f"RISK_PER_TRADE    = "
        f"{RISK_PER_TRADE * 100:.2f}%"
    )

    log(
        f"MAX_MARGIN_USAGE  = "
        f"{MAX_MARGIN_USAGE * 100:.1f}%"
    )

    log(
        f"MAX_POSITIONS     = {MAX_POSITIONS}"
    )

    log(
        f"INTERVAL          = {INTERVAL}"
    )

    log(
        f"INITIAL_CANDLES   = {INITIAL_CANDLES}"
    )

    log(
        "STRATEGY          = FReinforced"
    )

    log(
        "EMA               = 8 / 21"
    )

    log(
        "HTF               = 1H SMA 50"
    )

    log(
        "================================================"
    )

    if not API_KEY or not API_SECRET:

        log(
            "FATAL: faltan "
            "BINANCE_API_KEY / "
            "BINANCE_API_SECRET"
        )

        return

    # --------------------------------------------------------
    # HEALTH
    # --------------------------------------------------------

    threading.Thread(
        target=health_server,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # REGLAS
    # --------------------------------------------------------

    try:

        load_symbol_rules()

    except Exception as e:

        log(
            f"FATAL REGLAS BINANCE: {e}"
        )

        return

    # --------------------------------------------------------
    # HISTORIAL
    # --------------------------------------------------------

    try:

        load_initial_candles()

    except Exception as e:

        log(
            f"ERROR HISTORIAL: {e}"
        )

    # --------------------------------------------------------
    # LEVERAGE
    # --------------------------------------------------------

    configure_all_leverage()

    # --------------------------------------------------------
    # MARKET WS
    # --------------------------------------------------------

    for symbol in SYMBOLS:

        if symbol not in symbol_rules:

            continue

        threading.Thread(
            target=market_websocket_loop,
            args=(symbol,),
            daemon=True
        ).start()

        time.sleep(
            0.10
        )

    # --------------------------------------------------------
    # USER DATA WS
    #
    # SE MANTIENE IGUAL QUE V3.
    # --------------------------------------------------------

    threading.Thread(
        target=user_websocket_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # KEEPALIVE
    # --------------------------------------------------------

    threading.Thread(
        target=user_stream_keepalive_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # POSITION MANAGER
    # --------------------------------------------------------

    threading.Thread(
        target=position_manager_loop,
        daemon=True
    ).start()

    # --------------------------------------------------------
    # STATUS
    # --------------------------------------------------------

    threading.Thread(
        target=status_loop,
        daemon=True
    ).start()

    log(
        "================================================"
    )

    log(
        "BOT V4 INICIADO"
    )

    log(
        "MODO = TEST"
    )

    log(
        f"CAPITAL SIMULADO = "
        f"{START_BALANCE:.4f} USDT"
    )

    log(
        "NO SE ENVIARAN ORDENES REALES"
    )

    log(
        f"LEVERAGE SIMULADO = x{LEVERAGE}"
    )

    log(
        f"RISK = "
        f"{RISK_PER_TRADE * 100:.2f}%"
    )

    log(
        "ESTRATEGIA = FREINFORCED"
    )

    log(
        "ESPERANDO VELAS CERRADAS..."
    )

    log(
        "================================================"
    )

    while True:

        time.sleep(
            60
        )


# ============================================================
# START
# ============================================================

if __name__ == "__main__":

    try:

        main()

    except KeyboardInterrupt:

        log(
            "BOT DETENIDO MANUALMENTE"
        )

    except Exception as e:

        log(
            f"FATAL ERROR: {e}"
        )

        while True:

            time.sleep(
                60
            )
