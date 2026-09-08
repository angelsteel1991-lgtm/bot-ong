import os, time, math, json, uuid, hmac, hashlib, threading, sqlite3
from datetime import datetime, timezone
from urllib.parse import urlencode
import urllib.request
import websocket
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
load_dotenv()

# ============================================================
# CONFIG - MODO SIMULACRO BLINDADO
# ============================================================
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").lower() == "true"
PAPER_MODE = os.getenv("PAPER_MODE", "true").lower() == "true"

if LIVE_TRADING and PAPER_MODE:
    raise Exception("CONFIG ERROR: LIVE_TRADING=true y PAPER_MODE=true -> NO OPERAR")

if not (LIVE_TRADING and not PAPER_MODE):
    LIVE_TRADING = False
    PAPER_MODE = True

SYMBOL = os.getenv("SYMBOL", "ONGUSDT")
LEVERAGE = int(os.getenv("LEVERAGE", "6"))
CAPITAL_ARS = float(os.getenv("CAPITAL_REFERENCE_ARS", "10000"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "0.005"))
ATR_SL_MULT = float(os.getenv("ATR_SL_MULTIPLIER", "1.2"))
TP1_R = float(os.getenv("TP1_R", "1.5"))
TP2_R = float(os.getenv("TP2_R", "2.5"))
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "cambia_este_secret")
USDT_ARS_RATE = float(os.getenv("USDT_ARS_RATE", "1500"))
DB_PATH = os.getenv("DATABASE_URL", "sqlite:///ong_paper.db").replace("sqlite:///","")
PORT = int(os.getenv("PORT", "10000"))

API_KEY = os.getenv("BINANCE_API_KEY")
API_SECRET = os.getenv("BINANCE_API_SECRET")

MARKET_WS_BASE = "wss://fstream.binance.com/market/ws/"
EXCHANGE_INFO_URL = "https://fapi.binance.com/fapi/v1/exchangeInfo"

symbol_rules = {}
market_price = {SYMBOL: None}

def log(msg):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] {msg}", flush=True)

class SecurityBlock(Exception): pass

def can_execute_real_order():
    paper = os.getenv("PAPER_MODE", "true").lower() == "true"
    live = os.getenv("LIVE_TRADING", "false").lower() == "true"
    if paper and live: raise Exception("CONTRADICCION BLOQUEADA")
    if paper: return False
    if not live: return False
    return True

def validate_secret(received, expected):
    return hmac.compare_digest(received or "", expected or "")

def load_symbol_rules(symbols):
    global symbol_rules
    fallback = {s: {"step": 0.001, "min_qty": 0.001, "min_notional": 5.0, "tick": 0.00001} for s in symbols}
    try:
        req = urllib.request.Request(EXCHANGE_INFO_URL, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode())
        info_by_symbol = {s["symbol"]: s for s in data.get("symbols", [])}
        rules = {}
        for symbol in symbols:
            info = info_by_symbol.get(symbol)
            if not info:
                rules[symbol] = fallback[symbol]
                continue
            step=0.001; min_qty=0.001; min_notional=5.0; tick=0.01
            filters = info.get("filters", [])
            lot = next((f for f in filters if f.get("filterType") == "MARKET_LOT_SIZE"), None) or next((f for f in filters if f.get("filterType") == "LOT_SIZE"), None)
            if lot:
                step = float(lot.get("stepSize", step))
                min_qty = float(lot.get("minQty", min_qty))
            notional = next((f for f in filters if f.get("filterType") == "MIN_NOTIONAL"), None)
            if notional: min_notional = float(notional.get("notional", min_notional))
            price_f = next((f for f in filters if f.get("filterType") == "PRICE_FILTER"), None)
            if price_f: tick = float(price_f.get("tickSize", tick))
            rules[symbol] = {"step": step, "min_qty": min_qty, "min_notional": min_notional, "tick": tick}
        symbol_rules = rules
        return rules
    except Exception as e:
        symbol_rules = fallback
        return fallback

def normalize_quantity(symbol, quantity, price):
    rule = symbol_rules.get(symbol)
    if not rule: return 0.0
    step = rule["step"]; min_qty=rule["min_qty"]; min_notional=rule["min_notional"]
    if step<=0: return 0.0
    quantity = math.floor(quantity/step)*step
    if quantity < min_qty: quantity = min_qty
    if quantity * price < min_notional:
        quantity = math.ceil((min_notional/price)/step)*step
    decimals = max(0, int(round(-math.log10(step)))) if step<1 else 0
    decimals = min(decimals, 12)
    return float(f"{quantity:.{decimals}f}")

db_lock = threading.Lock()

def init_db():
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("CREATE TABLE IF NOT EXISTS signals (signal_id TEXT PRIMARY KEY, symbol TEXT, side TEXT, price REAL, atr REAL, timestamp INTEGER, candle_time INTEGER, received_at TEXT, raw_json TEXT, status TEXT, reason TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS paper_account (id INTEGER PRIMARY KEY CHECK (id=1), balance_ars REAL, updated_at TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS paper_positions (id INTEGER PRIMARY KEY AUTOINCREMENT, signal_id TEXT, symbol TEXT, side TEXT, entry_price REAL, qty REAL, notional REAL, margin REAL, leverage INTEGER, sl REAL, tp1 REAL, tp2 REAL, risk_ars REAL, status TEXT, entry_time TEXT, exit_price REAL, exit_time TEXT, exit_reason TEXT, pnl_ars REAL, pnl_r REAL, balance_before REAL, balance_after REAL)")
        c.execute("INSERT OR IGNORE INTO paper_account (id, balance_ars, updated_at) VALUES (1, 10000,?)", (datetime.utcnow().isoformat(),))
        conn.commit()
        conn.close()

def is_duplicate(sid):
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT 1 FROM signals WHERE signal_id=?", (sid,))
        e = c.fetchone() is not None
        conn.close()
        return e

def save_signal(sid, symbol, side, price, atr, ts, ct, raw, status, reason=None):
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT OR REPLACE INTO signals VALUES (?,?,?,?,?,?,?,?,?,?,?)", (sid, symbol, side, price, atr, ts, ct, datetime.utcnow().isoformat(), json.dumps(raw), status, reason))
        conn.commit()
        conn.close()

def get_balance():
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT balance_ars FROM paper_account WHERE id=1")
        row = c.fetchone()
        conn.close()
        return float(row[0]) if row else 10000.0

def get_open_position():
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT * FROM paper_positions WHERE status='OPEN' LIMIT 1")
        row = c.fetchone()
        conn.close()
        return row

def simulate_entry(signal_id, symbol, side, entry, atr):
    balance_before = get_balance()
    sl_dist = atr * ATR_SL_MULT
    if side=="LONG":
        sl = entry - sl_dist
        tp1 = entry + sl_dist * TP1_R
    else:
        sl = entry + sl_dist
        tp1 = entry - sl_dist * TP1_R
    risk_ars = balance_before * RISK_PER_TRADE
    risk_usdt = risk_ars / USDT_ARS_RATE
    sl_distance = abs(entry - sl)
    if sl_distance==0: return {"ok": False, "reason": "SL_ZERO"}
    qty_raw = risk_usdt / sl_distance
    qty = normalize_quantity(symbol, qty_raw, entry)
    if qty<=0: return {"ok": False, "reason": "QTY_ZERO"}
    notional = qty * entry
    margin = notional / LEVERAGE
    if margin > (balance_before/USDT_ARS_RATE): return {"ok": False, "reason": "INSUFFICIENT_MARGIN"}
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT INTO paper_positions (signal_id, symbol, side, entry_price, qty, notional, margin, leverage, sl, tp1, tp2, risk_ars, status, entry_time, balance_before) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (signal_id, symbol, side, entry, qty, notional, margin, LEVERAGE, sl, tp1, tp1, risk_ars, 'OPEN', datetime.utcnow().isoformat(), balance_before))
        conn.commit()
        conn.close()
    log(f"[PAPER ORDER] SIMULATED {side} {symbol} entry={entry} qty={qty} SL={sl:.6f} TP1={tp1:.6f} risk={risk_ars:.2f} ARS")
    return {"ok": True, "sl": sl, "tp1": tp1, "qty": qty}

def simulate_exit(current_price, reason):
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("SELECT id, side, entry_price, qty, balance_before FROM paper_positions WHERE status='OPEN' LIMIT 1")
        row = c.fetchone()
        if not row:
            conn.close()
            return
        pos_id, side, entry, qty, bal_before = row
        pnl_usdt = (current_price - entry) * qty if side=="LONG" else (entry - current_price) * qty
        pnl_ars = pnl_usdt * USDT_ARS_RATE
        risk_usdt = (bal_before * RISK_PER_TRADE) / USDT_ARS_RATE
        pnl_r = pnl_usdt / risk_usdt if risk_usdt!=0 else 0
        new_bal = bal_before + pnl_ars
        c.execute("UPDATE paper_positions SET status='CLOSED', exit_price=?, exit_time=?, exit_reason=?, pnl_ars=?, pnl_r=?, balance_after=? WHERE id=?", (current_price, datetime.utcnow().isoformat(), reason, pnl_ars, pnl_r, new_bal, pos_id))
        c.execute("UPDATE paper_account SET balance_ars=? WHERE id=1", (new_bal,))
        conn.commit()
        conn.close()
    log(f"[PAPER RESULT] {side} EXIT={current_price:.6f} ENTRY={entry:.6f} PnL={pnl_ars:.2f} ARS R={pnl_r:.2f} BAL {bal_before:.2f}->{new_bal:.2f} Reason={reason}")

def on_market_message(ws, message):
    try:
        data = json.loads(message)
        data = data.get("data", data)
        k = data.get("k")
        if not k: return
        price = float(k["c"])
        market_price[SYMBOL] = price
        pos = get_open_position()
        if pos:
            side = pos[3]
            sl = pos[8]
            tp1 = pos[9]
            if side=="LONG":
                if price <= sl: simulate_exit(price, "SL")
                elif price >= tp1: simulate_exit(price, "TP1")
            else:
                if price >= sl: simulate_exit(price, "SL")
                elif price <= tp1: simulate_exit(price, "TP1")
    except Exception as e:
        log(f"Market WS error: {e}")

def market_ws_loop():
    url = MARKET_WS_BASE + "ongusdt@kline_5m"
    while True:
        try:
            log(f"ONG Market WS conectando {url}")
            ws = websocket.WebSocketApp(url, on_message=on_market_message)
            ws.run_forever(ping_interval=60, ping_timeout=20)
        except Exception as e:
            log(f"Market WS exception: {e}")
        time.sleep(10)

app = FastAPI(title="ONG Bot PAPER - Single File")

@app.on_event("startup")
def startup():
    log("==========================================")
    log("ONG BOT SINGLE FILE - MODO PAPER")
    log(f"LIVE={LIVE_TRADING} PAPER={PAPER_MODE} SYMBOL={SYMBOL} LEV=x{LEVERAGE} CAP={CAPITAL_ARS} ARS")
    log("==========================================")
    init_db()
    load_symbol_rules([SYMBOL])
    threading.Thread(target=market_ws_loop, daemon=True).start()

@app.get("/health")
def health():
    return {"status":"ok","bot":"ONG-TV","mode":"PAPER","symbol":SYMBOL,"balance_ars":get_balance(),"price":market_price.get(SYMBOL)}

@app.post("/webhook")
async def webhook(request: Request):
    try:
        payload = await request.json()
    except:
        return JSONResponse({"ok": False, "reason": "INVALID_JSON"}, status_code=400)
    secret = payload.get("secret")
    if not validate_secret(secret, WEBHOOK_SECRET):
        log("[TV SIGNAL] REJECTED bad secret")
        return {"ok": False, "reason": "BAD_SECRET"}
    symbol = payload.get("symbol", SYMBOL)
    side = payload.get("side")
    price = float(payload.get("price", 0))
    atr = float(payload.get("atr", 0) or 0.001)
    ts = int(payload.get("timestamp", 0))
    ct = int(payload.get("candle_time", 0))
    signal_id = payload.get("signal_id") or f"{symbol}_{side}_{ct}"
    if symbol!= SYMBOL:
        save_signal(signal_id, symbol, side, price, atr, ts, ct, payload, "REJECTED", "SYMBOL_MISMATCH")
        return {"ok": False, "reason": "SYMBOL_MISMATCH"}
    if side not in ("LONG","SHORT"):
        save_signal(signal_id, symbol, side, price, atr, ts, ct, payload, "REJECTED", "INVALID_SIDE")
        return {"ok": False, "reason": "INVALID_SIDE"}
    if is_duplicate(signal_id):
        log(f"[TV SIGNAL] DUPLICATE {signal_id}")
        save_signal(signal_id, symbol, side, price, atr, ts, ct, payload, "REJECTED", "DUPLICATE")
        return {"ok": False, "reason": "DUPLICATE"}
    if ts and (int(time.time()) - ts) > 180:
        save_signal(signal_id, symbol, side, price, atr, ts, ct, payload, "REJECTED", "TOO_OLD")
        return {"ok": False, "reason": "TOO_OLD"}
    if get_open_position():
        save_signal(signal_id, symbol, side, price, atr, ts, ct, payload, "REJECTED", "MAX_POSITIONS")
        log(f"[VALIDATION] REJECTED {symbol} {side} Reason: MAX_POSITIONS")
        return {"ok": False, "reason": "MAX_POSITIONS"}
    save_signal(signal_id, symbol, side, price, atr, ts, ct, payload, "ACCEPTED", None)
    log(f"[TV SIGNAL] {symbol} {side} PRICE={price} ATR={atr} ID={signal_id}")
    result = simulate_entry(signal_id, symbol, side, price, atr)
    if not result["ok"]:
        save_signal(signal_id, symbol, side, price, atr, ts, ct, payload, "REJECTED", result["reason"])
        return {"ok": False, "reason": result["reason"]}
    return {"ok": True, "signal_id": signal_id, "entry": price, "sl": result["sl"], "tp1": result["tp1"]}

@app.get("/")
def root():
    return {"message":"ONG Bot PAPER running","health":"/health","webhook":"/webhook"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
