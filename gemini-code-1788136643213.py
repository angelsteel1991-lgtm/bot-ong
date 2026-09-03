# ============================================================
# USER DATA STREAM
# ============================================================
def start_user_data_stream():
    global listen_key
    log("Solicitando listenKey vía REST para User Data Stream...")
    
    # Se solicita el listenKey usando HTTP POST directa
    data = signed_request("POST", "/fapi/v1/listenKey")
    key = data.get("listenKey")
    
    if not key:
        raise Exception("Binance no devolvió listenKey")
        
    with user_stream_control_lock:
        listen_key = key
        
    log("ListenKey recibido correctamente vía REST")
    return listen_key

def user_stream_keepalive_loop():
    global listen_key
    while True:
        time.sleep(30 * 60) # Renueva cada 30 minutos
        try:
            if listen_key:
                signed_request("PUT", "/fapi/v1/listenKey")
                log("USER DATA STREAM KEEPALIVE OK (vía REST)")
        except Exception as e:
            log(f"User Data keepalive error: {e}")
