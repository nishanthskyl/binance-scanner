import streamlit as st
import pandas as pd
import requests
from datetime import datetime, timedelta
import time
from dateutil.relativedelta import relativedelta
import pytz
from zoneinfo import ZoneInfo
import io
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import numpy as np
import threading
import json
import os
import sys
import logging
import websocket
from collections import deque
from streamlit_autorefresh import st_autorefresh

# Configure logging so warmup errors are visible in the terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("BSLoader")

# ==============================================
# AUTOMATIC EUROPEAN PROXY ROUTER FOR US SERVERS (BYPASS US BLOCK)
# ==============================================

CURRENT_PROXY = None
PROXY_HOST = None
PROXY_PORT = None
PROXY_READY = False
PROXY_NEEDED = None  # None = unknown, True = yes, False = no

def init_proxy_rotator():
    global CURRENT_PROXY, PROXY_HOST, PROXY_PORT, PROXY_READY, PROXY_NEEDED
    if PROXY_NEEDED is not None:
        return
    log.info("[Proxy] Checking if running in US and blocked by Binance...")
    _original_get = requests.get
    
    try:
        # Check if direct request to Binance works
        r = _original_get("https://fapi.binance.com/fapi/v1/ping", timeout=3)
        if r.status_code == 200:
            log.info("[Proxy] Direct connection to Binance works! No proxy needed.")
            PROXY_NEEDED = False
            PROXY_READY = True
            return
    except Exception as e:
        log.warning(f"[Proxy] Direct connection blocked/failed: {e}. Fetching global proxy list...")
        PROXY_NEEDED = True
    
    # Try fetching proxies from a highly-updated public repository
    try:
        url = "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt"
        resp = _original_get(url, timeout=5)
        if resp.status_code == 200:
            proxies_list = [p.strip() for p in resp.text.split("\n") if p.strip()]
            log.info(f"[Proxy] Fetched {len(proxies_list)} potential proxies. Testing in parallel...")
            
            # Parallel testing helper
            def test_single_proxy(proxy):
                test_proxies = {
                    "http": f"http://{proxy}",
                    "https": f"http://{proxy}"
                }
                try:
                    test_r = _original_get("https://fapi.binance.com/fapi/v1/ping", proxies=test_proxies, timeout=2.5)
                    if test_r.status_code == 200:
                        return proxy, test_proxies
                except:
                    pass
                return None

            # Test first 150 proxies in parallel
            test_subset = proxies_list[:150]
            with ThreadPoolExecutor(max_workers=30) as executor:
                results = list(executor.map(test_single_proxy, test_subset))
                
                # Use the first working proxy
                for res in results:
                    if res:
                        proxy, test_proxies = res
                        log.info(f"[Proxy] Found working proxy bypassing Binance block: {proxy}")
                        CURRENT_PROXY = test_proxies
                        parts = proxy.split(":")
                        PROXY_HOST = parts[0]
                        PROXY_PORT = int(parts[1])
                        PROXY_READY = True
                        return
    except Exception as ex:
        log.error(f"[Proxy] Error fetching proxy list: {ex}")
    
    log.error("[Proxy] Could not find any working proxy.")
    PROXY_READY = True  # Mark ready so calls don't hang forever even if all fail

# Run initialization in a non-blocking background thread
threading.Thread(target=init_proxy_rotator, daemon=True).start()

# Overwrite requests.get to automatically inject the proxy
_original_get = requests.get
def proxy_get(url, *args, **kwargs):
    if CURRENT_PROXY and "proxies" not in kwargs:
        kwargs["proxies"] = CURRENT_PROXY
    return _original_get(url, *args, **kwargs)
requests.get = proxy_get

# Silence noise
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# Set page configuration
st.set_page_config(
    page_title="Binance Scanner Pro (WS-Buffer) - Breakout + Volume Analysis",
    page_icon="🚀",
    layout="wide"
)

# ==============================================
# WEBSOCKET MANAGER WITH K-LINE BUFFERING
# ==============================================

class BinanceWSLoader:
    def __init__(self):
        self.data = {}
        self.klines = {} 
        self.sockets = []
        self.launcher_thread = None
        self.warmup_thread = None
        self.running = False
        self.initialized = False
        self.status = "Offline"
        self.last_update = None
        self.msg_count = 0
        self.warmup_progress = 0
        self.total_symbols = 0
        self.realtime_intervals = ["1h", "4h"] 
        self.all_intervals = ["1h", "4h", "1d", "1w", "1M"]
        self._lock = threading.Lock()
        # --- Debug / diagnostics ---
        self.warmup_errors = []       # list of error strings from warmup
        self.kline_errors = 0         # count of kline handler errors
        self.warmup_fetched = 0       # successful REST kline fetches
        self.warmup_failed = 0        # failed REST kline fetches

    def start(self):
        with self._lock:
            if self.initialized: return
            self.initialized = True
            self.running = True
            self.status = "Initializing..."
        
        # Start a single launcher thread that handles everything else in background
        self.launcher_thread = threading.Thread(target=self._launch_sequence, daemon=True)
        self.launcher_thread.start()

    def _launch_sequence(self):
        try:
            # Step 1: Start with hardcoded fallback instantly
            symbols = [
                "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT", "DOGEUSDT", 
                "DOTUSDT", "LINKUSDT", "MATICUSDT", "LTCUSDT", "1000SHIBUSDT", "TRXUSDT", "UNIUSDT", "NEARUSDT",
                "FILUSDT", "ATOMUSDT", "VETUSDT", "ETCUSDT", "ICPUSDT", "FTMUSDT", "INJUSDT", "OPUSDT", 
                "ARBUSDT", "TIAUSDT", "SEIUSDT", "SUIUSDT", "RUNEUSDT", "ORDIUSDT", "1000PEPEUSDT", "1000FLOKIUSDT",
                "WLDUSDT", "ARKMUSDT", "PYTHUSDT", "JUPUSDT", "DYMUSDT", "STRKUSDT", "ENAUSDT", "ONDOUSDT"
            ]
            self.total_symbols = len(symbols)
            self.status = "Connecting WebSocket..."
            
            # Start WebSocket threads
            self._start_websockets(symbols)
            
            # Start Warmup thread
            self.warmup_thread = threading.Thread(target=self._warm_up, args=(symbols,), daemon=True)
            self.warmup_thread.start()
            
            # Step 2: Try to discover more markets in background quietly
            try:
                r = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=5)
                full_symbols = [s['symbol'] for s in r.json()['symbols'] if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING']
                if len(full_symbols) > len(symbols):
                    pass
            except:
                pass
                
        except Exception as e:
            self.status = f"Launch Failed: {e}"
            self.initialized = False

    def _start_websockets(self, symbols):
        # 1. Price Tickers Stream
        self._create_socket("wss://fstream.binance.com/ws/!miniTicker@arr")
        
        # 2. Key K-Lines (1h, 4h)
        streams = []
        for s in symbols:
            for intv in self.realtime_intervals:
                streams.append(f"{s.lower()}@kline_{intv}")
        
        # Binance URL limit: ~2048 bytes. Batches of 50 are safer (~750 bytes).
        for i in range(0, len(streams), 50):
            batch = streams[i:i+50]
            url = f"wss://fstream.binance.com/stream?streams={'/'.join(batch)}"
            self._create_socket(url)

    def _create_socket(self, url):
        def on_message(ws, message):
            self.last_update = datetime.now()
            self.msg_count += 1
            data = json.loads(message)
            
            if isinstance(data, dict) and 'data' in data:
                payload = data['data']
                stream = data.get('stream', '')
                if 'kline' in stream:
                    self._handle_kline(payload)
            elif isinstance(data, list):
                self._handle_ticker(data)
            elif isinstance(data, dict):
                if data.get('e') == 'kline':
                    self._handle_kline(data)

        def on_error(ws, error): pass
        def on_close(ws, close_status_code, close_msg): pass
        def on_open(ws): pass

        ws = websocket.WebSocketApp(url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
        
        kwargs = {}
        if PROXY_HOST and PROXY_PORT:
            kwargs["http_proxy_host"] = PROXY_HOST
            kwargs["http_proxy_port"] = PROXY_PORT
            
        wst = threading.Thread(target=ws.run_forever, kwargs=kwargs, daemon=True)
        wst.start()
        self.sockets.append(ws)

    def _handle_ticker(self, payload):
        for t in payload:
            if not isinstance(t, dict): continue
            sym = t.get('s')
            if sym: 
                self.data[sym] = {
                    'price': float(t.get('c', 0)), 
                    'quoteVolume': float(t.get('q', 0)), 
                    'priceChangePercent': float(t.get('P', 0))
                }

    def _handle_kline(self, k_data):
        try:
            s = k_data.get('s')
            k = k_data.get('k')
            if not s or not k: return
            interval = k.get('i')
            if s not in self.klines: 
                self.klines[s] = {intv: deque(maxlen=60) for intv in self.all_intervals}
            if interval in self.all_intervals:
                candle = [
                    k.get('t'), k.get('o'), k.get('h'), k.get('l'), k.get('c'), 
                    k.get('v'), k.get('T'), k.get('q'), k.get('n'), k.get('V'), k.get('Q'), "0"
                ]
                buffer = self.klines[s][interval]
                if len(buffer) > 0 and buffer[-1][0] == candle[0]:
                    buffer[-1] = candle
                else:
                    buffer.append(candle)
        except Exception as ex:
            self.kline_errors += 1
            if self.kline_errors <= 5:  # Only log first 5 to avoid spam
                log.warning(f"[kline_handler] Error: {ex}")

    def _warm_up(self, symbols):
        # Wait for the background proxy selector if we are in a blocked region
        for _ in range(20):
            if PROXY_NEEDED is None or (PROXY_NEEDED and not PROXY_READY):
                time.sleep(0.5)
            else:
                break
                
        try:
            self.status = "Buffering History (1H/4H)..."
            self.warmup_progress = 0
            self.warmup_errors = []
            self.warmup_fetched = 0
            self.warmup_failed = 0
            log.info(f"[Warmup] Starting Phase 1 (1H/4H) for {len(symbols)} symbols")
            
            def fetch_priority(s):
                if not self.running: return
                if s not in self.klines: 
                    self.klines[s] = {intv: deque(maxlen=60) for intv in self.all_intervals}
                for interval in ["1h", "4h"]:
                    try:
                        resp = requests.get(
                            "https://fapi.binance.com/fapi/v1/klines",
                            params={'symbol': s, 'interval': interval, 'limit': 60},
                            timeout=5
                        )
                        if resp.status_code == 200:
                            rows = resp.json()
                            self.klines[s][interval].extend(rows)
                            self.warmup_fetched += 1
                            log.debug(f"[Warmup] {s} {interval}: {len(rows)} candles")
                        else:
                            err = f"{s}/{interval}: HTTP {resp.status_code}"
                            self.warmup_errors.append(err)
                            self.warmup_failed += 1
                            log.warning(f"[Warmup] {err}")
                    except Exception as ex:
                        err = f"{s}/{interval}: {ex}"
                        self.warmup_errors.append(err)
                        self.warmup_failed += 1
                        log.warning(f"[Warmup] {err}")
                self.warmup_progress += 0.5

            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
                for s in symbols:
                    executor.submit(fetch_priority, s)
                    time.sleep(0.01)

            log.info(f"[Warmup] Phase 1 done. fetched={self.warmup_fetched}, failed={self.warmup_failed}, klines_keys={len(self.klines)}")

            self.status = "Buffering History (D/W/M)..."
            log.info(f"[Warmup] Starting Phase 2 (D/W/M)")

            def fetch_secondary(s):
                if not self.running: return
                if s not in self.klines:
                    self.klines[s] = {intv: deque(maxlen=60) for intv in self.all_intervals}
                for interval in ["1d", "1w", "1M"]:
                    try:
                        resp = requests.get(
                            "https://fapi.binance.com/fapi/v1/klines",
                            params={'symbol': s, 'interval': interval, 'limit': 60},
                            timeout=5
                        )
                        if resp.status_code == 200:
                            self.klines[s][interval].extend(resp.json())
                            self.warmup_fetched += 1
                        else:
                            err = f"{s}/{interval}: HTTP {resp.status_code}"
                            self.warmup_errors.append(err)
                            self.warmup_failed += 1
                            log.warning(f"[Warmup] {err}")
                    except Exception as ex:
                        err = f"{s}/{interval}: {ex}"
                        self.warmup_errors.append(err)
                        self.warmup_failed += 1
                        log.warning(f"[Warmup] {err}")
                self.warmup_progress += 0.5

            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                for s in symbols:
                    executor.submit(fetch_secondary, s)
                    time.sleep(0.02)

            log.info(f"[Warmup] Phase 2 done. fetched={self.warmup_fetched}, failed={self.warmup_failed}, klines_keys={len(self.klines)}")
            self.status = "Live (Buffered)"
        except Exception as e:
            err_msg = f"Warmup Failed: {e}"
            self.status = err_msg
            self.warmup_errors.append(err_msg)
            log.error(f"[Warmup] CRITICAL: {e}", exc_info=True)

    def get_klines(self, symbol, interval):
        if symbol in self.klines and interval in self.klines[symbol]:
            buf = list(self.klines[symbol][interval])
            if len(buf) > 5: return buf
        return None

@st.cache_resource
def get_ws_loader():
    """Persistent singleton for the WebSocket manager"""
    loader = BinanceWSLoader()
    return loader

def render_ws_status():
    WS_LOADER = get_ws_loader()
    with st.sidebar:
        st.divider()
        st.subheader("🌐 WebSocket status")
        
        # Auto-refresh UI every 10 seconds while buffering to show progress
        if WS_LOADER.status != "Live (Buffered)":
            st_autorefresh(interval=10000, key="ws_status_refresh")
        
        # Auto-start if not running
        if not WS_LOADER.initialized:
            WS_LOADER.start()
            
        if WS_LOADER.status in ["Connecting WebSocket...", "Buffering History...", "Fetching Markets...", "Starting Manager...", "Initializing..."]:
            st.info(f"⏳ {WS_LOADER.status}")
            if WS_LOADER.total_symbols > 0:
                progress = WS_LOADER.warmup_progress / WS_LOADER.total_symbols
                progress = max(0.0, min(1.0, progress))
                st.progress(progress)
                st.caption(f"Buffering: {WS_LOADER.warmup_progress}/{WS_LOADER.total_symbols} coins...")
            # Removed st.rerun() to prevent UI hang. Scanner will render below.
            st.caption("Background data buffering...")
            if st.button("🔄 Refresh Status"):
                st.rerun()
        elif WS_LOADER.status == "Live (Buffered)":
            st.success("🟢 WebSocket Active & Synced")
            st.caption(f"Memory Sync Active: {len(WS_LOADER.klines)} coins")
            if WS_LOADER.last_update:
                st.caption(f"Last data: {WS_LOADER.last_update.strftime('%H:%M:%S')}")
            st.caption(f"Messages: {WS_LOADER.msg_count}")
            # Debug warmup stats
            st.caption(f"✅ REST fetches: {WS_LOADER.warmup_fetched} | ❌ Failed: {WS_LOADER.warmup_failed}")
            if WS_LOADER.kline_errors:
                st.caption(f"⚠️ Kline handler errors: {WS_LOADER.kline_errors}")
            if WS_LOADER.warmup_errors:
                with st.expander(f"⚠️ {len(WS_LOADER.warmup_errors)} warmup error(s)"):
                    for e in WS_LOADER.warmup_errors[-10:]:
                        st.code(e)
        else:
            st.error(f"🔴 {WS_LOADER.status}")
            if st.button("Reconnect WebSocket"):
                WS_LOADER.start()
                st.rerun()
                
        st.divider()


# ==============================================
# GLOBAL CONSTANTS
# ==============================================

# Binance API configuration
BINANCE_API_URL = "https://fapi.binance.com"
BINANCE_SPOT_API_URL = "https://api.binance.com"

# Timezone configuration
IST = ZoneInfo("Asia/Kolkata")
UTC = ZoneInfo("UTC")

# Timeframe mapping to Binance intervals
TIMEFRAME_MAP = {
    "1H": "1h",
    "4H": "4h",
    "Daily": "1d",
    "Weekly": "1w",
    "Monthly": "1M"
}

# ==============================================
# CACHED API FUNCTIONS
# ==============================================

@st.cache_data(ttl=3600)
def get_perpetual_symbols():
    """Fetch all USDT perpetual trading pairs from Binance"""
    # Wait for the background proxy selector if we are in a blocked region
    for _ in range(20):
        if PROXY_NEEDED is None or (PROXY_NEEDED and not PROXY_READY):
            time.sleep(0.5)
        else:
            break
            
    try:
        response = requests.get(f"{BINANCE_API_URL}/fapi/v1/exchangeInfo", timeout=10)
        
        # Add debug prints here
        print(f"DEBUG: Binance API Status Code: {response.status_code}")
        print(f"DEBUG: Binance API Response Text: {response.text[:500]}...") # Print first 500 chars to avoid flooding
        
        data = response.json()
        print(f"DEBUG: Type of data from response.json(): {type(data)}")
        if isinstance(data, dict):
            print(f"DEBUG: Keys in data: {data.keys()}")
        
        perpetual_symbols = []
        for symbol_info in data['symbols']:
            if (symbol_info['contractType'] == 'PERPETUAL' and 
                symbol_info['quoteAsset'] == 'USDT' and
                symbol_info['status'] == 'TRADING'):
                perpetual_symbols.append(symbol_info['symbol'])
        
        return sorted(perpetual_symbols)
    except Exception as e:
        st.error(f"Error fetching symbols: {e}")
        return []

@st.cache_data(ttl=3600)
def get_spot_symbols():
    """Fetch all USDT spot trading pairs from Binance"""
    try:
        response = requests.get(f"{BINANCE_SPOT_API_URL}/api/v3/exchangeInfo", timeout=10)
        data = response.json()
        
        spot_symbols = []
        for symbol_info in data['symbols']:
            if (symbol_info['quoteAsset'] == 'USDT' and
                symbol_info['status'] == 'TRADING' and
                symbol_info['isSpotTradingAllowed']):
                spot_symbols.append(symbol_info['symbol'])
        
        return set(spot_symbols)
    except Exception as e:
        return set()

@st.cache_data(ttl=300)
def get_top_volume_symbols(limit=500):
    WS_LOADER = get_ws_loader()
    symbols_data = []
    
    # Use WS loader if available
    if WS_LOADER.initialized and WS_LOADER.data:
        volume_data = []
        for symbol, ticker in WS_LOADER.data.items():
            if symbol.endswith("USDT"):
                volume_data.append({"symbol": symbol, "volume": ticker["quoteVolume"], "priceChangePercent": ticker.get("priceChangePercent", 0)})
        if volume_data:
            volume_data.sort(key=lambda x: x["volume"], reverse=True)
            return volume_data[:limit]
    """Get symbols sorted by 24h USDT volume"""
    try:
        response = requests.get(f"{BINANCE_API_URL}/fapi/v1/ticker/24hr", timeout=10)
        data = response.json()
        
        volume_data = []
        for ticker in data:
            symbol = ticker['symbol']
            if symbol.endswith('USDT'):
                quote_volume = float(ticker.get('quoteVolume', 0))
                volume_data.append({
                    'symbol': symbol,
                    'volume': quote_volume,
                    'priceChangePercent': float(ticker.get('priceChangePercent', 0))
                })
        
        volume_data.sort(key=lambda x: x['volume'], reverse=True)
        return volume_data
    except Exception as e:
        return []

@st.cache_data(ttl=300)
def get_top_volume_symbols_list(limit=500):
    """Get just the symbols sorted by volume"""
    volume_data = get_top_volume_symbols(limit)
    return [item['symbol'] for item in volume_data[:limit]]

@st.cache_data(ttl=60)
def get_open_interest(symbol):
    """Fetch current open interest for a symbol"""
    try:
        params = {'symbol': symbol}
        response = requests.get(f"{BINANCE_API_URL}/fapi/v1/openInterest", params=params, timeout=5)
        data = response.json()
        return float(data.get('openInterest', 0))
    except Exception as e:
        return None

# ==============================================
# TIMEFRAME-BASED OPEN INTEREST CALCULATION
# ==============================================

@st.cache_data(ttl=60)
def get_historical_open_interest(symbol, timeframe="Daily", limit=2):
    """
    Fetch historical open interest for comparison BASED ON SELECTED TIMEFRAME
    timeframe: Monthly, Weekly, Daily, 4H, 1H
    """
    try:
        # Map your timeframe to Binance OI period
        period_map = {
            "Monthly": "1M",   # Compare this month vs last month
            "Weekly": "1w",    # Compare this week vs last week
            "Daily": "1d",     # Compare today vs yesterday
            "4H": "4h",        # Compare this 4h vs previous 4h
            "1H": "1h"         # Compare this hour vs previous hour
        }
        
        # Get the appropriate Binance period based on selected timeframe
        binance_period = period_map.get(timeframe, "1d")
        
        params = {
            'symbol': symbol,
            'period': binance_period,
            'limit': limit
        }
        
        response = requests.get(f"{BINANCE_API_URL}/futures/data/openInterestHist", params=params, timeout=5)
        data = response.json()
        
        if data and len(data) >= 2:
            # Get OI values for current and previous period
            current_oi = float(data[-1]['sumOpenInterest'])   # Current period
            previous_oi = float(data[-2]['sumOpenInterest'])  # Previous period
            
            if previous_oi > 0:
                oi_change = ((current_oi - previous_oi) / previous_oi) * 100
                
                # Return both change and period info
                return {
                    'change_percent': round(oi_change, 2),
                    'current_oi': current_oi,
                    'previous_oi': previous_oi,
                    'period': binance_period,
                    'timeframe': timeframe,
                    'comparison_text': f"{timeframe} ({binance_period})"
                }
        return {
            'change_percent': 0,
            'current_oi': 0,
            'previous_oi': 0,
            'period': binance_period,
            'timeframe': timeframe,
            'comparison_text': f"{timeframe} ({binance_period})"
        }
    except Exception as e:
        return {
            'change_percent': 0,
            'current_oi': 0,
            'previous_oi': 0,
            'period': 'N/A',
            'timeframe': timeframe,
            'comparison_text': f"{timeframe} (Error)"
        }

# ==============================================
# HELPER FUNCTIONS
# ==============================================

def format_large_number(num):
    """Format large numbers with K, M, B suffixes"""
    if num >= 1_000_000_000:
        return f"{num/1_000_000_000:.2f}B"
    elif num >= 1_000_000:
        return f"{num/1_000_000:.2f}M"
    elif num >= 1_000:
        return f"{num/1_000:.2f}K"
    else:
        return f"{num:.2f}"

def format_volume_ratio(ratio):
    """Format volume ratio as 1:X format"""
    if ratio == float('inf'):
        return "∞:1"
    elif ratio >= 1:
        return f"1:{ratio:.1f}"
    else:
        return f"{1/ratio:.1f}:1"

def calculate_volume_percentile(current_volume, historical_volumes):
    """Calculate where current volume stands compared to history"""
    if not historical_volumes:
        return 0
    sorted_volumes = sorted(historical_volumes)
    count_less = sum(1 for v in sorted_volumes if v < current_volume)
    percentile = (count_less / len(sorted_volumes)) * 100
    return percentile

def get_candle_volumes(kline_data):
    """Extract both trade volume and USDT volume from kline"""
    if not kline_data or len(kline_data) < 8:
        return 0, 0
    trade_volume = float(kline_data[5])
    usdt_volume = float(kline_data[7])
    return trade_volume, usdt_volume

def check_spot_match_fast(symbol):
    """Fast spot market check with caching"""
    try:
        spot_symbols = get_spot_symbols()
        return symbol in spot_symbols
    except:
        return False

def get_historical_klines(symbol, interval, start_time=None, end_time=None, limit=500):
    if not start_time and not end_time and limit <= 100:
        _loader = get_ws_loader()
        buf = _loader.get_klines(symbol, interval)
        if buf: return buf[-limit:]
    """Fetch historical kline data for a specific time range"""
    try:
        params = {
            'symbol': symbol,
            'interval': interval,
            'limit': limit
        }
        
        if start_time:
            if isinstance(start_time, datetime):
                if start_time.tzinfo:
                    start_time_utc = start_time.astimezone(UTC)
                else:
                    start_time_utc = start_time.replace(tzinfo=UTC)
                params['startTime'] = int(start_time_utc.timestamp() * 1000)
            else:
                params['startTime'] = start_time
                
        if end_time:
            if isinstance(end_time, datetime):
                if end_time.tzinfo:
                    end_time_utc = end_time.astimezone(UTC)
                else:
                    end_time_utc = end_time.replace(tzinfo=UTC)
                params['endTime'] = int(end_time_utc.timestamp() * 1000)
            else:
                params['endTime'] = end_time
        
        response = requests.get(f"{BINANCE_API_URL}/fapi/v1/klines", params=params, timeout=5)
        data = response.json()
        
        if isinstance(data, dict) and 'code' in data:
            return None
        
        return data
    except Exception as e:
        return None

def get_btc_daily_volume(date):
    """Get BTC daily volume for comparison"""
    try:
        btc_symbol = "BTCUSDT"
        start_date = datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=IST)
        end_date = datetime(date.year, date.month, date.day, 23, 59, 59, tzinfo=IST)
        
        klines = get_historical_klines(btc_symbol, "1d", start_date, end_date, limit=1)
        
        if klines and len(klines) > 0:
            _, usdt_volume = get_candle_volumes(klines[0])
            return usdt_volume
        return 0
    except:
        return 0

def get_btc_historical_volumes(days=30):
    """Get BTC historical volumes for the last N days"""
    try:
        end_date = datetime.now(IST)
        start_date = end_date - timedelta(days=days)
        
        klines = get_historical_klines("BTCUSDT", "1d", start_date, end_date, limit=days + 5)
        
        btc_volumes = {}
        if klines:
            for kline in klines:
                try:
                    timestamp = kline[0]
                    candle_date = datetime.fromtimestamp(timestamp / 1000, tz=UTC)
                    candle_date_ist = candle_date.astimezone(IST)
                    date_str = candle_date_ist.strftime('%Y-%m-%d')
                    _, usdt_volume = get_candle_volumes(kline)
                    btc_volumes[date_str] = usdt_volume
                except:
                    continue
        return btc_volumes
    except:
        return {}

# ==============================================
# BREAKOUT DETECTION FUNCTIONS (Original)
# ==============================================

def fetch_single_breakout_worker(symbol, timeframe):
    """Worker function for parallel breakout scanning with Open Interest"""
    try:
        # Determine number of candles needed based on timeframe
        if timeframe == "Monthly":
            limit = 5
        elif timeframe == "Weekly":
            limit = 10
        elif timeframe == "Daily":
            limit = 30
        else:  # 4H, 1H
            limit = 50
            
        params = {
            'symbol': symbol,
            'interval': TIMEFRAME_MAP[timeframe],
            'limit': limit
        }
        
        response = requests.get(f"{BINANCE_API_URL}/fapi/v1/klines", params=params, timeout=5)
        data = response.json()
        
        if not data or len(data) < 3:
            return None
            
        # Get last two COMPLETED candles (not current)
        last_candle = data[-2]
        prev_candle = data[-3]
        
        # Extract OHLCV
        last_open = float(last_candle[1])
        last_high = float(last_candle[2])
        last_low = float(last_candle[3])
        last_close = float(last_candle[4])
        last_volume = float(last_candle[5])
        last_usdt_volume = float(last_candle[7])
        
        prev_high = float(prev_candle[2])
        prev_low = float(prev_candle[3])
        prev_volume = float(prev_candle[5])
        prev_usdt_volume = float(prev_candle[7])
        
        # Check for bullish breakout
        bullish_breakout = (last_close > prev_high and last_high > prev_high)
        
        # Check for bearish breakout
        bearish_breakout = (last_close < prev_low and last_low < prev_low)
        
        if not bullish_breakout and not bearish_breakout:
            return None
            
        # Calculate volume ratio
        if prev_volume > 0:
            vol_ratio = last_volume / prev_volume
            if vol_ratio < 1:
                return None
            vol_ratio_str = f"1:{int(vol_ratio)}"
            vol_ratio_num = int(vol_ratio)
        else:
            return None
        
        # Get Open Interest change - NOW TIMEFRAME AWARE!
        oi_data = get_historical_open_interest(symbol, timeframe)
        oi_change = oi_data['change_percent']
        
        result = {
            'Coin Name': symbol,
            'USDT Volume Ratio': f"1:{int(last_usdt_volume / prev_usdt_volume)}" if prev_usdt_volume > 0 else "1:1",
            'Coin volume Ratio': vol_ratio_str,
            'Volume_Ratio_Num': vol_ratio_num,
            'Open_Interest_change': oi_change,
            'OI_Period': oi_data['comparison_text']
        }
        
        if bullish_breakout:
            tested_low = last_low <= prev_low
            result['Breakout Type'] = 'Trap' if tested_low else 'Normal'
            result['Direction'] = 'Bullish'
            result['Open to High Change %'] = round(((last_high - last_open) / last_open) * 100, 2)
            result['Open to close Change %'] = round(((last_close - last_open) / last_open) * 100, 2)
            result['Open to Low Change %'] = '-'
        else:
            tested_high = last_high >= prev_high
            result['Breakout Type'] = 'Trap' if tested_high else 'Normal'
            result['Direction'] = 'Bearish'
            result['Open to Low Change %'] = round(((last_low - last_open) / last_open) * 100, 2)
            result['Open to close Change %'] = round(((last_close - last_open) / last_open) * 100, 2)
            result['Open to High Change %'] = '-'
        
        result['Spot Match'] = 'Yes' if check_spot_match_fast(symbol) else 'No'
        candle_time = datetime.fromtimestamp(last_candle[0] / 1000, tz=UTC)
        result['Candle_Date'] = candle_time.astimezone(IST).strftime('%Y-%m-%d %H:%M')
        
        return result
        
    except Exception as e:
        return None

def scan_breakouts_parallel(symbols, timeframe, max_workers=25):
    """Parallel breakout scanning"""
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {
            executor.submit(fetch_single_breakout_worker, symbol, timeframe): symbol 
            for symbol in symbols
        }
        
        completed = 0
        total = len(symbols)
        
        for future in as_completed(future_to_symbol):
            completed += 1
            if completed % 10 == 0 or completed == total:
                progress_bar.progress(completed / total)
                status_text.text(f"Scanning {completed}/{total} pairs... Found: {len(results)} breakouts")
            
            try:
                result = future.result(timeout=5)
                if result:
                    results.append(result)
            except Exception:
                pass
    
    progress_bar.empty()
    status_text.empty()
    return results

# ==============================================
# CLEANED: TIMEFRAME BREAKOUT SCANNER (YOUR LOGIC)
# ==============================================

def fetch_single_timeframe_breakout_worker(symbol, past_timeframe, current_timeframe, scan_mode="both", reference_time=None):
    """
    Worker function for timeframe-based breakout scanning
    
    Logic:
    - Past Timeframe: Monthly, Weekly, Daily (OPEN price)
    - Current Timeframe: Weekly, Daily, 4H (CLOSE price) - ONLY MOST RECENT candle
    
    Bullish: Past candle is bearish (close < open) AND current close > past open
    Bearish: Past candle is bullish (close > open) AND current close < past open
    """
    try:
        # Map timeframes to Binance intervals
        interval_map = {
            "Monthly": "1M",
            "Weekly": "1w", 
            "Daily": "1d",
            "4H": "4h"
        }
        
        # Get past timeframe data (need at least 2 candles to get previous completed candle)
        past_interval = interval_map.get(past_timeframe)
        if not past_interval:
            return None
            
        # Determine how many candles needed based on past timeframe
        if past_timeframe == "Monthly":
            past_limit = 3
        elif past_timeframe == "Weekly":
            past_limit = 5
        else:  # Daily
            past_limit = 10
            
        if reference_time:
            # Backtest mode: evaluate candles completed before reference_time
            past_data = get_historical_klines(symbol, past_interval, end_time=reference_time, limit=past_limit)
        else:
            params_past = {
                'symbol': symbol,
                'interval': past_interval,
                'limit': past_limit
            }
            response_past = requests.get(f"{BINANCE_API_URL}/fapi/v1/klines", params=params_past, timeout=5)
            past_data = response_past.json()
        
        if not past_data or len(past_data) < 2:
            return None
            
        # Get the LAST COMPLETED past candle
        past_candle = past_data[-2]
        
        past_open = float(past_candle[1])
        past_close = float(past_candle[4])
        past_high = float(past_candle[2])
        past_low = float(past_candle[3])
        
        # Determine if past candle is bullish or bearish
        past_is_bullish = past_close > past_open
        past_is_bearish = past_close < past_open
        past_body_size = abs(past_close - past_open)
        past_body_percent = (past_body_size / past_open) * 100 if past_open > 0 else 0
        
        # Get current timeframe data
        current_interval = interval_map.get(current_timeframe)
        if not current_interval:
            return None
            
        # For current timeframe, we only need the last 2 candles
        if current_timeframe == "Monthly":
            current_limit = 3
        elif current_timeframe == "Weekly":
            current_limit = 5
        elif current_timeframe == "Daily":
            current_limit = 10
        else:  # 4H
            current_limit = 25
            
        if reference_time:
            # Backtest mode: current timeframe candle is the latest completed before reference_time
            current_data = get_historical_klines(symbol, current_interval, end_time=reference_time, limit=current_limit)
        else:
            params_current = {
                'symbol': symbol,
                'interval': current_interval,
                'limit': current_limit
            }
            response_current = requests.get(f"{BINANCE_API_URL}/fapi/v1/klines", params=params_current, timeout=5)
            current_data = response_current.json()
        
        if not current_data or len(current_data) < 3:
            return None
            
        # IMPORTANT: ONLY check the LAST COMPLETED current candle
        # This ensures we only show breakouts from the most recent candle
        current_candle = current_data[-2]
        prev_current_candle = current_data[-3]
        
        current_close = float(current_candle[4])
        prev_current_close = float(prev_current_candle[4])
        current_volume = float(current_candle[5])
        current_usdt_volume = float(current_candle[7])
        
        # Candle times for display
        past_candle_time = datetime.fromtimestamp(past_candle[0] / 1000, tz=UTC)
        current_candle_time = datetime.fromtimestamp(current_candle[0] / 1000, tz=UTC)
        
        # Initialize result dictionary
        result = {
            'Coin Name': symbol,
            'Past Timeframe': past_timeframe,
            'Current Timeframe': current_timeframe,
            'Past Open': round(past_open, 4),
            'Past Close': round(past_close, 4),
            'Past Candle Type': 'Bullish' if past_is_bullish else 'Bearish' if past_is_bearish else 'Doji',
            'Past Body %': round(past_body_percent, 2),
            'Current Close': round(current_close, 4),
            'Price Difference': round(current_close - past_open, 4),
            'Diff %': round(((current_close - past_open) / past_open) * 100, 2),
            'Volume (USDT)': format_large_number(current_usdt_volume),
            'Past Candle Date': past_candle_time.astimezone(IST).strftime('%Y-%m-%d'),
            'Current Candle Date': current_candle_time.astimezone(IST).strftime('%Y-%m-%d %I:%M %p'),
            'Current Candle Time': current_candle_time.astimezone(IST).strftime('%I:%M %p'),
            'Spot Match': 'Yes' if check_spot_match_fast(symbol) else 'No',
            'Scan Mode': 'Backtest' if reference_time else 'Live'
        }
        
        # Get Open Interest data based on current timeframe (just for display)
        oi_data = get_historical_open_interest(symbol, current_timeframe)
        result['OI Change %'] = oi_data['change_percent']
        result['OI Period'] = oi_data['comparison_text']
        
        # Check conditions based on scan mode
        is_bullish_signal = False
        is_bearish_signal = False
        
        # Fresh bullish breakout:
        # 1) Past candle bearish
        # 2) Current close above past open
        # 3) Previous current candle close was at/below past open (first crossover now)
        if past_is_bearish and current_close > past_open and prev_current_close <= past_open:
            is_bullish_signal = True
            result['Signal Type'] = 'BULLISH 📈'
            result['Condition'] = "Past Bearish | Fresh Close Cross Above Past Open"
            
        # Fresh bearish breakout:
        # 1) Past candle bullish
        # 2) Current close below past open
        # 3) Previous current candle close was at/above past open (first crossover now)
        elif past_is_bullish and current_close < past_open and prev_current_close >= past_open:
            is_bearish_signal = True
            result['Signal Type'] = 'BEARISH 📉'
            result['Condition'] = "Past Bullish | Fresh Close Cross Below Past Open"
        
        # No signal
        else:
            return None
            
        # Filter based on scan mode
        if scan_mode == "bullish" and not is_bullish_signal:
            return None
        if scan_mode == "bearish" and not is_bearish_signal:
            return None
            
        return result
        
    except Exception as e:
        return None

def scan_timeframe_breakouts_parallel(symbols, past_timeframe, current_timeframe, scan_mode="both", max_workers=25, reference_time=None):
    """Parallel timeframe-based breakout scanning"""
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {
            executor.submit(
                fetch_single_timeframe_breakout_worker,
                symbol,
                past_timeframe,
                current_timeframe,
                scan_mode,
                reference_time
            ): symbol 
            for symbol in symbols
        }
        
        completed = 0
        total = len(symbols)
        
        for future in as_completed(future_to_symbol):
            completed += 1
            if completed % 10 == 0 or completed == total:
                progress_bar.progress(completed / total)
                status_text.text(f"Scanning {completed}/{total} pairs... Found: {len(results)} breakouts")
            
            try:
                result = future.result(timeout=5)
                if result:
                    results.append(result)
            except Exception:
                pass
    
    progress_bar.empty()
    status_text.empty()
    return results

# ==============================================
# CLEANED TAB: TIMEFRAME BREAKOUT SCANNER (NO VOLUME RATIO, NO ADVANCED FILTERS)
# ==============================================

def render_timeframe_breakout_scanner():
    """Render the cleaned timeframe-based breakout scanner tab"""
    
    st.header("🎯 Timeframe Breakout Scanner")
    st.markdown("""
    **Scan based on Past Timeframe OPEN vs Current Timeframe CLOSE**
    
    ### 📊 Logic:
    - **Bullish Signal**: Past candle is **BEARISH** (Close < Open) AND Current Close > Past Open
    - **Bearish Signal**: Past candle is **BULLISH** (Close > Open) AND Current Close < Past Open
    
    ### 🔍 How it works:
    1. Select **Past Timeframe** - uses the **OPEN** price of last completed candle
    2. Select **Current Timeframe** - uses the **CLOSE** price of the **MOST RECENT** completed candle
    3. Scanner checks condition across all Binance Futures pairs
    4. Results show ONLY breakouts from the latest candle
    """)
    
    # Initialize defaults once for cleaner preset handling
    if "past_tf" not in st.session_state:
        st.session_state["past_tf"] = "Daily"
    if "current_tf" not in st.session_state:
        st.session_state["current_tf"] = "Daily"
    if "scan_mode_tf" not in st.session_state:
        st.session_state["scan_mode_tf"] = "both"
    if "tf_backtest" not in st.session_state:
        st.session_state["tf_backtest"] = False
    if "tf_backtest_date" not in st.session_state:
        st.session_state["tf_backtest_date"] = datetime.now(IST).date() - timedelta(days=1)

    st.subheader("⚙️ Scan Setup")

    preset_col1, preset_col2, preset_col3 = st.columns(3)
    with preset_col1:
        if st.button("Preset: Weekly -> Daily (Fresh)", use_container_width=True, key="tf_preset_w_d"):
            st.session_state["past_tf"] = "Weekly"
            st.session_state["current_tf"] = "Daily"
            st.session_state["scan_mode_tf"] = "bullish"
            st.session_state["tf_backtest"] = False
            st.rerun()
    with preset_col2:
        if st.button("Preset: Weekly -> 4H (Fresh)", use_container_width=True, key="tf_preset_w_4h"):
            st.session_state["past_tf"] = "Weekly"
            st.session_state["current_tf"] = "4H"
            st.session_state["scan_mode_tf"] = "bullish"
            st.session_state["tf_backtest"] = False
            st.rerun()
    with preset_col3:
        if st.button("Preset: Monthly -> Daily", use_container_width=True, key="tf_preset_m_d"):
            st.session_state["past_tf"] = "Monthly"
            st.session_state["current_tf"] = "Daily"
            st.session_state["scan_mode_tf"] = "both"
            st.session_state["tf_backtest"] = False
            st.rerun()

    col1, col2, col3 = st.columns(3)
    
    with col1:
        past_timeframe = st.selectbox(
            "📅 Past Timeframe (OPEN price)",
            ["Monthly", "Weekly", "Daily"],
            index=2,
            key="past_tf",
            help="Select timeframe for the PAST candle OPEN price"
        )
    
    with col2:
        current_timeframe = st.selectbox(
            "⏰ Current Timeframe (CLOSE price)",
            ["Weekly", "Daily", "4H"],
            index=1,
            key="current_tf",
            help="Select timeframe for the CURRENT candle CLOSE price (only most recent candle)"
        )
    
    with col3:
        scan_mode = st.selectbox(
            "🎯 Scan Mode",
            ["both", "bullish", "bearish"],
            index=0,
            key="scan_mode_tf",
            format_func=lambda x: "Both Bullish & Bearish" if x == "both" else "Bullish Only" if x == "bullish" else "Bearish Only"
        )
    
    col1, col2 = st.columns(2)
    
    with col1:
        n_coins = st.number_input(
            "Number of coins to display",
            min_value=5, max_value=100, value=20, step=5,
            key="tf_n_coins"
        )
    
    with col2:
        priority_scan = st.checkbox(
            "Priority Scan (High volume first)",
            value=True,
            key="tf_priority"
        )
    
    backtest_mode = st.checkbox("Backtest Mode", value=False, key="tf_backtest")
    selected_4h_slot = None
    if backtest_mode:
        backtest_date = st.date_input(
            "Backtest Date (scan as of this date)",
            value=datetime.now(IST).date() - timedelta(days=1),
            key="tf_backtest_date"
        )
        if current_timeframe == "4H":
            # Binance 4H candles are UTC-aligned; in IST they start at 01:30, 05:30, 09:30, 13:30, 17:30, 21:30
            candle_time_map = {
                "01:30 AM - 05:30 AM": (0, 5, 30),
                "05:30 AM - 09:30 AM": (0, 9, 30),
                "09:30 AM - 01:30 PM": (0, 13, 30),
                "01:30 PM - 05:30 PM": (0, 17, 30),
                "05:30 PM - 09:30 PM": (0, 21, 30),
                "09:30 PM - 01:30 AM": (1, 1, 30),
            }
            selected_4h_slot = st.selectbox(
                "4H Candle to Check (IST, 12-hour format)",
                options=list(candle_time_map.keys()),
                index=1,
                key="tf_backtest_4h_slot"
            )
            close_day_offset, close_hour, close_minute = candle_time_map[selected_4h_slot]
            slot_close_date = backtest_date + timedelta(days=close_day_offset)
            reference_time = datetime.combine(slot_close_date, datetime.min.time(), tzinfo=IST) + timedelta(
                hours=close_hour,
                minutes=close_minute
            )
            st.info(
                f"🧪 Backtest mode: scanning **{selected_4h_slot}** candle on "
                f"**{backtest_date.strftime('%Y-%m-%d')}** ({current_timeframe} close vs {past_timeframe} open)"
            )
        else:
            reference_time = datetime.combine(backtest_date, datetime.min.time(), tzinfo=IST)
            st.info(
                f"🧪 Backtest mode: scanning candles completed before **{backtest_date.strftime('%Y-%m-%d')}** "
                f"({current_timeframe} close vs {past_timeframe} open)"
            )
    else:
        reference_time = None
        st.info(f"🔍 Live mode: scanning ONLY the most recent completed **{current_timeframe}** candle")

    # Compact scan summary bar
    if backtest_mode:
        slot_text = f" | Slot: {selected_4h_slot}" if selected_4h_slot else ""
        st.success(
            f"Backtest | {past_timeframe} Open vs {current_timeframe} Close | "
            f"Date: {backtest_date.strftime('%Y-%m-%d')}{slot_text} | "
            f"Mode: {'Bullish' if scan_mode == 'bullish' else 'Bearish' if scan_mode == 'bearish' else 'Both'}"
        )
    else:
        st.success(
            f"Live | {past_timeframe} Open vs {current_timeframe} Close | "
            f"Mode: {'Bullish' if scan_mode == 'bullish' else 'Bearish' if scan_mode == 'bearish' else 'Both'}"
        )
    
    parallel_workers = st.slider(
        "Parallel Workers (Faster = More API calls)",
        min_value=5, max_value=50, value=25,
        key="tf_workers"
    )
    
    scan_btn_label = "🧪 Start Backtest Scan" if backtest_mode else "🔍 Start Live Scan"
    can_scan = not (backtest_mode and not backtest_date)
    if st.button(scan_btn_label, type="primary", use_container_width=True, key="tf_scan_btn", disabled=not can_scan):
        st.subheader("📊 Results")
        start_time = time.time()
        
        with st.spinner("Loading all perpetual pairs..."):
            all_symbols = get_perpetual_symbols()
            if not all_symbols:
                st.error("Failed to fetch perpetual pairs.")
                st.stop()
            
            if priority_scan:
                top_volume = get_top_volume_symbols_list(limit=500)
                symbols_to_scan = [s for s in top_volume if s in all_symbols]
                remaining = [s for s in all_symbols if s not in symbols_to_scan]
                symbols_to_scan.extend(remaining)
            else:
                symbols_to_scan = all_symbols
            
            st.success(f"✅ Loaded {len(all_symbols)} perpetual pairs")
            scan_label = "backtest candles" if backtest_mode else "most recent candle only"
            st.info(
                f"⚡ Scanning {past_timeframe} OPEN vs {current_timeframe} CLOSE "
                f"({scan_label}) with {parallel_workers} parallel workers..."
            )
        
        # Run the scan
        results = scan_timeframe_breakouts_parallel(
            symbols_to_scan, 
            past_timeframe, 
            current_timeframe, 
            scan_mode,
            parallel_workers,
            reference_time
        )
        
        if results:
            df_results = pd.DataFrame(results)
            
            # Sort by difference percentage
            df_results = df_results.sort_values('Diff %', ascending=False).head(n_coins)
            
            elapsed_time = time.time() - start_time
            
            if len(df_results) > 0:
                st.success(f"✅ Found {len(df_results)} breakout signals in {elapsed_time:.1f} seconds!")
                
                # Display results in a nice table
                display_data = []
                for idx, row in df_results.iterrows():
                    signal_emoji = "🟢" if "BULLISH" in row['Signal Type'] else "🔴"
                    
                    display_data.append({
                        'S.No': len(display_data) + 1,
                        'Coin': row['Coin Name'],
                        'Signal': f"{signal_emoji} {row['Signal Type']}",
                        'Past TF': row['Past Timeframe'],
                        'Current TF': row['Current Timeframe'],
                        'Past Open': row['Past Open'],
                        'Current Close': row['Current Close'],
                        'Diff %': f"{row['Diff %']:.2f}%",
                        'Volume': row['Volume (USDT)'],
                        'OI Change': f"{row['OI Change %']:.2f}%",
                        'Past Candle': f"{row['Past Candle Type']} ({row['Past Body %']:.1f}%)",
                        'Candle Time': row['Current Candle Time'],
                        'Spot': row['Spot Match']
                    })
                
                display_df = pd.DataFrame(display_data)
                
                # Color coding for signals
                def color_signals(val):
                    if "BULLISH" in str(val):
                        return 'color: green; font-weight: bold'
                    elif "BEARISH" in str(val):
                        return 'color: red; font-weight: bold'
                    return ''
                
                def color_oi(val):
                    if isinstance(val, str) and '%' in val:
                        try:
                            num = float(val.replace('%', ''))
                            if num > 20:
                                return 'color: darkgreen; font-weight: bold'
                            elif num > 10:
                                return 'color: green'
                            elif num > 5:
                                return 'color: lightgreen'
                            elif num < -20:
                                return 'color: darkred; font-weight: bold'
                            elif num < -10:
                                return 'color: red'
                            elif num < -5:
                                return 'color: lightcoral'
                        except:
                            pass
                    return ''
                
                tab_all, tab_bull, tab_bear = st.tabs(["All Signals", "Bullish", "Bearish"])

                with tab_all:
                    st.dataframe(
                        display_df.style.map(color_signals, subset=['Signal']).map(color_oi, subset=['OI Change']),
                        use_container_width=True,
                        hide_index=True
                    )

                with tab_bull:
                    bull_df = display_df[display_df['Signal'].str.contains('BULLISH', na=False)].copy()
                    if bull_df.empty:
                        st.info("No bullish signals in this scan.")
                    else:
                        st.dataframe(
                            bull_df.style.map(color_signals, subset=['Signal']).map(color_oi, subset=['OI Change']),
                            use_container_width=True,
                            hide_index=True
                        )

                with tab_bear:
                    bear_df = display_df[display_df['Signal'].str.contains('BEARISH', na=False)].copy()
                    if bear_df.empty:
                        st.info("No bearish signals in this scan.")
                    else:
                        st.dataframe(
                            bear_df.style.map(color_signals, subset=['Signal']).map(color_oi, subset=['OI Change']),
                            use_container_width=True,
                            hide_index=True
                        )
                
                # Statistics
                st.subheader("📊 Scan Statistics")
                col1, col2, col3, col4 = st.columns(4)
                
                bullish_count = len(df_results[df_results['Signal Type'].str.contains('BULLISH', na=False)])
                bearish_count = len(df_results[df_results['Signal Type'].str.contains('BEARISH', na=False)])
                
                with col1:
                    st.metric("Total Signals", len(df_results))
                with col2:
                    st.metric("Bullish Signals", bullish_count)
                with col3:
                    st.metric("Bearish Signals", bearish_count)
                with col4:
                    avg_diff = df_results['Diff %'].mean()
                    st.metric("Avg Diff %", f"{avg_diff:.2f}%")
                
                # Download button
                csv = display_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Breakout Results (CSV)",
                    data=csv,
                    file_name=f"timeframe_breakout_{past_timeframe}_vs_{current_timeframe}_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
                
                # Show detailed view in expander
                with st.expander("🔍 View Detailed Data"):
                    detail_cols = ['Coin', 'Signal', 'Past Candle', 'Past Open', 'Current Close', 
                                 'Diff %', 'Volume', 'OI Change', 'Candle Time']
                    st.dataframe(display_df[detail_cols], use_container_width=True, hide_index=True)
            
            else:
                st.warning("No breakout signals match your filters.")
        else:
            st.warning(f"No {past_timeframe} vs {current_timeframe} breakout signals found in the most recent candle.")

# ==============================================
# OPEN INTEREST SCANNER FUNCTIONS - TIMEFRAME AWARE
# ==============================================

def fetch_single_oi_data(symbol, timeframe):
    """Fetch Open Interest data for a single symbol - TIMEFRAME AWARE"""
    try:
        # Get OI change based on selected timeframe
        oi_data = get_historical_open_interest(symbol, timeframe, limit=2)
        oi_change = oi_data['change_percent']
        current_oi = oi_data['current_oi']
        previous_oi = oi_data['previous_oi']
        
        if current_oi == 0:
            return None
        
        # Get 24h volume and price change
        try:
            ticker_response = requests.get(f"{BINANCE_API_URL}/fapi/v1/ticker/24hr", params={'symbol': symbol}, timeout=3)
            ticker_data = ticker_response.json()
            volume_24h = float(ticker_data.get('quoteVolume', 0))
            price_change = float(ticker_data.get('priceChangePercent', 0))
        except:
            volume_24h = 0
            price_change = 0
        
        # Determine OI trend and signal based on change percentage
        if oi_change > 20:
            trend = "🚀 Strongly Increasing"
            signal = "Strong Bullish"
        elif oi_change > 10:
            trend = "📈 Increasing"
            signal = "Bullish"
        elif oi_change > 5:
            trend = "↗️ Slightly Increasing"
            signal = "Mildly Bullish"
        elif oi_change > 2:
            trend = "➡️ Moderate Increase"
            signal = "Cautiously Bullish"
        elif oi_change > 0.5:
            trend = "↗️ Minor Increase"
            signal = "Slightly Bullish"
        elif oi_change < -20:
            trend = "💥 Strongly Decreasing"
            signal = "Strong Bearish"
        elif oi_change < -10:
            trend = "📉 Decreasing"
            signal = "Bearish"
        elif oi_change < -5:
            trend = "↘️ Slightly Decreasing"
            signal = "Mildly Bearish"
        elif oi_change < -2:
            trend = "➡️ Moderate Decrease"
            signal = "Cautiously Bearish"
        elif oi_change < -0.5:
            trend = "↘️ Minor Decrease"
            signal = "Slightly Bearish"
        else:
            trend = "➡️ Neutral"
            signal = "Neutral"
        
        # Add volume trend based on price and OI
        if price_change > 2 and oi_change > 10:
            signal = "🚀 Strong Bullish (Price + OI)"
        elif price_change < -2 and oi_change < -10:
            signal = "💀 Strong Bearish (Price + OI)"
        elif price_change > 2 and oi_change < -10:
            signal = "⚠️ Warning (Price Up, OI Down)"
        elif price_change < -2 and oi_change > 10:
            signal = "⚠️ Warning (Price Down, OI Up)"
        
        return {
            'Coin Name': symbol,
            'OI_Change_%': oi_change,
            'Current_OI': current_oi,
            'Previous_OI': previous_oi,
            'OI_Trend': trend,
            'Signal': signal,
            '24h_Volume': volume_24h,
            '24h_Volume_Display': format_large_number(volume_24h),
            'Price_Change_%': round(price_change, 2),
            'OI_Period': oi_data['comparison_text'],
            'Timeframe': timeframe
        }
        
    except Exception as e:
        return None

def scan_open_interest_parallel(symbols, timeframe, max_workers=25, min_oi_change=1.0, oi_direction="Both"):
    """Parallel Open Interest scanner - TIMEFRAME AWARE"""
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {
            executor.submit(fetch_single_oi_data, symbol, timeframe): symbol 
            for symbol in symbols
        }
        
        completed = 0
        total = len(symbols)
        
        for future in as_completed(future_to_symbol):
            completed += 1
            if completed % 10 == 0 or completed == total:
                progress_bar.progress(completed / total)
                status_text.text(f"Scanning OI {completed}/{total} pairs... Found: {len(results)} OI changes")
            
            try:
                result = future.result(timeout=5)
                if result:
                    oi_change = result['OI_Change_%']
                    
                    # Apply filters
                    if oi_direction == "Positive (Increasing)" and oi_change <= 0:
                        continue
                    if oi_direction == "Negative (Decreasing)" and oi_change >= 0:
                        continue
                    if abs(oi_change) < min_oi_change:
                        continue
                    
                    results.append(result)
            except Exception:
                pass
    
    progress_bar.empty()
    status_text.empty()
    return results

# ==============================================
# TAB 1: BREAKOUT SCANNER (WITH TIMEFRAME-BASED OI)
# ==============================================

def render_breakout_scanner():
    """Render Tab 1: Breakout Scanner with Timeframe-Based Open Interest Scanner"""
    st.header("🚀 Breakout Scanner")
    st.markdown("""
    **Fast parallel scanner for Binance perpetual breakouts & Open Interest**
    - 🔍 **Breakout Detection**: Find bullish/bearish breakouts with volume confirmation
    - 📊 **Open Interest Scanner**: Find coins with largest OI changes based on your selected timeframe
    - 🎯 **Timeframe-Aware OI**: Compares OI for the SAME timeframe you select (Monthly, Weekly, Daily, 4H, 1H)
    - ⚡ **Ultra-fast parallel processing** (500+ coins in seconds)
    """)
    
    # ==========================================
    # SCAN MODE SELECTION
    # ==========================================
    scan_mode = st.radio(
        "Select Scan Mode:",
        ["🚀 Breakout Detection", "📊 Open Interest Scanner"],
        horizontal=True,
        key="scan_mode_main"
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        timeframe = st.selectbox(
            "Timeframe",
            ["Monthly", "Weekly", "Daily", "4H", "1H"],
            index=2,  # Default to Daily
            key="breakout_tf_main",
            help="Select timeframe for analysis. OI will compare this period vs previous period"
        )
        
        n_coins = st.number_input(
            "Number of coins to display",
            min_value=5, max_value=100, value=20, step=5,
            key="breakout_n_main"
        )
        
        priority_scan = st.checkbox(
            "Priority Scan (High volume first)",
            value=True,
            key="breakout_priority_main"
        )
    
    with col2:
        if scan_mode == "🚀 Breakout Detection":
            min_vol_ratio = st.slider(
                "Minimum Volume Ratio",
                min_value=1, max_value=10, value=2,
                key="breakout_vol_main"
            )
            
            direction_filter = st.multiselect(
                "Breakout Direction",
                ["Bullish", "Bearish"],
                default=["Bullish", "Bearish"],
                key="breakout_dir_main"
            )
            
            breakout_type_filter = st.multiselect(
                "Breakout Type",
                ["Normal", "Trap"],
                default=["Normal", "Trap"],
                key="breakout_type_main"
            )
            
        else:  # Open Interest Scanner
            # ==========================================
            # UPDATED: SLIDER RANGE 0.10 TO 25
            # ==========================================
            min_oi_change = st.slider(
                "Minimum OI Change %",
                min_value=0.10, max_value=25.0, value=1.0, step=0.10,
                format="%.2f%%",
                key="min_oi_main",
                help="Filter coins with OI change greater than this percentage (0.10% to 25%)"
            )
            
            oi_direction = st.radio(
                "OI Direction",
                ["Positive (Increasing)", "Negative (Decreasing)", "Both"],
                horizontal=True,
                key="oi_direction_main"
            )
            
            # Show timeframe info for OI
            st.info(f"📊 Comparing OI: **Current {timeframe} vs Previous {timeframe}**")
    
    parallel_workers = st.slider(
        "Parallel Workers (Faster = More API calls)",
        min_value=5, max_value=50, value=25,
        key="breakout_workers_main"
    )
    
    if st.button("🔍 Start Scan", type="primary", use_container_width=True, key="breakout_btn_main"):
        start_time = time.time()
        
        with st.spinner("Loading all perpetual pairs (500+)..."):
            all_symbols = get_perpetual_symbols()
            if not all_symbols:
                st.error("Failed to fetch perpetual pairs.")
                st.stop()
            
            if priority_scan:
                top_volume = get_top_volume_symbols_list(limit=500)
                symbols_to_scan = [s for s in top_volume if s in all_symbols]
                remaining = [s for s in all_symbols if s not in symbols_to_scan]
                symbols_to_scan.extend(remaining)
            else:
                symbols_to_scan = all_symbols
            
            st.success(f"✅ Loaded {len(all_symbols)} perpetual pairs")
            st.info(f"⚡ Scanning with {parallel_workers} parallel workers...")
        
        if scan_mode == "🚀 Breakout Detection":
            # ==========================================
            # BREAKOUT DETECTION MODE
            # ==========================================
            results = scan_breakouts_parallel(symbols_to_scan, timeframe, parallel_workers)
            
            if results:
                df_results = pd.DataFrame(results)
                df_results = df_results[df_results['Volume_Ratio_Num'] >= min_vol_ratio]
                
                if direction_filter:
                    df_results = df_results[df_results['Direction'].isin(direction_filter)]
                if breakout_type_filter:
                    df_results = df_results[df_results['Breakout Type'].isin(breakout_type_filter)]
                
                df_results = df_results.sort_values('Volume_Ratio_Num', ascending=False).head(n_coins)
                
                if len(df_results) > 0:
                    elapsed_time = time.time() - start_time
                    st.success(f"✅ Found {len(df_results)} breakouts in {elapsed_time:.1f} seconds!")
                    
                    display_data = []
                    for idx, row in df_results.iterrows():
                        data_row = {
                            'S.No': len(display_data) + 1,
                            'Coin': row['Coin Name'],
                            'USDT Vol Ratio': row['USDT Volume Ratio'],
                            'Coin Vol Ratio': row['Coin volume Ratio'],
                            'OI Change %': f"{row['Open_Interest_change']}%",
                            'OI Period': row.get('OI_Period', timeframe),
                            'Type': row['Breakout Type'],
                            'Direction': row['Direction'],
                            'Spot': row['Spot Match']
                        }
                        
                        if row['Direction'] == 'Bullish':
                            data_row['Open→High %'] = f"{row['Open to High Change %']}%"
                            data_row['Open→Close %'] = f"{row['Open to close Change %']}%"
                            data_row['Open→Low %'] = '-'
                        else:
                            data_row['Open→Low %'] = f"{row['Open to Low Change %']}%"
                            data_row['Open→Close %'] = f"{row['Open to close Change %']}%"
                            data_row['Open→High %'] = '-'
                        
                        display_data.append(data_row)
                    
                    display_df = pd.DataFrame(display_data)
                    
                    # Color coding for OI Change
                    def color_oi(val):
                        if isinstance(val, str) and '%' in val:
                            try:
                                num = float(val.replace('%', ''))
                                if num > 20:
                                    return 'color: darkgreen; font-weight: bold'
                                elif num > 10:
                                    return 'color: green'
                                elif num > 5:
                                    return 'color: lightgreen'
                                elif num > 2:
                                    return 'color: #90EE90'
                                elif num > 0.5:
                                    return 'color: #E6FFE6'
                                elif num < -20:
                                    return 'color: darkred; font-weight: bold'
                                elif num < -10:
                                    return 'color: red'
                                elif num < -5:
                                    return 'color: lightcoral'
                                elif num < -2:
                                    return 'color: #FFB6C1'
                                elif num < -0.5:
                                    return 'color: #FFE6E6'
                            except:
                                pass
                        return ''
                    
                    st.dataframe(
                        display_df.style.map(color_oi, subset=['OI Change %']),
                        use_container_width=True, 
                        hide_index=True
                    )
                    
                    col1, col2, col3, col4, col5 = st.columns(5)
                    with col1:
                        st.metric("Bullish", len(df_results[df_results['Direction'] == 'Bullish']))
                    with col2:
                        st.metric("Bearish", len(df_results[df_results['Direction'] == 'Bearish']))
                    with col3:
                        st.metric("Normal", len(df_results[df_results['Breakout Type'] == 'Normal']))
                    with col4:
                        st.metric("Trap", len(df_results[df_results['Breakout Type'] == 'Trap']))
                    with col5:
                        avg_oi = df_results['Open_Interest_change'].mean()
                        st.metric(f"Avg OI Change ({timeframe})", f"{avg_oi:.2f}%")
                    
                    csv = display_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Breakout Results (CSV)",
                        data=csv,
                        file_name=f"breakout_{timeframe}_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.warning("No breakouts match your filters.")
            else:
                st.warning("No breakouts found.")
        
        else:
            # ==========================================
            # OPEN INTEREST SCANNER MODE - TIMEFRAME AWARE
            # ==========================================
            with st.spinner(f"Scanning Open Interest changes for {len(symbols_to_scan)} coins..."):
                oi_results = scan_open_interest_parallel(
                    symbols_to_scan, 
                    timeframe, 
                    parallel_workers, 
                    min_oi_change, 
                    oi_direction
                )
                
                if oi_results:
                    df_oi = pd.DataFrame(oi_results)
                    df_oi = df_oi.sort_values('OI_Change_%', ascending=False).head(n_coins)
                    
                    elapsed_time = time.time() - start_time
                    st.success(f"✅ Found {len(df_oi)} coins with significant OI changes in {elapsed_time:.1f} seconds!")
                    st.info(f"📊 OI Comparison: **Current {timeframe} vs Previous {timeframe}**")
                    
                    # Display Open Interest results
                    display_data = []
                    for idx, row in df_oi.iterrows():
                        display_data.append({
                            'S.No': len(display_data) + 1,
                            'Coin': row['Coin Name'],
                            'OI Change %': f"{row['OI_Change_%']:.2f}%",
                            'Current OI': format_large_number(row['Current_OI']),
                            'Previous OI': format_large_number(row['Previous_OI']),
                            '24h Volume': row['24h_Volume_Display'],
                            'Price Change %': f"{row['Price_Change_%']:.2f}%",
                            'OI Trend': row['OI_Trend'],
                            'Signal': row['Signal']
                        })
                    
                    display_df = pd.DataFrame(display_data)
                    
                    # Color coding for OI Change
                    def color_oi_scan(val):
                        if isinstance(val, str) and '%' in val:
                            try:
                                num = float(val.replace('%', ''))
                                if num > 20:
                                    return 'color: darkgreen; font-weight: bold'
                                elif num > 10:
                                    return 'color: green'
                                elif num > 5:
                                    return 'color: lightgreen'
                                elif num > 2:
                                    return 'color: #90EE90'
                                elif num > 0.5:
                                    return 'color: #E6FFE6'
                                elif num < -20:
                                    return 'color: darkred; font-weight: bold'
                                elif num < -10:
                                    return 'color: red'
                                elif num < -5:
                                    return 'color: lightcoral'
                                elif num < -2:
                                    return 'color: #FFB6C1'
                                elif num < -0.5:
                                    return 'color: #FFE6E6'
                            except:
                                pass
                        return ''
                    
                    st.dataframe(
                        display_df.style.map(color_oi_scan, subset=['OI Change %']),
                        use_container_width=True, 
                        hide_index=True
                    )
                    
                    # Statistics for OI scan
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        positive_oi = len(df_oi[df_oi['OI_Change_%'] > 0])
                        st.metric(f"Positive OI ({timeframe})", positive_oi)
                    with col2:
                        negative_oi = len(df_oi[df_oi['OI_Change_%'] < 0])
                        st.metric(f"Negative OI ({timeframe})", negative_oi)
                    with col3:
                        strong_positive = len(df_oi[df_oi['OI_Change_%'] > 20])
                        st.metric("Strong Positive (>20%)", strong_positive)
                    with col4:
                        strong_negative = len(df_oi[df_oi['OI_Change_%'] < -20])
                        st.metric("Strong Negative (<-20%)", strong_negative)
                    
                    # OI Distribution Chart
                    st.subheader(f"📊 OI Change Distribution ({timeframe} Comparison)")
                    fig = go.Figure()
                    fig.add_trace(go.Histogram(
                        x=df_oi['OI_Change_%'],
                        nbinsx=30,
                        marker_color='#4CAF50',
                        name=f'OI Change % ({timeframe})'
                    ))
                    fig.update_layout(
                        title=f'Distribution of Open Interest Changes ({timeframe} vs Previous {timeframe})',
                        xaxis_title=f'OI Change % ({timeframe})',
                        yaxis_title='Number of Coins',
                        template='plotly_white',
                        height=300,
                        bargap=0.1
                    )
                    fig.add_vline(x=0, line_dash="dash", line_color="red", line_width=1)
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Trading Signals based on OI
                    st.subheader("💡 OI Trading Signals")
                    
                    tab1, tab2, tab3 = st.tabs(["🚀 Strong Bullish", "⚠️ Strong Bearish", "📊 OI-Price Divergence"])
                    
                    with tab1:
                        strong_bullish = df_oi[df_oi['OI_Change_%'] > 20].head(10)
                        if not strong_bullish.empty:
                            for _, row in strong_bullish.iterrows():
                                st.markdown(f"""
                                - **{row['Coin Name']}**: OI +{row['OI_Change_%']:.2f}% ({timeframe}) | Price: {row['Price_Change_%']:.2f}% | Vol: {row['24h_Volume_Display']}
                                """)
                        else:
                            st.info(f"No strong bullish signals found for {timeframe} timeframe")
                    
                    with tab2:
                        strong_bearish = df_oi[df_oi['OI_Change_%'] < -20].head(10)
                        if not strong_bearish.empty:
                            for _, row in strong_bearish.iterrows():
                                st.markdown(f"""
                                - **{row['Coin Name']}**: OI {row['OI_Change_%']:.2f}% ({timeframe}) | Price: {row['Price_Change_%']:.2f}% | Vol: {row['24h_Volume_Display']}
                                """)
                        else:
                            st.info(f"No strong bearish signals found for {timeframe} timeframe")
                    
                    with tab3:
                        # Price up, OI down
                        divergence1 = df_oi[(df_oi['Price_Change_%'] > 3) & (df_oi['OI_Change_%'] < -10)].head(5)
                        # Price down, OI up
                        divergence2 = df_oi[(df_oi['Price_Change_%'] < -3) & (df_oi['OI_Change_%'] > 10)].head(5)
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown("**⚠️ Price Up, OI Down (Weakness)**")
                            if not divergence1.empty:
                                for _, row in divergence1.iterrows():
                                    st.markdown(f"- {row['Coin Name']}: +{row['Price_Change_%']:.2f}% price, {row['OI_Change_%']:.2f}% OI ({timeframe})")
                            else:
                                st.info("None found")
                        
                        with col2:
                            st.markdown("**⚠️ Price Down, OI Up (Accumulation)**")
                            if not divergence2.empty:
                                for _, row in divergence2.iterrows():
                                    st.markdown(f"- {row['Coin Name']}: {row['Price_Change_%']:.2f}% price, +{row['OI_Change_%']:.2f}% OI ({timeframe})")
                            else:
                                st.info("None found")
                    
                    # Download OI scan results
                    csv = display_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download OI Scan Results (CSV)",
                        data=csv,
                        file_name=f"oi_scan_{timeframe}_{datetime.now(IST).strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                    
                else:
                    st.warning(f"No significant Open Interest changes found for {timeframe} timeframe. Try adjusting the minimum OI change filter.")

# ==============================================
# TAB 2: SMART VOLUME SCANNER (FULL 500+ COINS)
# ==============================================

def analyze_volume_spike(symbol, klines, current_index, lookback=20):
    """Analyze volume spike with proper context"""
    if len(klines) < current_index + 1 or current_index < 1:
        return None, None, None, None
    
    current_kline = klines[current_index]
    previous_kline = klines[current_index - 1]
    
    current_trade_vol, current_usdt_vol = get_candle_volumes(current_kline)
    prev_trade_vol, prev_usdt_vol = get_candle_volumes(previous_kline)
    
    if prev_usdt_vol > 0:
        usdt_volume_ratio = current_usdt_vol / prev_usdt_vol
    else:
        usdt_volume_ratio = float('inf')
    
    usdt_volumes = []
    start_idx = max(0, current_index - lookback)
    for i in range(start_idx, current_index):
        if i < len(klines):
            _, usdt_vol = get_candle_volumes(klines[i])
            usdt_volumes.append(usdt_vol)
    
    usdt_volume_percentile = calculate_volume_percentile(current_usdt_vol, usdt_volumes) if usdt_volumes else 0
    
    return usdt_volume_ratio, usdt_volume_percentile, current_usdt_vol, prev_usdt_vol

def enhanced_volume_scanner_full(timeframe="1H", min_usdt_ratio=2.0, min_usdt_percentile=80, 
                               min_usdt_volume=100000, top_n=20, backtest_date=None, 
                               specific_coins=None, priority_scan=True):
    """Enhanced scanner using USDT volume - SCANS ALL COINS"""
    
    if specific_coins:
        symbols_to_scan = specific_coins
    else:
        all_symbols = get_perpetual_symbols()
        if priority_scan:
            volume_data = get_top_volume_symbols(500)
            symbols_to_scan = [item['symbol'] for item in volume_data if item['symbol'] in all_symbols]
        else:
            symbols_to_scan = all_symbols
    
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, symbol in enumerate(symbols_to_scan):
        progress_bar.progress((i + 1) / len(symbols_to_scan))
        status_text.text(f"Scanning: {symbol} ({i+1}/{len(symbols_to_scan)})")
        
        if backtest_date:
            end_date = backtest_date
            if timeframe == "1H":
                start_date = end_date - timedelta(days=7)
            elif timeframe == "4H":
                start_date = end_date - timedelta(days=14)
            elif timeframe == "Daily":
                start_date = end_date - timedelta(days=60)
            else:
                start_date = end_date - timedelta(days=30)
            
            klines = get_historical_klines(symbol, TIMEFRAME_MAP[timeframe], start_date, end_date, limit=100)
        else:
            klines = get_historical_klines(symbol, TIMEFRAME_MAP[timeframe], limit=100)
        
        if klines and len(klines) >= 21:
            latest_index = len(klines) - 1
            usdt_ratio, usdt_percentile, usdt_volume, prev_usdt_vol = analyze_volume_spike(
                symbol, klines, latest_index, lookback=20
            )
            
            if all(v is not None for v in [usdt_ratio, usdt_percentile, usdt_volume]):
                if (usdt_ratio >= min_usdt_ratio and 
                    usdt_percentile >= min_usdt_percentile and 
                    usdt_volume >= min_usdt_volume):
                    
                    latest_kline = klines[latest_index]
                    current_price = float(latest_kline[4])
                    open_price = float(latest_kline[1])
                    price_change = ((current_price - open_price) / open_price) * 100
                    
                    candle_timestamp = latest_kline[0]
                    candle_date = datetime.fromtimestamp(candle_timestamp / 1000, tz=UTC)
                    candle_date_ist = candle_date.astimezone(IST)
                    
                    results.append({
                        'symbol': symbol,
                        'usdt_volume_ratio': usdt_ratio,
                        'usdt_volume_percentile': usdt_percentile,
                        'current_usdt_volume': usdt_volume,
                        'usdt_volume_display': format_large_number(usdt_volume),
                        'price': current_price,
                        'price_change_%': price_change,
                        'candle_date': candle_date_ist.strftime('%Y-%m-%d %H:%M'),
                        'timeframe': timeframe
                    })
        
        time.sleep(0.01)
    
    status_text.empty()
    
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values('usdt_volume_ratio', ascending=False)
        df = df.head(top_n).reset_index(drop=True)
        df.index = df.index + 1
        df = df.rename_axis('Rank').reset_index()
        return df
    else:
        return pd.DataFrame()

def render_volume_scanner():
    """Render Tab 2: Smart Volume Scanner - FULL SCAN"""
    st.header("💎 Smart Volume Scanner")
    st.markdown("**Advanced volume scanning with USDT volume analysis - SCANS ALL 500+ COINS**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        timeframe = st.selectbox(
            "Scan Timeframe:",
            options=["1H", "4H", "Daily", "Weekly"],
            index=1,
            key="volume_tf"
        )
        top_n = st.number_input("Number of results:", min_value=1, max_value=50, value=20, key="volume_n")
        priority_scan = st.checkbox("Priority Scan (High volume first)", value=True, key="volume_priority")
    
    with col2:
        min_usdt_ratio = st.slider("Min USDT Volume Ratio:", 1.0, 10.0, 2.0, 0.1, key="volume_ratio")
        min_usdt_percentile = st.slider("Min Volume Percentile:", 50, 99, 80, 1, key="volume_percentile")
    
    min_usdt_volume = st.slider("Min USDT Volume ($):", 1000, 1000000, 100000, 10000, key="volume_min")
    
    backtest_mode = st.checkbox("Backtest Mode", value=False, key="volume_backtest")
    
    if backtest_mode:
        backtest_date = st.date_input("Backtest Date:", value=datetime.now(IST).date() - timedelta(days=7))
        backtest_date_dt = datetime.combine(backtest_date, datetime.min.time(), tzinfo=IST)
    else:
        backtest_date_dt = None
    
    if st.button("🚀 Run Smart Volume Scan (Full 500+ Coins)", type="primary", use_container_width=True, key="volume_btn"):
        with st.spinner(f"Smart scanning ALL {len(get_perpetual_symbols())} coins..."):
            results_df = enhanced_volume_scanner_full(
                timeframe=timeframe,
                min_usdt_ratio=min_usdt_ratio,
                min_usdt_percentile=min_usdt_percentile,
                min_usdt_volume=min_usdt_volume,
                top_n=top_n,
                backtest_date=backtest_date_dt,
                priority_scan=priority_scan
            )
            
            if not results_df.empty:
                st.success(f"✅ Found {len(results_df)} high-quality volume spikes from full scan!")
                
                display_cols = ['Rank', 'symbol', 'usdt_volume_ratio', 'usdt_volume_percentile', 
                              'usdt_volume_display', 'price', 'price_change_%', 'candle_date']
                
                st.dataframe(
                    results_df[display_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Rank": st.column_config.NumberColumn("Rank", width="small"),
                        "symbol": st.column_config.TextColumn("Coin", width="medium"),
                        "usdt_volume_ratio": st.column_config.NumberColumn("Vol Ratio", width="small", format="%.1fx"),
                        "usdt_volume_percentile": st.column_config.NumberColumn("Percentile", width="small", format="%.0f%%"),
                        "usdt_volume_display": st.column_config.TextColumn("USDT Vol", width="small"),
                        "price": st.column_config.NumberColumn("Price", width="small", format="%.4f"),
                        "price_change_%": st.column_config.NumberColumn("Change %", width="small", format="%.1f%%"),
                        "candle_date": st.column_config.TextColumn("Date/Time", width="medium")
                    }
                )
                
                csv = results_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Results (CSV)",
                    data=csv,
                    file_name=f"volume_scan_full_{timeframe}_{datetime.now(IST).strftime('%Y%m%d_%H%M')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            else:
                st.warning("No volume spikes found. Try adjusting parameters.")

# ==============================================
# TAB 3: INDIVIDUAL COIN CHECKER
# ==============================================

def render_individual_checker():
    """Render Tab 3: Individual Coin Checker"""
    st.header("🎯 Individual Coin Checker")
    st.markdown("**Check specific coins for volume spikes**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        all_coins = get_perpetual_symbols()
        selection_method = st.radio("Selection Method:", ["Dropdown", "Manual Input"], horizontal=True, key="ind_radio")
        
        if selection_method == "Dropdown":
            selected_coins = st.multiselect("Select coins:", options=all_coins, 
                                          default=["BTCUSDT", "ETHUSDT"] if all_coins else [],
                                          max_selections=10, key="ind_dropdown")
        else:
            coin_input = st.text_area("Enter coins (one per line):", 
                                     value="BTCUSDT\nETHUSDT\nSOLUSDT", height=100, key="ind_text")
            coins = []
            for line in coin_input.split('\n'):
                line = line.strip()
                if ',' in line:
                    coins.extend([c.strip().upper() for c in line.split(',') if c.strip()])
                elif line:
                    coins.append(line.upper())
            selected_coins = list(set(coins))
        
        if selected_coins:
            st.success(f"✅ Selected {len(selected_coins)} coins")
    
    with col2:
        individual_timeframe = st.selectbox("Timeframe:", ["1H", "4H", "Daily", "Weekly"], index=1, key="ind_tf")
        individual_min_ratio = st.slider("Min Volume Ratio:", 1.0, 10.0, 2.0, 0.1, key="ind_ratio")
    
    if st.button("🔍 Check Selected Coins", type="primary", use_container_width=True, key="ind_btn"):
        if not selected_coins:
            st.warning("Please select at least one coin!")
        else:
            with st.spinner(f"Checking {len(selected_coins)} coins..."):
                results_df = enhanced_volume_scanner_full(
                    timeframe=individual_timeframe,
                    min_usdt_ratio=individual_min_ratio,
                    min_usdt_percentile=70,
                    min_usdt_volume=50000,
                    top_n=50,
                    specific_coins=selected_coins,
                    priority_scan=False
                )
                
                if not results_df.empty:
                    st.success(f"✅ Found volume spikes in {len(results_df)} coins!")
                    display_cols = ['Rank', 'symbol', 'usdt_volume_ratio', 'usdt_volume_percentile', 
                                  'usdt_volume_display', 'price_change_%']
                    st.dataframe(results_df[display_cols], use_container_width=True, hide_index=True)
                else:
                    st.info("No volume spikes found in selected coins.")

# ==============================================
# TAB 4: VOLUME HISTORY (WITH BTC COMPARISON)
# ==============================================

def get_historical_volume_analysis_with_btc(symbol, days_back=20):
    """Get daily USDT, Coin, and BTC volume for a specific coin over X days"""
    end_date = datetime.now(IST)
    start_date = end_date - timedelta(days=days_back)
    klines = get_historical_klines(symbol, "1d", start_date, end_date, limit=days_back + 10)
    
    if not klines:
        return pd.DataFrame(), pd.DataFrame()
    
    # Get BTC volumes for the same period
    btc_volumes = get_btc_historical_volumes(days_back)
    
    results = []
    for kline in klines:
        try:
            timestamp = kline[0]
            candle_date = datetime.fromtimestamp(timestamp / 1000, tz=UTC)
            candle_date_ist = candle_date.astimezone(IST)
            date_str = candle_date_ist.strftime('%b %d')
            full_date_str = candle_date_ist.strftime('%Y-%m-%d')
            
            open_price = float(kline[1])
            high_price = float(kline[2])
            low_price = float(kline[3])
            close_price = float(kline[4])
            trade_volume, usdt_volume = get_candle_volumes(kline)
            price_change = ((close_price - open_price) / open_price) * 100
            
            # Get BTC volume for this date
            btc_volume = btc_volumes.get(full_date_str, 0)
            
            results.append({
                'Date': date_str,
                'Full_Date': full_date_str,
                'USDT Volume (USD)': usdt_volume,
                'Coin Volume (USD)': trade_volume,
                'BTC Volume (USD)': btc_volume,
                'USDT_Display': format_large_number(usdt_volume),
                'Coin_Display': format_large_number(trade_volume),
                'BTC_Display': format_large_number(btc_volume),
                'price_change_%': round(price_change, 2)
            })
        except Exception as e:
            continue
    
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values('Full_Date', ascending=True).reset_index(drop=True)
        
        display_df = pd.DataFrame()
        display_df['Date'] = df['Date']
        display_df['USDT Volume (USD)'] = df['USDT_Display']
        display_df['Coin Volume (USD)'] = df['Coin_Display']
        display_df['BTC Volume (USD)'] = df['BTC_Display']
        
        return display_df, df
    else:
        return pd.DataFrame(), pd.DataFrame()

def create_volume_bar_chart(volume_df_raw, symbol):
    """Create GROUPED BAR CHART with Coin, BTC, and USDT volumes side by side"""
    if volume_df_raw.empty:
        return None
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Coin Volume',
        x=volume_df_raw['Date'],
        y=volume_df_raw['Coin Volume (USD)'],
        text=volume_df_raw['Coin_Display'],
        textposition='outside',
        marker_color='#2196F3',
        hovertemplate='Date: %{x}<br>Coin Volume: %{text}<extra></extra>'
    ))
    
    fig.add_trace(go.Bar(
        name='BTC Volume',
        x=volume_df_raw['Date'],
        y=volume_df_raw['BTC Volume (USD)'],
        text=volume_df_raw['BTC_Display'],
        textposition='outside',
        marker_color='#FF9800',
        hovertemplate='Date: %{x}<br>BTC Volume: %{text}<extra></extra>'
    ))
    
    fig.add_trace(go.Bar(
        name='USDT Volume',
        x=volume_df_raw['Date'],
        y=volume_df_raw['USDT Volume (USD)'],
        text=volume_df_raw['USDT_Display'],
        textposition='outside',
        marker_color='#4CAF50',
        hovertemplate='Date: %{x}<br>USDT Volume: %{text}<extra></extra>'
    ))
    
    fig.update_layout(
        barmode='group',
        bargap=0.15,
        bargroupgap=0.1,
        title={
            'text': f'{symbol} - Daily Volume Comparison (Coin vs BTC vs USDT)',
            'x': 0.5,
            'xanchor': 'center',
            'font': dict(size=18, color='#333')
        },
        xaxis_title='Date',
        yaxis_title='Volume (USD)',
        yaxis_type='log',
        template='plotly_white',
        height=600,
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        margin=dict(t=80, b=50, l=60, r=40)
    )
    
    fig.update_xaxes(tickangle=45, tickfont=dict(size=11))
    fig.update_yaxes(tickfont=dict(size=11), gridcolor='lightgrey')
    
    return fig

def create_volume_distribution_chart(volume_df_raw, symbol):
    """Create stacked bar chart showing volume distribution percentages"""
    if volume_df_raw.empty:
        return None
    
    df_copy = volume_df_raw.copy()
    df_copy['Total'] = df_copy['USDT Volume (USD)'] + df_copy['Coin Volume (USD)'] + df_copy['BTC Volume (USD)']
    df_copy['USDT_%'] = (df_copy['USDT Volume (USD)'] / df_copy['Total'] * 100).round(1)
    df_copy['Coin_%'] = (df_copy['Coin Volume (USD)'] / df_copy['Total'] * 100).round(1)
    df_copy['BTC_%'] = (df_copy['BTC Volume (USD)'] / df_copy['Total'] * 100).round(1)
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        name='Coin Volume %',
        x=df_copy['Date'],
        y=df_copy['Coin_%'],
        marker_color='#2196F3',
        text=df_copy['Coin_%'].apply(lambda x: f'{x}%'),
        textposition='inside',
        hovertemplate='Date: %{x}<br>Coin Volume: %{y}%<extra></extra>'
    ))
    
    fig.add_trace(go.Bar(
        name='BTC Volume %',
        x=df_copy['Date'],
        y=df_copy['BTC_%'],
        marker_color='#FF9800',
        text=df_copy['BTC_%'].apply(lambda x: f'{x}%'),
        textposition='inside',
        hovertemplate='Date: %{x}<br>BTC Volume: %{y}%<extra></extra>'
    ))
    
    fig.add_trace(go.Bar(
        name='USDT Volume %',
        x=df_copy['Date'],
        y=df_copy['USDT_%'],
        marker_color='#4CAF50',
        text=df_copy['USDT_%'].apply(lambda x: f'{x}%'),
        textposition='inside',
        hovertemplate='Date: %{x}<br>USDT Volume: %{y}%<extra></extra>'
    ))
    
    fig.update_layout(
        barmode='stack',
        title={
            'text': f'{symbol} - Daily Volume Distribution %',
            'x': 0.5,
            'xanchor': 'center',
            'font': dict(size=16, color='#333')
        },
        xaxis_title='Date',
        yaxis_title='Volume Distribution (%)',
        template='plotly_white',
        height=400,
        hovermode='x unified',
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        yaxis=dict(range=[0, 100])
    )
    
    fig.update_xaxes(tickangle=45, tickfont=dict(size=11))
    fig.update_yaxes(tickfont=dict(size=11), gridcolor='lightgrey')
    
    return fig

def render_volume_history():
    """Render Tab 4: Volume History with GROUPED BAR CHARTS"""
    st.header("📈 Historical Volume Analysis")
    st.markdown("""
    **Compare Coin Volume vs BTC Volume vs USDT Volume - BAR CHART VIEW**
    - 📊 **Grouped Bar Chart**: Side-by-side comparison of all three volumes
    - 📋 **Distribution Chart**: Percentage breakdown per day
    - 📑 **Table View**: Exact format as requested
    """)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        all_coins = get_perpetual_symbols()
        selected_coin = st.selectbox("Select Coin:", options=all_coins, 
                                    index=all_coins.index("BTCUSDT") if "BTCUSDT" in all_coins else 0,
                                    key="hist_coin")
        days_back = st.slider("Days to analyze:", 7, 90, 20, key="hist_days")
    
    with col2:
        volume_data = get_top_volume_symbols(50)
        coin_data = next((item for item in volume_data if item['symbol'] == selected_coin), None)
        if coin_data:
            st.metric("24h USDT Volume", format_large_number(coin_data['volume']))
            st.metric("24h Change", f"{coin_data['priceChangePercent']:.2f}%")
        
        btc_data = next((item for item in volume_data if item['symbol'] == 'BTCUSDT'), None)
        if btc_data:
            st.metric("BTC 24h Volume", format_large_number(btc_data['volume']))
    
    if st.button("📊 Show Volume Bar Chart", type="primary", use_container_width=True, key="hist_btn"):
        with st.spinner(f"Analyzing {days_back} days of volume data..."):
            display_df, raw_df = get_historical_volume_analysis_with_btc(selected_coin, days_back)
            
            if not display_df.empty:
                st.success(f"✅ Found {len(display_df)} days of volume data!")
                
                st.subheader("📊 Grouped Bar Chart - Volume Comparison")
                fig1 = create_volume_bar_chart(raw_df, selected_coin)
                if fig1:
                    st.plotly_chart(fig1, use_container_width=True)
                
                st.subheader("📊 Volume Distribution %")
                fig2 = create_volume_distribution_chart(raw_df, selected_coin)
                if fig2:
                    st.plotly_chart(fig2, use_container_width=True)
                
                st.subheader("📊 Volume Statistics")
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    avg_usdt = raw_df['USDT Volume (USD)'].mean()
                    st.metric("Avg USDT Volume", format_large_number(avg_usdt))
                
                with col2:
                    avg_coin = raw_df['Coin Volume (USD)'].mean()
                    st.metric("Avg Coin Volume", format_large_number(avg_coin))
                
                with col3:
                    avg_btc = raw_df['BTC Volume (USD)'].mean()
                    st.metric("Avg BTC Volume", format_large_number(avg_btc))
                
                with col4:
                    latest = raw_df.iloc[-1]
                    volumes = {
                        'USDT': latest['USDT Volume (USD)'],
                        'Coin': latest['Coin Volume (USD)'],
                        'BTC': latest['BTC Volume (USD)']
                    }
                    dominant = max(volumes, key=volumes.get)
                    st.metric("Latest Dominant", dominant)
                
                st.subheader("📋 Daily Volume Data - Exact Format")
                
                st.markdown("""
                <style>
                .volume-table {
                    width: 100%;
                    border-collapse: collapse;
                    font-family: 'Courier New', monospace;
                    font-size: 14px;
                }
                .volume-table th {
                    background-color: #4CAF50;
                    color: white;
                    padding: 12px;
                    text-align: left;
                    font-weight: bold;
                }
                .volume-table td {
                    padding: 10px;
                    border-bottom: 1px solid #ddd;
                }
                .volume-table tr:hover {
                    background-color: #f5f5f5;
                }
                </style>
                """, unsafe_allow_html=True)
                
                html_table = "<table class='volume-table'><tr>"
                html_table += "<th>Date</th><th>USDT Volume (USD)</th><th>Coin Volume (USD)</th><th>BTC Volume (USD)</th></tr>"
                
                for idx, row in display_df.iterrows():
                    html_table += f"<tr>"
                    html_table += f"<td>{row['Date']}</td>"
                    html_table += f"<td>{row['USDT Volume (USD)']}</td>"
                    html_table += f"<td>{row['Coin Volume (USD)']}</td>"
                    html_table += f"<td>{row['BTC Volume (USD)']}</td>"
                    html_table += f"</tr>"
                
                html_table += "</table>"
                st.markdown(html_table, unsafe_allow_html=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    csv = display_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Table as CSV",
                        data=csv,
                        file_name=f"volume_{selected_coin}_{days_back}days_{datetime.now(IST).strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                
                with col2:
                    if fig1:
                        st.download_button(
                            label="📥 Download Chart as HTML",
                            data=fig1.to_html(),
                            file_name=f"chart_{selected_coin}_{days_back}days.html",
                            mime="text/html",
                            use_container_width=True
                        )
                
                with st.expander("📈 View Raw Data with Price Changes"):
                    raw_display = raw_df[['Date', 'USDT Volume (USD)', 'Coin Volume (USD)', 
                                        'BTC Volume (USD)', 'price_change_%']].copy()
                    raw_display['USDT Volume (USD)'] = raw_display['USDT Volume (USD)'].apply(lambda x: format_large_number(x))
                    raw_display['Coin Volume (USD)'] = raw_display['Coin Volume (USD)'].apply(lambda x: format_large_number(x))
                    raw_display['BTC Volume (USD)'] = raw_display['BTC Volume (USD)'].apply(lambda x: format_large_number(x))
                    raw_display['price_change_%'] = raw_display['price_change_%'].apply(lambda x: f"{x}%")
                    
                    st.dataframe(raw_display, use_container_width=True, hide_index=True)
                
            else:
                st.warning(f"No historical data found for {selected_coin}.")

# ==============================================
# TAB 5: TOP GAINERS/LOSERS (FULL SCAN)
# ==============================================

def get_top_gainers_full(timeframe, period_date, top_n=10):
    """Find top gainers - SCANS ALL COINS"""
    symbols = get_perpetual_symbols()
    if not symbols:
        return pd.DataFrame()
    
    if timeframe == 'Monthly':
        start_date = datetime(period_date.year, period_date.month, 1, tzinfo=IST)
        if period_date.month == 12:
            end_date = datetime(period_date.year + 1, 1, 1, tzinfo=IST) - timedelta(seconds=1)
        else:
            end_date = datetime(period_date.year, period_date.month + 1, 1, tzinfo=IST) - timedelta(seconds=1)
        interval = "1d"
        limit = 31
    elif timeframe == 'Weekly':
        start_date = period_date - timedelta(days=period_date.weekday())
        start_date = datetime(start_date.year, start_date.month, start_date.day, 0, 0, 0, tzinfo=IST)
        end_date = start_date + timedelta(days=6, hours=23, minutes=59, seconds=59)
        interval = "4h"
        limit = 42
    else:  # Daily
        start_date = datetime(period_date.year, period_date.month, period_date.day, 0, 0, 0, tzinfo=IST)
        end_date = datetime(period_date.year, period_date.month, period_date.day, 23, 59, 59, tzinfo=IST)
        interval = "1h"
        limit = 24
    
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, symbol in enumerate(symbols):
        progress_bar.progress((i + 1) / len(symbols))
        status_text.text(f"Analyzing: {symbol} ({i+1}/{len(symbols)})")
        
        klines = get_historical_klines(symbol, interval, start_date, end_date, limit=limit)
        
        if klines and len(klines) >= 2:
            try:
                low_prices = [float(kline[3]) for kline in klines]
                high_prices = [float(kline[2]) for kline in klines]
                
                period_low = min(low_prices)
                period_high = max(high_prices)
                
                if period_low > 0:
                    percent_change = ((period_high - period_low) / period_low) * 100
                    open_price = float(klines[0][1])
                    close_price = float(klines[-1][4])
                    open_close_change = ((close_price - open_price) / open_price) * 100
                    
                    results.append({
                        'symbol': symbol,
                        'low_to_high_%': percent_change,
                        'open_close_%': open_close_change
                    })
            except:
                continue
        
        time.sleep(0.005)
    
    status_text.empty()
    
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values('low_to_high_%', ascending=False).head(top_n).reset_index(drop=True)
        df.index = df.index + 1
        df = df.rename_axis('Rank').reset_index()
        return df
    else:
        return pd.DataFrame()

def render_top_gainers():
    """Render Tab 5: Top Gainers/Losers - FULL SCAN"""
    st.header("🏆 Top Gainers & Losers Finder")
    st.markdown("**Find the biggest gainers and losers for any period - SCANS ALL 500+ COINS**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        timeframe = st.selectbox("Timeframe:", ["Monthly", "Weekly", "Daily"], index=0, key="gainers_tf")
        analysis_type = st.radio("Analysis Type:", ["Gainers Only", "Losers Only", "Both"], horizontal=True, key="gainers_type")
    
    with col2:
        top_n = st.number_input("Number to fetch:", 1, 50, 10, key="gainers_n")
        now_ist = datetime.now(IST)
        
        if timeframe == "Monthly":
            selected_month = st.date_input("Select Month:", value=now_ist - relativedelta(months=1), key="gainers_month")
            period_date = datetime(selected_month.year, selected_month.month, 1, tzinfo=IST)
        elif timeframe == "Weekly":
            selected_week = st.date_input("Select any day in week:", value=now_ist - timedelta(days=7), key="gainers_week")
            period_date = datetime(selected_week.year, selected_week.month, selected_week.day, tzinfo=IST)
        else:
            selected_day = st.date_input("Select Day:", value=now_ist - timedelta(days=1), key="gainers_day")
            period_date = datetime(selected_day.year, selected_day.month, selected_day.day, tzinfo=IST)
    
    if st.button("🚀 Find Top Performers (Full Scan)", type="primary", use_container_width=True, key="gainers_btn"):
        with st.spinner(f"Finding top performers from ALL {len(get_perpetual_symbols())} coins..."):
            df = get_top_gainers_full(timeframe, period_date, top_n * 2)
            
            if not df.empty:
                gainers_df = df[df['open_close_%'] > 0].copy().head(top_n)
                losers_df = df[df['open_close_%'] < 0].copy().head(top_n)
                
                if not losers_df.empty:
                    losers_df['high_to_low_%'] = abs(losers_df['low_to_high_%']) * -1
                
                if analysis_type in ["Gainers Only", "Both"] and not gainers_df.empty:
                    st.subheader(f"🏆 Top {len(gainers_df)} Gainers")
                    st.dataframe(
                        gainers_df[['Rank', 'symbol', 'open_close_%', 'low_to_high_%']],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "open_close_%": st.column_config.NumberColumn("Open→Close %", format="%.2f%%"),
                            "low_to_high_%": st.column_config.NumberColumn("Low→High %", format="%.2f%%")
                        }
                    )
                
                if analysis_type in ["Losers Only", "Both"] and not losers_df.empty:
                    st.subheader(f"📉 Top {len(losers_df)} Losers")
                    st.dataframe(
                        losers_df[['Rank', 'symbol', 'open_close_%', 'high_to_low_%']],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "open_close_%": st.column_config.NumberColumn("Open→Close %", format="%.2f%%"),
                            "high_to_low_%": st.column_config.NumberColumn("High→Low %", format="%.2f%%")
                        }
                    )
                
                col1, col2 = st.columns(2)
                with col1:
                    if not gainers_df.empty:
                        csv = gainers_df.to_csv(index=False)
                        st.download_button("📥 Download Gainers", csv, f"gainers_{period_date.strftime('%Y%m%d')}.csv", use_container_width=True)
                with col2:
                    if not losers_df.empty:
                        csv = losers_df.to_csv(index=False)
                        st.download_button("📥 Download Losers", csv, f"losers_{period_date.strftime('%Y%m%d')}.csv", use_container_width=True)
            else:
                st.warning("No data found for selected period.")

# ==============================================
# TAB 6: DAY PATTERN ANALYZER (WITH CHARTS)
# ==============================================

def analyze_day_patterns_full(symbol, day_of_week, timeframe, lookback_days=30, min_volume_usdt=100000, price_threshold=0.5):
    """Analyze patterns for a specific day of week - WITH CHARTS"""
    end_date = datetime.now(IST)
    start_date = end_date - timedelta(days=lookback_days)
    
    binance_interval = '1d' if timeframe == 'Daily' else ('4h' if timeframe == '4H' else '1h')
    candles_per_day = 1 if timeframe == 'Daily' else (6 if timeframe == '4H' else 24)
    
    klines = get_historical_klines(symbol, binance_interval, start_date, end_date, 
                                  limit=lookback_days * candles_per_day + 10)
    
    if not klines:
        return pd.DataFrame()
    
    results = []
    day_name_map = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 
                    4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    
    for kline in klines:
        try:
            timestamp = kline[0]
            candle_date = datetime.fromtimestamp(timestamp / 1000, tz=UTC)
            candle_date_ist = candle_date.astimezone(IST)
            candle_day = day_name_map[candle_date_ist.weekday()]
            
            if candle_day == day_of_week:
                open_price = float(kline[1])
                high_price = float(kline[2])
                low_price = float(kline[3])
                close_price = float(kline[4])
                trade_volume, usdt_volume = get_candle_volumes(kline)
                price_change_pct = ((close_price - open_price) / open_price) * 100
                
                if price_change_pct > price_threshold:
                    status = 'Bullish'
                elif price_change_pct < -price_threshold:
                    status = 'Bearish'
                else:
                    status = 'Neutral'
                
                if timeframe in ['4H', '1H']:
                    time_period = candle_date_ist.strftime('%H:%M')
                    if timeframe == '4H':
                        end_time = (candle_date_ist + timedelta(hours=4)).strftime('%H:%M')
                        time_period = f"{time_period}-{end_time}"
                else:
                    time_period = 'Daily'
                
                results.append({
                    'date': candle_date_ist.strftime('%Y-%m-%d'),
                    'day': candle_day,
                    'time_period': time_period,
                    'status': status,
                    'price_change_%': price_change_pct,
                    'usdt_volume': usdt_volume,
                    'usdt_volume_display': format_large_number(usdt_volume),
                    'coin_volume': trade_volume,
                    'coin_volume_display': format_large_number(trade_volume),
                    'open_price': open_price,
                    'close_price': close_price,
                    'high_price': high_price,
                    'low_price': low_price,
                    'volume_spike': 'Yes' if usdt_volume >= min_volume_usdt else 'No'
                })
        except:
            continue
    
    if results:
        df = pd.DataFrame(results)
        df = df.sort_values('date', ascending=False).reset_index(drop=True)
        df.index = df.index + 1
        df = df.rename_axis('No.').reset_index()
        return df
    else:
        return pd.DataFrame()

def create_day_pattern_chart(pattern_df, symbol, day_of_week):
    """Create interactive chart for day patterns"""
    if pattern_df.empty:
        return None
    
    chart_df = pattern_df.sort_values('date')
    
    fig = make_subplots(
        rows=3, cols=1,
        subplot_titles=(
            f'{symbol} - {day_of_week} Price Change Pattern',
            f'{symbol} - {day_of_week} USDT Volume',
            f'{symbol} - {day_of_week} Performance Distribution'
        ),
        specs=[
            [{"type": "xy"}],
            [{"type": "xy"}],
            [{"type": "domain"}]
        ],
        vertical_spacing=0.15,
        row_heights=[0.4, 0.3, 0.3]
    )
    
    colors = ['#4CAF50' if x == 'Bullish' else '#F44336' if x == 'Bearish' else '#FFC107' 
              for x in chart_df['status']]
    
    fig.add_trace(
        go.Bar(
            x=chart_df['date'],
            y=chart_df['price_change_%'],
            name='Price Change %',
            marker_color=colors,
            text=chart_df['status'],
            textposition='outside',
            hovertemplate='Date: %{x}<br>Change: %{y:.2f}%<br>Status: %{text}<extra></extra>'
        ),
        row=1, col=1
    )
    
    fig.add_trace(
        go.Bar(
            x=chart_df['date'],
            y=chart_df['usdt_volume'],
            name='USDT Volume',
            marker_color='#2196F3',
            text=chart_df['usdt_volume_display'],
            textposition='outside'
        ),
        row=2, col=1
    )
    
    status_counts = pattern_df['status'].value_counts()
    colors_pie = {'Bullish': '#4CAF50', 'Bearish': '#F44336', 'Neutral': '#FFC107'}
    pie_colors = [colors_pie.get(status, '#9E9E9E') for status in status_counts.index]
    
    fig.add_trace(
        go.Pie(
            labels=status_counts.index,
            values=status_counts.values,
            name='Distribution',
            marker_colors=pie_colors,
            textinfo='label+percent',
            hovertemplate='%{label}: %{value} occurrences (%{percent})<extra></extra>'
        ),
        row=3, col=1
    )
    
    fig.update_layout(
        height=900,
        showlegend=True,
        template='plotly_white',
        hovermode='x unified',
        title=f"{symbol} - {day_of_week} Pattern Analysis",
        title_x=0.5
    )
    
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_yaxes(title_text="Price Change %", row=1, col=1)
    fig.update_yaxes(title_text="USDT Volume", row=2, col=1)
    
    return fig

def render_day_patterns():
    """Render Tab 6: Day Pattern Analyzer with Charts"""
    st.header("📅 Day Pattern Analyzer")
    st.markdown("**Analyze day-of-week patterns for any cryptocurrency with interactive charts**")
    
    col1, col2 = st.columns(2)
    
    with col1:
        all_coins = get_perpetual_symbols()
        selected_coin = st.selectbox("Select Coin:", options=all_coins,
                                    index=all_coins.index("BTCUSDT") if "BTCUSDT" in all_coins else 0,
                                    key="pattern_coin")
    
    with col2:
        volume_data = get_top_volume_symbols(50)
        coin_data = next((item for item in volume_data if item['symbol'] == selected_coin), None)
        if coin_data:
            st.metric("24h Volume", format_large_number(coin_data['volume']))
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        day_of_week = st.selectbox("Day of Week:", 
                                  ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
                                  key="pattern_day")
    
    with col2:
        timeframe = st.selectbox("Timeframe:", ['Daily', '4H', '1H'], index=0, key="pattern_tf")
    
    with col3:
        lookback_days = st.slider("Lookback Days:", 10, 365, 90, key="pattern_lookback")
    
    min_volume = st.slider("Min USDT Volume:", 1000, 10000000, 100000, 10000, key="pattern_volume")
    
    if st.button("🔍 Analyze Day Patterns", type="primary", use_container_width=True, key="pattern_btn"):
        with st.spinner(f"Analyzing {day_of_week} patterns for {selected_coin}..."):
            pattern_df = analyze_day_patterns_full(selected_coin, day_of_week, timeframe, 
                                                  lookback_days, min_volume, 0.5)
            
            if not pattern_df.empty:
                st.success(f"✅ Found {len(pattern_df)} {day_of_week} occurrences!")
                
                bullish_count = len(pattern_df[pattern_df['status'] == 'Bullish'])
                bearish_count = len(pattern_df[pattern_df['status'] == 'Bearish'])
                neutral_count = len(pattern_df[pattern_df['status'] == 'Neutral'])
                total = len(pattern_df)
                
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Total Days", total)
                with col2:
                    st.metric("Bullish", f"{bullish_count}", f"{(bullish_count/total*100):.1f}%")
                with col3:
                    st.metric("Bearish", f"{bearish_count}", f"{(bearish_count/total*100):.1f}%")
                with col4:
                    st.metric("Neutral", f"{neutral_count}", f"{(neutral_count/total*100):.1f}%")
                with col5:
                    st.metric("Avg Change", f"{pattern_df['price_change_%'].mean():.2f}%")
                
                fig = create_day_pattern_chart(pattern_df, selected_coin, day_of_week)
                if fig:
                    st.plotly_chart(fig, use_container_width=True)
                
                st.subheader("📋 Detailed Pattern Data")
                display_cols = ['No.', 'date', 'time_period', 'status', 'price_change_%', 
                              'usdt_volume_display', 'volume_spike']
                
                st.dataframe(
                    pattern_df[display_cols],
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "price_change_%": st.column_config.NumberColumn("Change %", format="%.2f%%")
                    }
                )
                
                csv = pattern_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Pattern Data (CSV)",
                    data=csv,
                    file_name=f"{selected_coin}_{day_of_week}_patterns.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            else:
                st.warning(f"No {day_of_week} data found for {selected_coin}.")

# ==============================================
# TAB 7: MONTHLY REPORT GENERATOR (FULL SCAN)
# ==============================================

def analyze_single_day_for_gainers_full(date, coins_per_day=20):
    """Analyze a single day for top gainers and losers - FULL SCAN"""
    try:
        start_date = datetime(date.year, date.month, date.day, 0, 0, 0, tzinfo=IST)
        end_date = datetime(date.year, date.month, date.day, 23, 59, 59, tzinfo=IST)
        
        all_symbols = get_perpetual_symbols()
        results = []
        
        for symbol in all_symbols:
            try:
                day_klines = get_historical_klines(symbol, "1d", start_date, end_date, limit=2)
                if day_klines and len(day_klines) >= 1:
                    kline = day_klines[0]
                    open_price = float(kline[1])
                    high_price = float(kline[2])
                    low_price = float(kline[3])
                    close_price = float(kline[4])
                    
                    open_close_pct = ((close_price - open_price) / open_price) * 100
                    low_high_pct = ((high_price - low_price) / low_price) * 100
                    
                    results.append({
                        'symbol': symbol,
                        'open_close_%': open_close_pct,
                        'low_high_%': low_high_pct,
                        'date': date.strftime('%Y-%m-%d')
                    })
            except:
                continue
        
        if not results:
            return pd.DataFrame(), pd.DataFrame()
        
        df = pd.DataFrame(results)
        gainers = df[df['open_close_%'] > 0].copy().sort_values('low_high_%', ascending=False).head(coins_per_day)
        losers = df[df['open_close_%'] < 0].copy().sort_values('low_high_%', ascending=False).head(coins_per_day)
        
        if not gainers.empty:
            gainers = gainers[['date', 'symbol', 'open_close_%', 'low_high_%']]
            gainers.columns = ['Date', 'Coin', 'Open to Close %', 'Low to High %']
            gainers['Rank'] = range(1, len(gainers) + 1)
        
        if not losers.empty:
            losers = losers[['date', 'symbol', 'open_close_%', 'low_high_%']]
            losers.columns = ['Date', 'Coin', 'Open to Close %', 'Low to High %']
            losers['Rank'] = range(1, len(losers) + 1)
        
        return gainers, losers
        
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame()

def generate_monthly_report_full(selected_month, coins_per_day=20, max_workers=5):
    """Generate monthly report for each day - FULL SCAN"""
    try:
        year = selected_month.year
        month = selected_month.month
        
        if month == 12:
            next_month = datetime(year + 1, 1, 1, tzinfo=IST)
        else:
            next_month = datetime(year, month + 1, 1, tzinfo=IST)
        
        last_day = next_month - timedelta(days=1)
        days_in_month = last_day.day
        
        days = [datetime(year, month, day, tzinfo=IST) for day in range(1, days_in_month + 1)]
        today = datetime.now(IST)
        days = [day for day in days if day.date() <= today.date()]
        
        if not days:
            return {}, {}
        
        all_gainers = {}
        all_losers = {}
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_date = {executor.submit(analyze_single_day_for_gainers_full, day, coins_per_day): day for day in days}
            completed = 0
            total = len(future_to_date)
            
            for future in as_completed(future_to_date):
                date = future_to_date[future]
                date_str = date.strftime('%Y-%m-%d')
                
                try:
                    gainers_df, losers_df = future.result(timeout=60)
                    if not gainers_df.empty:
                        all_gainers[date_str] = gainers_df
                    if not losers_df.empty:
                        all_losers[date_str] = losers_df
                    
                    completed += 1
                    progress_bar.progress(completed / total)
                    status_text.text(f"✅ Processed {date_str} ({completed}/{total})")
                except Exception:
                    completed += 1
                    progress_bar.progress(completed / total)
        
        status_text.empty()
        return all_gainers, all_losers
        
    except Exception as e:
        return {}, {}

def render_monthly_report():
    """Render Tab 7: Monthly Report Generator - FULL SCAN"""
    st.header("📊 Monthly Report Generator")
    st.markdown("**Generate comprehensive daily reports for an entire month - SCANS ALL 500+ COINS**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        now_ist = datetime.now(IST)
        available_months = []
        month_descriptions = []
        
        for i in range(12):
            month_date = now_ist - relativedelta(months=i)
            month_str = month_date.strftime("%B %Y")
            
            if month_date.month < now_ist.month or month_date.year < now_ist.year:
                available_months.append(month_date)
                month_descriptions.append(f"{month_str} (Complete)")
            elif i == 0:
                available_months.append(month_date)
                month_descriptions.append(f"{month_str} (Current - Up to {now_ist.day})")
        
        if available_months:
            selected_month_idx = st.selectbox("Select Month:", range(len(available_months)),
                                            format_func=lambda x: month_descriptions[x], index=1,
                                            key="report_month")
            selected_month = available_months[selected_month_idx]
        else:
            st.error("No valid months available.")
            return
    
    with col2:
        coins_per_day = st.number_input("Coins per day:", 5, 50, 20, 5, key="report_coins")
    
    with col3:
        max_workers = st.slider("Parallel workers:", 1, 10, 5, key="report_workers")
    
    if st.button("🚀 Generate Monthly Report (Full Scan)", type="primary", use_container_width=True, key="report_btn"):
        if selected_month > datetime.now(IST):
            st.error("Cannot generate report for future dates.")
            return
        
        total_coins = len(get_perpetual_symbols())
        with st.spinner(f"Generating report for {selected_month.strftime('%B %Y')} - Scanning ALL {total_coins} coins..."):
            all_gainers, all_losers = generate_monthly_report_full(selected_month, coins_per_day, max_workers)
            
            if all_gainers or all_losers:
                st.success(f"✅ Report generated! {len(all_gainers)} days with gainers, {len(all_losers)} days with losers")
                
                gainers_list = []
                for date_str in sorted(all_gainers.keys()):
                    df = all_gainers[date_str].copy()
                    gainers_list.append(df)
                
                losers_list = []
                for date_str in sorted(all_losers.keys()):
                    df = all_losers[date_str].copy()
                    losers_list.append(df)
                
                st.subheader("📊 Monthly Summary")
                summary_data = []
                for date_str in sorted(all_gainers.keys()):
                    gainers_df = all_gainers[date_str]
                    losers_df = all_losers.get(date_str, pd.DataFrame())
                    
                    summary_data.append({
                        'Date': date_str,
                        'Gainers': len(gainers_df),
                        'Losers': len(losers_df),
                        'Top Gainer': gainers_df.iloc[0]['Coin'] if not gainers_df.empty else 'N/A',
                        'Top Gainer %': f"{gainers_df.iloc[0]['Open to Close %']:.2f}%" if not gainers_df.empty else 'N/A',
                        'Top Loser': losers_df.iloc[0]['Coin'] if not losers_df.empty else 'N/A',
                        'Top Loser %': f"{losers_df.iloc[0]['Open to Close %']:.2f}%" if not losers_df.empty else 'N/A'
                    })
                
                summary_df = pd.DataFrame(summary_data)
                st.dataframe(summary_df, use_container_width=True, hide_index=True)
                
                col1, col2 = st.columns(2)
                with col1:
                    if gainers_list:
                        consolidated_gainers = pd.concat(gainers_list, ignore_index=True)
                        csv = consolidated_gainers.to_csv(index=False)
                        st.download_button(
                            label="📥 Download All Gainers CSV",
                            data=csv,
                            file_name=f"gainers_{selected_month.strftime('%Y%m')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                
                with col2:
                    if losers_list:
                        consolidated_losers = pd.concat(losers_list, ignore_index=True)
                        csv = consolidated_losers.to_csv(index=False)
                        st.download_button(
                            label="📥 Download All Losers CSV",
                            data=csv,
                            file_name=f"losers_{selected_month.strftime('%Y%m')}.csv",
                            mime="text/csv",
                            use_container_width=True
                        )
                
                st.subheader("📅 Sample Daily Reports")
                sample_dates = sorted(all_gainers.keys())[:3]
                for date_str in sample_dates:
                    with st.expander(f"📊 {date_str} - Daily Report"):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.markdown("**🏆 Top Gainers**")
                            st.dataframe(all_gainers[date_str][['Rank', 'Coin', 'Open to Close %', 'Low to High %']], 
                                       use_container_width=True, hide_index=True)
                        with col2:
                            if date_str in all_losers:
                                st.markdown("**📉 Top Losers**")
                                st.dataframe(all_losers[date_str][['Rank', 'Coin', 'Open to Close %', 'Low to High %']], 
                                           use_container_width=True, hide_index=True)
            else:
                st.error("No data available for selected month.")

# ==============================================
# MAIN APP
# ==============================================

def main():
    render_ws_status()
    WS_LOADER = get_ws_loader()
    if not WS_LOADER.initialized:
        WS_LOADER.start()
    st.title("🚀 Binance Scanner Pro - Breakout + Volume Analysis")
    st.markdown("---")
    
    # Create 8 tabs (added new tab)
    tab1, tab2, tab3, tab4, tab5, tab6, tab7, tab8 = st.tabs([
        "🚀 Breakout Scanner",           # Original breakout scanner
        "🎯 TF Breakout",                 # NEW: Your timeframe breakout scanner
        "💎 Smart Scanner",               # Volume scanner
        "🎯 Individual Coin",              # Individual checker
        "📈 Volume History",               # Volume history
        "🏆 Top Gainers",                  # Top gainers
        "📅 Day Patterns",                 # Day patterns
        "📊 Monthly Report"                # Monthly report
    ])
    
    with tab1:
        render_breakout_scanner()          # Original
    
    with tab2:                             # NEW TAB
        render_timeframe_breakout_scanner()
    
    with tab3:
        render_volume_scanner()
    
    with tab4:
        render_individual_checker()
    
    with tab5:
        render_volume_history()
    
    with tab6:
        render_top_gainers()
    
    with tab7:
        render_day_patterns()
    
    with tab8:
        render_monthly_report()
    
    # Sidebar info
    with st.sidebar:
        st.header("ℹ️ About")
        st.markdown("""
        **Binance Scanner Pro - Enhanced Edition**
        
        🚀 **New Feature Added:**
        - ✅ **Timeframe Breakout Scanner** - Compare Past OPEN vs Current CLOSE
        - ✅ **Bullish Signal**: Past Bearish Candle + Current Close > Past Open
        - ✅ **Bearish Signal**: Past Bullish Candle + Current Close < Past Open
        - ✅ **Multiple Timeframe Combinations**: Monthly/Daily, Weekly/4H, etc.
        - ✅ **Only Most Recent Candle**: Shows ONLY breakouts from the latest candle
        
        **📊 Timeframe-Based OI Calculation:**
        - **Monthly**: Current month vs last month
        - **Weekly**: This week vs last week  
        - **Daily**: Today vs yesterday
        - **4H**: Current 4h vs previous 4h
        
        **📈 OI Change Interpretation:**
        - **>20%**: Strongly Increasing (🚀)
        - **10-20%**: Increasing (📈)
        - **5-10%**: Slightly Increasing (↗️)
        - **2-5%**: Moderate Increase (➡️)
        - **0.5-2%**: Minor Increase (↗️)
        - **-0.5 to 0.5%**: Neutral (➡️)
        - **-2 to -0.5%**: Minor Decrease (↘️)
        - **-5 to -2%**: Moderate Decrease (➡️)
        - **-10 to -5%**: Slightly Decreasing (↘️)
        - **-20 to -10%**: Decreasing (📉)
        - **<-20%**: Strongly Decreasing (💥)
        
        **Performance Tips:**
        - Full scans take 1-2 minutes for 500+ coins
        - Use priority scan for faster results
        - First scan may be slower due to caching
        """)
        
        # Show stats
        all_symbols = get_perpetual_symbols()
        if all_symbols:
            st.metric("Total Perpetual Pairs", len(all_symbols))
        
        volume_data = get_top_volume_symbols(5)
        if volume_data:
            st.subheader("🏆 Top Volume Coins")
            for item in volume_data[:5]:
                st.text(f"{item['symbol']}: {format_large_number(item['volume'])}")
        
        # Clear cache button
        if st.button("🧹 Clear Cache", use_container_width=True):
            st.cache_data.clear()
            st.success("Cache cleared! Data will refresh on next scan.")

if __name__ == "__main__":
    main()