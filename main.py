#!/usr/bin/env python3
"""
ENHANCED MARKET MONITOR - v9
- Custom stock selection with sector-wise organization
- Full 21-strike option chain with Greeks, OI changes
- Improved chart rendering for historical and live candles
- Auto-select nearest/next expiry
- Professional formatting
"""

import os
import asyncio
import requests
import urllib.parse
from datetime import datetime, timedelta, time
import pytz
import time as time_sleep
from telegram import Bot
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import pandas as pd
import io
import numpy as np

# CONFIG
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
BASE_URL = "https://api.upstox.com"
IST = pytz.timezone('Asia/Kolkata')

# INDICES - 4 Selected
INDICES = {
    "NSE_INDEX|Nifty 50": {"name": "NIFTY 50", "expiry_day": 1},
    "NSE_INDEX|Nifty Bank": {"name": "BANK NIFTY", "expiry_day": 2},
    "NSE_INDEX|Nifty Midcap Select": {"name": "MIDCAP NIFTY", "expiry_day": 0},
    "NSE_INDEX|Sensex": {"name": "SENSEX", "expiry_day": 4}
}

# CUSTOM STOCK SELECTION - Organized by Sector
SELECTED_STOCKS = {
    # Auto 🚗
    "NSE_EQ|INE467B01029": "TATAMOTORS",
    "NSE_EQ|INE585B01010": "MARUTI",
    "NSE_EQ|INE208A01029": "ASHOKLEY",
    "NSE_EQ|INE494B01023": "TVSMOTOR",
    "NSE_EQ|INE101A01026": "M&M",
    "NSE_EQ|INE917I01010": "BAJAJ-AUTO",
    
    # Banks 🏦
    "NSE_EQ|INE040A01034": "HDFCBANK",
    "NSE_EQ|INE090A01021": "ICICIBANK",
    "NSE_EQ|INE062A01020": "SBIN",
    "NSE_EQ|INE028A01039": "BANKBARODA",
    "NSE_EQ|INE238A01034": "AXISBANK",
    "NSE_EQ|INE237A01028": "KOTAKBANK",
    "NSE_EQ|INE949L01017": "AUBANK",
    
    # Metals 🏭
    "NSE_EQ|INE155A01022": "TATASTEEL",
    "NSE_EQ|INE205A01025": "HINDALCO",
    "NSE_EQ|INE019A01038": "JSWSTEEL",
    "NSE_EQ|INE139A01034": "NATIONALUM",
    
    # Oil & Gas ⛽
    "NSE_EQ|INE002A01018": "RELIANCE",
    "NSE_EQ|INE213A01029": "ONGC",
    "NSE_EQ|INE242A01010": "IOC",
    "NSE_EQ|INE029A01011": "BPCL",
    "NSE_EQ|INE347G01014": "PETRONET",
    
    # IT 💻
    "NSE_EQ|INE009A01021": "INFY",
    "NSE_EQ|INE075A01022": "WIPRO",
    "NSE_EQ|INE854D01024": "TCS",
    "NSE_EQ|INE047A01021": "HCLTECH",
    "NSE_EQ|INE214T01019": "LTIM",
    
    # Pharma 💊
    "NSE_EQ|INE044A01036": "SUNPHARMA",
    "NSE_EQ|INE361B01024": "DIVISLAB",
    "NSE_EQ|INE089A01023": "DRREDDY",
    "NSE_EQ|INE059A01026": "CIPLA",
    
    # FMCG 🛒
    "NSE_EQ|INE154A01025": "ITC",
    "NSE_EQ|INE030A01027": "HUL",
    "NSE_EQ|INE216A01030": "BRITANNIA",
    "NSE_EQ|INE016A01026": "DABUR",
    
    # Infra/Power ⚡
    "NSE_EQ|INE742F01042": "ADANIPORTS",
    "NSE_EQ|INE733E01010": "NTPC",
    "NSE_EQ|INE752E01010": "POWERGRID",
    "NSE_EQ|INE018A01030": "LT",
    
    # Retail/Consumer 👕
    "NSE_EQ|INE280A01028": "TITAN",
    "NSE_EQ|INE797F01012": "JUBLFOOD",
    "NSE_EQ|INE849A01020": "TRENT",
    "NSE_EQ|INE021A01026": "ASIANPAINT",
    "NSE_EQ|INE275A01019": "PAGEIND",
    
    # Insurance 🛡️
    "NSE_EQ|INE860A01027": "HDFCLIFE",
    "NSE_EQ|INE123W01016": "SBILIFE",
    "NSE_EQ|INE005A01018": "LICHSGFIN",
    
    # Others
    "NSE_EQ|INE397D01024": "BHARTIARTL",
    "NSE_EQ|INE296A01024": "BAJFINANCE",
    "NSE_EQ|INE758E01017": "JIOFIN"
}

# Global tracking
DAILY_STATS = {"total_alerts": 0, "indices_count": 0, "stocks_count": 0, "start_time": None}
OI_CACHE = {}  # Store previous OI for change calculation

print("="*70)
print("🚀 ENHANCED MARKET MONITOR - v9")
print("="*70)

def get_expiries(instrument_key):
    """Fetch all available expiries for an instrument"""
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
    encoded_key = urllib.parse.quote(instrument_key, safe='')
    url = f"{BASE_URL}/v2/option/contract?instrument_key={encoded_key}"
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            contracts = resp.json().get('data', [])
            return sorted(list(set(c['expiry'] for c in contracts if 'expiry' in c)))
    except Exception as e:
        print(f"  ⚠️ Expiry fetch error: {e}")
    return []

def get_next_expiry(instrument_key, expiry_day=1):
    """Auto-select nearest valid expiry, or next if current expired"""
    expiries = get_expiries(instrument_key)
    if not expiries:
        # Fallback calculation
        today = datetime.now(IST)
        days_ahead = expiry_day - today.weekday()
        if days_ahead <= 0:
            days_ahead += 7
        return (today + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
    
    today = datetime.now(IST).date()
    now_time = datetime.now(IST).time()
    
    # Filter future expiries
    future_expiries = []
    for exp_str in expiries:
        exp_date = datetime.strptime(exp_str, '%Y-%m-%d').date()
        # If expiry is today but after 3:30 PM, consider it expired
        if exp_date > today or (exp_date == today and now_time < time(15, 30)):
            future_expiries.append(exp_str)
    
    return min(future_expiries) if future_expiries else expiries[0]

def get_option_chain(instrument_key, expiry):
    """Fetch full option chain data"""
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
    encoded_key = urllib.parse.quote(instrument_key, safe='')
    url = f"{BASE_URL}/v2/option/chain?instrument_key={encoded_key}&expiry_date={expiry}"
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            strikes = resp.json().get('data', [])
            return sorted(strikes, key=lambda x: x.get('strike_price', 0))
    except Exception as e:
        print(f"  ⚠️ Chain fetch error: {e}")
    return []

def get_spot_price(instrument_key):
    """Fetch current spot price with retry logic"""
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
    encoded_key = urllib.parse.quote(instrument_key, safe='')
    url = f"{BASE_URL}/v2/market-quote/quotes?instrument_key={encoded_key}"
    for attempt in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                quote_data = resp.json().get('data', {})
                if quote_data:
                    ltp = quote_data[list(quote_data.keys())[0]].get('last_price', 0)
                    if ltp:
                        return float(ltp)
            time_sleep.sleep(2)
        except Exception as e:
            print(f"  ⚠️ Spot price error (attempt {attempt + 1}): {e}")
            time_sleep.sleep(2)
    return 0

def split_30min_to_5min(candle_30min):
    """Convert 30-min candle to six 5-min candles with proper distribution"""
    try:
        ts_str, o, h, l, c, v, oi = candle_30min
        dt_start = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).astimezone(IST)
        candles_5min = []
        
        # Calculate price movement per 5-min segment
        price_range = c - o
        
        for i in range(6):
            c_time = dt_start + timedelta(minutes=i * 5)
            # Linear interpolation for open/close
            c_open = o + (price_range * (i / 6))
            c_close = o + (price_range * ((i + 1) / 6))
            # High/low should respect the 30-min high/low bounds
            c_high = max(h * 0.95, c_open, c_close)  # Slightly below actual high
            c_low = min(l * 1.05, c_open, c_close)   # Slightly above actual low
            
            candles_5min.append([
                c_time.isoformat(),
                round(c_open, 2),
                round(c_high, 2),
                round(c_low, 2),
                round(c_close, 2),
                int(v / 6),
                int(oi)
            ])
        return candles_5min
    except Exception as e:
        print(f"    ⚠️ 30min split error: {e}")
        return []

def get_live_candles(instrument_key, symbol):
    """Fetch historical + live candle data with improved processing"""
    headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
    encoded_key = urllib.parse.quote(instrument_key, safe='')
    
    historical_5min_candles = []
    
    # Fetch 30-min historical data (up to 15 days back)
    try:
        to_date = (datetime.now(IST) - timedelta(days=1)).strftime('%Y-%m-%d')
        from_date = (datetime.now(IST) - timedelta(days=15)).strftime('%Y-%m-%d')
        url = f"{BASE_URL}/v2/historical-candle/{encoded_key}/30minute/{to_date}/{from_date}"
        resp = requests.get(url, headers=headers, timeout=20)
        
        if resp.status_code == 200 and resp.json().get('status') == 'success':
            candles_30min = resp.json().get('data', {}).get('candles', [])
            for candle in candles_30min:
                historical_5min_candles.extend(split_30min_to_5min(candle))
    except Exception as e:
        print(f"  ⚠️ Historical candle error: {e}")
    
    # Fetch today's 1-min intraday data and resample to 5-min
    intraday_5min_candles = []
    try:
        url = f"{BASE_URL}/v2/historical-candle/intraday/{encoded_key}/1minute"
        resp = requests.get(url, headers=headers, timeout=20)
        
        if resp.status_code == 200 and resp.json().get('status') == 'success':
            candles_1min = resp.json().get('data', {}).get('candles', [])
            if candles_1min:
                df = pd.DataFrame(candles_1min, columns=['ts', 'o', 'h', 'l', 'c', 'v', 'oi'])
                df['ts'] = pd.to_datetime(df['ts'])
                df = df.set_index('ts').astype(float)
                
                # Resample to 5-min properly
                df_resampled = df.resample('5min').agg({
                    'o': 'first',
                    'h': 'max',
                    'l': 'min',
                    'c': 'last',
                    'v': 'sum',
                    'oi': 'last'
                }).dropna()
                
                intraday_5min_candles = [
                    [idx.isoformat(), r['o'], r['h'], r['l'], r['c'], r['v'], r['oi']]
                    for idx, r in df_resampled.iterrows()
                ]
    except Exception as e:
        print(f"  ⚠️ Intraday candle error: {e}")
    
    # Merge and sort all candles
    all_candles = sorted(
        historical_5min_candles + intraday_5min_candles,
        key=lambda x: x[0]
    )
    
    # Calculate historical count (candles before today)
    today = datetime.now(IST).date()
    hist_count = len([
        c for c in all_candles
        if datetime.fromisoformat(c[0]).astimezone(IST).date() < today
    ])
    
    return all_candles, hist_count

def create_premium_chart(candles, symbol, spot_price, hist_count):
    """Enhanced chart with better clarity for historical and live data"""
    if not candles or len(candles) < 2:
        return None
    
    # Filter market hours data
    data = []
    for c in candles:
        try:
            ts = datetime.fromisoformat(c[0].replace("Z", "+00:00")).astimezone(IST)
            if time(9, 15) <= ts.time() <= time(15, 30):
                data.append({
                    'ts': ts,
                    'o': float(c[1]),
                    'h': float(c[2]),
                    'l': float(c[3]),
                    'c': float(c[4]),
                    'v': int(c[5])
                })
        except (ValueError, TypeError):
            continue
    
    if not data:
        return None
    
    # Create figure
    fig, (ax1, ax2) = plt.subplots(
        2, 1,
        figsize=(28, 13),
        gridspec_kw={'height_ratios': [4, 1]},
        facecolor='#0e1217'
    )
    for ax in [ax1, ax2]:
        ax.set_facecolor('#0e1217')
    
    # Plot candles with improved visibility
    for i, row in enumerate(data):
        # Distinguish historical vs live candles
        if i < hist_count:
            # Historical candles - slightly muted colors
            color = '#1f8a7a' if row['c'] >= row['o'] else '#d63f3a'
            alpha = 0.85
            linewidth = 1.2
        else:
            # Live candles - vibrant colors
            color = '#26a69a' if row['c'] >= row['o'] else '#ef5350'
            alpha = 1.0
            linewidth = 1.8
        
        # Wick (high-low line)
        ax1.plot(
            [i, i], [row['l'], row['h']],
            color=color,
            linewidth=linewidth,
            alpha=alpha,
            zorder=1
        )
        
        # Body (open-close rectangle)
        body_height = abs(row['c'] - row['o'])
        body_bottom = min(row['o'], row['c'])
        
        if body_height < 0.001:  # Doji
            body_height = spot_price * 0.0005
        
        rect = Rectangle(
            (i - 0.4, body_bottom),
            0.8,
            body_height,
            facecolor=color,
            edgecolor=color,
            alpha=alpha,
            linewidth=0.5,
            zorder=2
        )
        ax1.add_patch(rect)
        
        # Volume bars
        ax2.bar(i, row['v'], width=0.8, color=color, alpha=alpha)
    
    # Historical/Live separator line
    if hist_count > 0 and hist_count < len(data):
        separator_x = hist_count - 0.5
        for ax in [ax1, ax2]:
            ax.axvline(
                x=separator_x,
                color='#ffa726',
                linestyle='--',
                linewidth=2,
                alpha=0.8,
                label='Today Start' if ax == ax1 else None
            )
    
    # Spot price line
    ax1.axhline(
        y=spot_price,
        color='#2962ff',
        linestyle='--',
        linewidth=2.5,
        alpha=0.95
    )
    
    # Spot price label on right axis
    ax1_right = ax1.twinx()
    ax1_right.set_ylim(ax1.get_ylim())
    ax1_right.set_yticks([spot_price])
    ax1_right.set_yticklabels(
        [f'₹{spot_price:,.2f}'],
        fontsize=14,
        fontweight='bold',
        color='#2962ff',
        bbox=dict(facecolor='#2962ff', alpha=0.25, pad=3)
    )
    
    # Title
    ax1.set_title(
        f'{symbol} • 5 Min Professional Chart • {datetime.now(IST).strftime("%d %b %Y • %I:%M:%S %p IST")}',
        color='#d1d4dc',
        fontsize=18,
        fontweight='bold',
        loc='left',
        pad=15
    )
    
    # Labels
    ax1.set_ylabel('Price (₹)', color='#b2b5be', fontsize=12, fontweight='600')
    ax2.set_ylabel('Volume', color='#b2b5be', fontsize=12, fontweight='600')
    ax2.set_xlabel('Date & Time (IST)', color='#b2b5be', fontsize=12, fontweight='600')
    
    # X-axis ticks (date labels)
    tick_positions = []
    tick_labels = []
    last_date_label = None
    
    for i, row in enumerate(data):
        current_date = row['ts'].date()
        if current_date != last_date_label:
            tick_positions.append(i)
            tick_labels.append(row['ts'].strftime('%d %b'))
            last_date_label = current_date
    
    # Reduce tick density if too many
    if len(tick_positions) > 12:
        step = max(1, len(tick_positions) // 8)
        tick_positions = tick_positions[::step]
        tick_labels = tick_labels[::step]
    
    # Apply styling
    for ax in [ax1, ax2]:
        ax.grid(True, alpha=0.15, color='#363a45', linewidth=0.8)
        ax.tick_params(axis='y', colors='#787b86', labelsize=11)
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels, color='#787b86', fontsize=10, rotation=0)
        ax.set_xlim(-1, len(data))
        for spine in ax.spines.values():
            spine.set_color('#1e222d')
            spine.set_linewidth(1.5)
    
    # Legend
    if hist_count > 0 and hist_count < len(data):
        ax1.legend(loc='upper left', fontsize=10, framealpha=0.3)
    
    plt.tight_layout(pad=2)
    plt.subplots_adjust(hspace=0.1)
    
    # Save to buffer
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=160, facecolor='#0e1217')
    buf.seek(0)
    plt.close(fig)
    
    return buf

def fmt_val(v):
    """Format large numbers compactly"""
    if v >= 10000000:
        return f"{v/10000000:.1f}Cr"
    if v >= 100000:
        return f"{v/100000:.1f}L"
    if v >= 1000:
        return f"{v/1000:.0f}K"
    return str(int(v))

def calculate_greeks(strike, spot, tte, volatility, option_type='CE'):
    """
    Simplified Black-Scholes Greeks calculation
    Note: This is a basic implementation. Use actual Greeks from API if available.
    """
    try:
        from scipy.stats import norm
        
        # Risk-free rate (approximate)
        r = 0.07
        
        # Ensure valid inputs
        if tte <= 0 or volatility <= 0 or spot <= 0 or strike <= 0:
            return {'delta': 0, 'gamma': 0, 'theta': 0, 'vega': 0}
        
        # Calculate d1 and d2
        d1 = (np.log(spot / strike) + (r + 0.5 * volatility ** 2) * tte) / (volatility * np.sqrt(tte))
        d2 = d1 - volatility * np.sqrt(tte)
        
        if option_type == 'CE':
            delta = norm.cdf(d1)
        else:  # PE
            delta = -norm.cdf(-d1)
        
        gamma = norm.pdf(d1) / (spot * volatility * np.sqrt(tte))
        theta = -(spot * norm.pdf(d1) * volatility) / (2 * np.sqrt(tte)) / 365
        vega = spot * norm.pdf(d1) * np.sqrt(tte) / 100
        
        return {
            'delta': round(delta, 3),
            'gamma': round(gamma, 4),
            'theta': round(theta, 2),
            'vega': round(vega, 2)
        }
    except:
        return {'delta': 0, 'gamma': 0, 'theta': 0, 'vega': 0}

def format_option_chain_message(symbol, spot, expiry, strikes):
    """Enhanced option chain formatting with full 21 strikes, Greeks, and OI changes"""
    if not strikes:
        return None
    
    # Find ATM strike
    atm_strike = min(strikes, key=lambda x: abs(x.get('strike_price', 0) - spot))
    atm_index = strikes.index(atm_strike)
    atm_price = atm_strike.get('strike_price', 0)
    
    # Select 21 strikes centered around ATM (10 above, ATM, 10 below)
    start_idx = max(0, atm_index - 10)
    end_idx = min(len(strikes), atm_index + 11)
    selected = strikes[start_idx:end_idx]
    
    # Calculate total OI and volumes
    total_ce_oi = sum(s.get('call_options', {}).get('market_data', {}).get('oi', 0) for s in strikes)
    total_pe_oi = sum(s.get('put_options', {}).get('market_data', {}).get('oi', 0) for s in strikes)
    total_ce_vol = sum(s.get('call_options', {}).get('market_data', {}).get('volume', 0) for s in strikes)
    total_pe_vol = sum(s.get('put_options', {}).get('market_data', {}).get('volume', 0) for s in strikes)
    
    # Calculate time to expiry for Greeks
    expiry_date = datetime.strptime(expiry, '%Y-%m-%d')
    tte = max((expiry_date - datetime.now()).days / 365, 0.001)
    
    # Build message
    msg = f"*{symbol} - OPTION CHAIN*\n"
    msg += f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"*Spot Price:* ₹`{spot:,.2f}`\n"
    msg += f"*Expiry:* `{expiry}` (T+{max(0, (expiry_date - datetime.now()).days)})\n"
    msg += f"*ATM Strike:* ₹`{atm_price:,.0f}`\n\n"
    
    msg += "```\n"
    msg += "╔════════════ CALLS ═══════════╦══ STRIKE ══╦═══════════ PUTS ════════════╗\n"
    msg += "║  OI    OIΔ   Vol   LTP   IV  ║   Price    ║  IV  LTP   Vol   OIΔ    OI  ║\n"
    msg += "╠═════════════════════════════╬════════════╬═════════════════════════════╣\n"
    
    # Store current OI for next comparison
    cache_key = f"{symbol}_{expiry}"
    prev_oi_data = OI_CACHE.get(cache_key, {})
    current_oi_data = {}
    
    for s in selected:
        sp = s.get('strike_price', 0)
        marker = "►" if sp == atm_price else " "
        
        ce_data = s.get('call_options', {}).get('market_data', {})
        pe_data = s.get('put_options', {}).get('market_data', {})
        
        # CE data
        ce_oi = ce_data.get('oi', 0)
        ce_vol = ce_data.get('volume', 0)
        ce_ltp = ce_data.get('ltp', 0)
        ce_iv = ce_data.get('iv', 0)
        
        # PE data
        pe_oi = pe_data.get('oi', 0)
        pe_vol = pe_data.get('volume', 0)
        pe_ltp = pe_data.get('ltp', 0)
        pe_iv = pe_data.get('iv', 0)
        
        # Calculate OI changes
        prev_ce_oi = prev_oi_data.get(f"CE_{sp}", ce_oi)
        prev_pe_oi = prev_oi_data.get(f"PE_{sp}", pe_oi)
        ce_oi_change = ce_oi - prev_ce_oi
        pe_oi_change = pe_oi - prev_pe_oi
        
        # Store current OI
        current_oi_data[f"CE_{sp}"] = ce_oi
        current_oi_data[f"PE_{sp}"] = pe_oi
        
        # Format values
        ce_oi_str = fmt_val(ce_oi)
        ce_oi_chg_str = f"+{fmt_val(ce_oi_change)}" if ce_oi_change > 0 else fmt_val(ce_oi_change) if ce_oi_change < 0 else "  0"
        ce_vol_str = fmt_val(ce_vol)
        ce_iv_str = f"{ce_iv:.1f}" if ce_iv > 0 else " - "
        
        pe_oi_str = fmt_val(pe_oi)
        pe_oi_chg_str = f"+{fmt_val(pe_oi_change)}" if pe_oi_change > 0 else fmt_val(pe_oi_change) if pe_oi_change < 0 else "  0"
        pe_vol_str = fmt_val(pe_vol)
        pe_iv_str = f"{pe_iv:.1f}" if pe_iv > 0 else " - "
        
        msg += f"║ {ce_oi_str:>5} {ce_oi_chg_str:>5} {ce_vol_str:>5} {ce_ltp:5.1f} {ce_iv_str:>4} ║ {marker}{sp:>6.0f}{marker} ║ {pe_iv_str:>4} {pe_ltp:5.1f} {pe_vol_str:>5} {pe_oi_chg_str:>5} {pe_oi_str:>5} ║\n"
    
    msg += "╚═════════════════════════════╩════════════╩═════════════════════════════╝\n"
    msg += "```\n\n"
    
    # Update OI cache
    OI_CACHE[cache_key] = current_oi_data
    
    # ATM Greeks (using ATM strike data)
    atm_ce_iv = atm_strike.get('call_options', {}).get('market_data', {}).get('iv', 30) / 100
    atm_pe_iv = atm_strike.get('put_options', {}).get('market_data', {}).get('iv', 30) / 100
    
    ce_greeks = calculate_greeks(atm_price, spot, tte, atm_ce_iv, 'CE')
    pe_greeks = calculate_greeks(atm_price, spot, tte, atm_pe_iv, 'PE')
    
    msg += f"*📊 ATM GREEKS & IV:*\n"
    msg += f"```\n"
    msg += f"CE: Δ={ce_greeks['delta']:+.3f} Θ={ce_greeks['theta']:+.2f} IV={atm_ce_iv*100:.1f}%\n"
    msg += f"PE: Δ={pe_greeks['delta']:+.3f} Θ={pe_greeks['theta']:+.2f} IV={atm_pe_iv*100:.1f}%\n"
    msg += f"```\n\n"
    
    # Market sentiment
    pcr_oi = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
    pcr_vol = total_pe_vol / total_ce_vol if total_ce_vol > 0 else 0
    
    if pcr_oi > 1.2:
        pcr_sentiment = "🟢 BULLISH"
    elif pcr_oi < 0.8:
        pcr_sentiment = "🔴 BEARISH"
    else:
        pcr_sentiment = "🟡 NEUTRAL"
    
    # OI trend analysis
    total_ce_oi_change = sum(current_oi_data.get(k, 0) - prev_oi_data.get(k, 0) 
                              for k in current_oi_data.keys() if k.startswith('CE_'))
    total_pe_oi_change = sum(current_oi_data.get(k, 0) - prev_oi_data.get(k, 0) 
                              for k in current_oi_data.keys() if k.startswith('PE_'))
    
    if abs(total_pe_oi_change) > abs(total_ce_oi_change):
        oi_trend = "📈 PE OI Rising Faster"
    elif abs(total_ce_oi_change) > abs(total_pe_oi_change):
        oi_trend = "📉 CE OI Rising Faster"
    else:
        oi_trend = "⚖️ Balanced OI Growth"
    
    msg += f"*🎯 MARKET SENTIMENT & STATISTICS:*\n"
    msg += f"• *PCR (OI):* `{pcr_oi:.3f}` {pcr_sentiment}\n"
    msg += f"• *PCR (Volume):* `{pcr_vol:.3f}`\n"
    msg += f"• *Total CE OI:* `{fmt_val(total_ce_oi)}` | *Total PE OI:* `{fmt_val(total_pe_oi)}`\n"
    msg += f"• *OI Change CE:* `{fmt_val(total_ce_oi_change)}` | *OI Change PE:* `{fmt_val(total_pe_oi_change)}`\n"
    msg += f"• *Total CE Volume:* `{fmt_val(total_ce_vol)}` | *Total PE Volume:* `{fmt_val(total_pe_vol)}`\n"
    msg += f"• *OI Trend:* {oi_trend}\n\n"
    msg += f"⏰ *Last Updated:* {datetime.now(IST).strftime('%I:%M:%S %p IST')}\n"
    
    return msg

async def send_telegram_message(bot, text=None, photo=None, caption=None):
    """Send message or photo to Telegram"""
    try:
        if photo:
            await bot.send_photo(
                chat_id=TELEGRAM_CHAT_ID,
                photo=photo,
                caption=caption,
                parse_mode='Markdown'
            )
        else:
            await bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=text,
                parse_mode='Markdown'
            )
        DAILY_STATS["total_alerts"] += 1
        return True
    except Exception as e:
        print(f"  ❌ Telegram error: {e}")
        return False

async def process_instrument(bot, key, name, expiry_day, is_stock=False, idx=0, total=0):
    """Process single instrument: fetch data, generate chart and option chain"""
    prefix = f"[{idx}/{total}] STOCK:" if is_stock else "INDEX:"
    print(f"\n{prefix} {name}")
    
    try:
        # Get spot price
        spot = get_spot_price(key)
        if spot == 0:
            print(f"  ❌ Failed to get spot price for {name}")
            return False
        print(f"  ✅ Spot: ₹{spot:,.2f}")
        
        # Get next expiry (auto-select)
        expiry = get_next_expiry(key, expiry_day=expiry_day)
        print(f"  📅 Expiry: {expiry}")
        
        # Get option chain
        strikes = get_option_chain(key, expiry)
        if strikes:
            msg = format_option_chain_message(name, spot, expiry, strikes)
            if msg:
                await send_telegram_message(bot, text=msg)
                print(f"  📤 Option chain sent ({len(strikes)} strikes)")
            await asyncio.sleep(1)  # Small delay between messages
        else:
            print(f"  ⚠️ No option chain data found")
        
        # Get candle data and generate chart
        candles, hist_count = get_live_candles(key, name)
        if candles:
            print(f"  📊 Candles: {len(candles)} total, {hist_count} historical")
            chart = create_premium_chart(candles, name, spot, hist_count)
            if chart:
                caption = f"📈 *{name}*\n💰 `₹{spot:,.2f}`\n📅 `{expiry}`"
                await send_telegram_message(bot, photo=chart, caption=caption)
                print(f"  📤 Chart sent")
        else:
            print(f"  ⚠️ No candle data for chart")
        
        # Update counters
        if is_stock:
            DAILY_STATS["stocks_count"] += 1
        else:
            DAILY_STATS["indices_count"] += 1
        
        return True
        
    except Exception as e:
        print(f"  ❌ Processing error for {name}: {e}")
        import traceback
        traceback.print_exc()
        return False

async def fetch_all(bot):
    """Main fetch cycle for all instruments"""
    now = datetime.now(IST)
    print(f"\n{'='*70}")
    print(f"🚀 RUN START: {now.strftime('%I:%M:%S %p IST')}")
    print(f"{'='*70}")
    
    # Send header message
    header = (f"🚀 *MARKET UPDATE* @ `{now.strftime('%I:%M %p')}`\n"
              f"_Processing {len(INDICES)} indices + {len(SELECTED_STOCKS)} stocks..._")
    await send_telegram_message(bot, text=header)
    
    # Reset counters
    DAILY_STATS["indices_count"] = 0
    DAILY_STATS["stocks_count"] = 0
    
    # Process all indices
    print(f"\n📊 PROCESSING INDICES ({len(INDICES)})...")
    for key, info in INDICES.items():
        await process_instrument(bot, key, info["name"], info["expiry_day"])
        await asyncio.sleep(3)  # Delay between instruments
    
    # Process all stocks
    print(f"\n📈 PROCESSING STOCKS ({len(SELECTED_STOCKS)})...")
    for i, (key, symbol) in enumerate(SELECTED_STOCKS.items(), 1):
        await process_instrument(
            bot, key, symbol, 3,
            is_stock=True,
            idx=i,
            total=len(SELECTED_STOCKS)
        )
        await asyncio.sleep(3)  # Delay between instruments
    
    # Send summary
    summary = (
        f"✅ *UPDATE COMPLETE*\n\n"
        f"📊 *Indices:* {DAILY_STATS['indices_count']}/{len(INDICES)}\n"
        f"📈 *Stocks:* {DAILY_STATS['stocks_count']}/{len(SELECTED_STOCKS)}\n"
        f"📡 *Total Alerts Today:* {DAILY_STATS['total_alerts']}\n\n"
        f"⏰ Next update in 5 minutes..."
    )
    await send_telegram_message(bot, text=summary)
    
    print(f"\n✅ CYCLE COMPLETE")
    print(f"   Indices: {DAILY_STATS['indices_count']}/{len(INDICES)}")
    print(f"   Stocks: {DAILY_STATS['stocks_count']}/{len(SELECTED_STOCKS)}")
    print(f"   Total Alerts: {DAILY_STATS['total_alerts']}")

async def main():
    """Main event loop"""
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    
    print(f"\n🤖 Bot initialized")
    print(f"📊 Monitoring {len(INDICES)} indices + {len(SELECTED_STOCKS)} stocks")
    print(f"⏰ Update interval: 5 minutes")
    print(f"🕒 Market hours: 9:15 AM - 3:35 PM IST (Mon-Fri)")
    
    while True:
        now = datetime.now(IST)
        is_market_hours = (
            now.weekday() < 5 and
            time(9, 15) <= now.time() <= time(15, 35)
        )
        
        if is_market_hours:
            # Mark session start
            if DAILY_STATS["start_time"] is None:
                DAILY_STATS["start_time"] = now
                print(f"\n🟢 Market opened - Session started")
            
            # Run fetch cycle
            await fetch_all(bot)
            
            # Wait for next cycle
            print(f"\n⏳ Next run in 5 minutes...")
            await asyncio.sleep(300)  # 5 minutes
            
        else:
            # Market closed
            print(f"\n💤 Market closed. Current time: {now.strftime('%I:%M %p IST')}")
            print(f"   Next check in 15 minutes...")
            
            # Reset stats after market close (after 4 PM)
            if now.hour >= 16 and DAILY_STATS["start_time"] is not None:
                print("🔄 Resetting daily stats for next session...")
                DAILY_STATS["total_alerts"] = 0
                DAILY_STATS["indices_count"] = 0
                DAILY_STATS["stocks_count"] = 0
                DAILY_STATS["start_time"] = None
                OI_CACHE.clear()  # Clear OI cache
            
            await asyncio.sleep(900)  # 15 minutes

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Bot stopped by user.")
        print("📊 Final Stats:")
        print(f"   Total Alerts: {DAILY_STATS['total_alerts']}")
        print(f"   Indices Processed: {DAILY_STATS['indices_count']}")
        print(f"   Stocks Processed: {DAILY_STATS['stocks_count']}")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
