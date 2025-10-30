#!/usr/bin/env python3
"""
HYBRID TRADING BOT v13.0 - MULTI-TIMEFRAME BEAST
=================================================
✅ MULTI-TIMEFRAME STRATEGY:
   - 5min TF: Entry/Exit/Targets (precision)
   - 15min TF: Patterns + OI analysis (main signal)
   - 1hr TF: Trend confirmation (filter)

✅ 400+ CANDLESTICKS (15min):
   - Historical 30min data (10 days)
   - Today's 1min intraday data
   - Auto-resampled to 5m/15m/1h

✅ AI GETS ALL TIMEFRAMES:
   - 1hr trend → 15min pattern → 5min entry
   - Better context = Better decisions
   
✅ REDIS OI TRACKING (24h expiry)
✅ PNG CHART ALERTS
✅ CORRECTED ISINs (PAGEIND, LICHSGFIN, BAJAJFINSV)
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
import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import traceback
import re

# Redis import with fallback
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logging.warning("Redis not available - running without OI tracking")

# Setup logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# CONFIG
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
BASE_URL = "https://api.upstox.com"
IST = pytz.timezone('Asia/Kolkata')

# Redis expiry: 24 hours
REDIS_EXPIRY = 86400

# ✅ CORRECTED INDICES
INDICES = {
    "NSE_INDEX|Nifty 50": {"name": "NIFTY 50", "expiry_day": 1},
    "NSE_INDEX|Nifty Bank": {"name": "BANK NIFTY", "expiry_day": 2},
    "NSE_INDEX|NIFTY MID SELECT": {"name": "MIDCAP NIFTY", "expiry_day": 0},
    "BSE_INDEX|SENSEX": {"name": "SENSEX", "expiry_day": 4}
}

# ✅ CORRECTED STOCKS
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
    
    # Metals 🏭
    "NSE_EQ|INE155A01022": "TATASTEEL",
    "NSE_EQ|INE205A01025": "HINDALCO",
    "NSE_EQ|INE019A01038": "JSWSTEEL",
    
    # Oil & Gas ⛽
    "NSE_EQ|INE002A01018": "RELIANCE",
    "NSE_EQ|INE213A01029": "ONGC",
    "NSE_EQ|INE242A01010": "IOC",
    
    # IT 💻
    "NSE_EQ|INE009A01021": "INFY",
    "NSE_EQ|INE075A01022": "WIPRO",
    "NSE_EQ|INE854D01024": "TCS",
    "NSE_EQ|INE047A01021": "HCLTECH",
    
    # Pharma 💊
    "NSE_EQ|INE044A01036": "SUNPHARMA",
    "NSE_EQ|INE361B01024": "DIVISLAB",
    "NSE_EQ|INE089A01023": "DRREDDY",
    
    # FMCG 🛒
    "NSE_EQ|INE154A01025": "ITC",
    "NSE_EQ|INE030A01027": "HUL",
    "NSE_EQ|INE216A01030": "BRITANNIA",
    
    # Infra/Power ⚡
    "NSE_EQ|INE742F01042": "ADANIPORTS",
    "NSE_EQ|INE733E01010": "NTPC",
    "NSE_EQ|INE018A01030": "LT",
    
    # Retail/Consumer 👕
    "NSE_EQ|INE280A01028": "TITAN",
    "NSE_EQ|INE797F01012": "JUBLFOOD",
    "NSE_EQ|INE849A01020": "TRENT",
    "NSE_EQ|INE021A01026": "ASIANPAINT",
    "NSE_EQ|INE761H01022": "PAGEIND",
    
    # Insurance 🛡️
    "NSE_EQ|INE860A01027": "HDFCLIFE",
    "NSE_EQ|INE123W01016": "SBILIFE",
    "NSE_EQ|INE115A01026": "LICHSGFIN",
    
    # Others 📱
    "NSE_EQ|INE397D01024": "BHARTIARTL",
    "NSE_EQ|INE918I01026": "BAJAJFINSV",
    "NSE_EQ|INE758E01017": "JIOFIN"
}

# Analysis thresholds
CONFIDENCE_MIN = 75
SCORE_MIN = 90
ALIGNMENT_MIN = 18

SCAN_INTERVAL = 900  # 15 minutes

@dataclass
class OIData:
    strike: float
    ce_oi: int
    pe_oi: int
    ce_volume: int
    pe_volume: int
    ce_oi_change: int = 0
    pe_oi_change: int = 0
    ce_iv: float = 0.0
    pe_iv: float = 0.0
    pcr_at_strike: float = 0.0

@dataclass
class AggregateOIAnalysis:
    total_ce_oi: int
    total_pe_oi: int
    total_ce_volume: int
    total_pe_volume: int
    ce_oi_change_pct: float
    pe_oi_change_pct: float
    ce_volume_change_pct: float
    pe_volume_change_pct: float
    pcr: float
    overall_sentiment: str
    max_pain: float = 0.0

@dataclass
class MultiTimeframeData:
    """Container for all timeframe data"""
    df_5m: pd.DataFrame
    df_15m: pd.DataFrame
    df_1h: pd.DataFrame
    current_5m_price: float
    current_15m_price: float
    current_1h_price: float
    trend_1h: str  # BULLISH/BEARISH/NEUTRAL
    pattern_15m: str
    entry_5m: float

@dataclass
class DeepAnalysis:
    opportunity: str
    confidence: int
    chart_score: int
    option_score: int
    alignment_score: int
    total_score: int
    entry_price: float
    stop_loss: float
    target_1: float
    target_2: float
    risk_reward: str
    recommended_strike: int
    pattern_signal: str
    oi_flow_signal: str
    market_structure: str
    support_levels: List[float]
    resistance_levels: List[float]
    scenario_bullish: str
    scenario_bearish: str
    risk_factors: List[str]
    monitoring_checklist: List[str]
    # NEW: Multi-timeframe fields
    tf_1h_trend: str = "NEUTRAL"
    tf_15m_pattern: str = "NONE"
    tf_5m_entry: float = 0.0
    tf_alignment: str = "WEAK"

class RedisCache:
    """Redis cache for OI data with 24-hour expiry"""
    
    def __init__(self):
        self.redis_client = None
        self.connected = False
        
        if not REDIS_AVAILABLE:
            logger.warning("⚠️ Redis module not installed")
            return
        
        try:
            logger.info("🔄 Connecting to Redis...")
            self.redis_client = redis.from_url(
                REDIS_URL,
                decode_responses=True,
                socket_connect_timeout=5
            )
            self.redis_client.ping()
            self.connected = True
            logger.info("✅ Redis connected successfully!")
        except Exception as e:
            logger.error(f"❌ Redis connection failed: {e}")
            self.redis_client = None
            self.connected = False
    
    def store_option_chain(self, symbol: str, oi_data: List[OIData], spot_price: float) -> bool:
        """Store option chain data in Redis with 24h expiry"""
        try:
            if not self.redis_client or not self.connected:
                return False
            
            key = f"oi_data:{symbol}"
            value = json.dumps({
                'spot_price': spot_price,
                'strikes': [
                    {
                        'strike': oi.strike,
                        'ce_oi': oi.ce_oi,
                        'pe_oi': oi.pe_oi,
                        'ce_volume': oi.ce_volume,
                        'pe_volume': oi.pe_volume,
                        'ce_iv': oi.ce_iv,
                        'pe_iv': oi.pe_iv
                    }
                    for oi in oi_data
                ],
                'timestamp': datetime.now(IST).isoformat()
            })
            
            self.redis_client.setex(key, REDIS_EXPIRY, value)
            return True
        except Exception as e:
            logger.error(f"Redis store error: {e}")
            return False
    
    def get_oi_comparison(self, symbol: str, current_oi: List[OIData], 
                         current_price: float) -> Optional[AggregateOIAnalysis]:
        """Compare current OI with cached data"""
        try:
            if not self.redis_client or not self.connected:
                return self._calculate_aggregate_without_cache(current_oi)
            
            key = f"oi_data:{symbol}"
            cached = self.redis_client.get(key)
            
            if not cached:
                logger.info(f"{symbol}: First scan (no cache)")
                return self._calculate_aggregate_without_cache(current_oi)
            
            old_data = json.loads(cached)
            old_strikes = {s['strike']: s for s in old_data['strikes']}
            
            total_ce_oi_old = sum(s['ce_oi'] for s in old_data['strikes'])
            total_pe_oi_old = sum(s['pe_oi'] for s in old_data['strikes'])
            total_ce_volume_old = sum(s['ce_volume'] for s in old_data['strikes'])
            total_pe_volume_old = sum(s['pe_volume'] for s in old_data['strikes'])
            
            total_ce_oi_new = sum(oi.ce_oi for oi in current_oi)
            total_pe_oi_new = sum(oi.pe_oi for oi in current_oi)
            total_ce_volume_new = sum(oi.ce_volume for oi in current_oi)
            total_pe_volume_new = sum(oi.pe_volume for oi in current_oi)
            
            ce_oi_change_pct = ((total_ce_oi_new - total_ce_oi_old) / total_ce_oi_old * 100) if total_ce_oi_old > 0 else 0
            pe_oi_change_pct = ((total_pe_oi_new - total_pe_oi_old) / total_pe_oi_old * 100) if total_pe_oi_old > 0 else 0
            ce_volume_change_pct = ((total_ce_volume_new - total_ce_volume_old) / total_ce_volume_old * 100) if total_ce_volume_old > 0 else 0
            pe_volume_change_pct = ((total_pe_volume_new - total_pe_volume_old) / total_pe_volume_old * 100) if total_pe_volume_old > 0 else 0
            
            pcr = total_pe_oi_new / total_ce_oi_new if total_ce_oi_new > 0 else 0
            
            sentiment = "NEUTRAL"
            if pe_oi_change_pct > 5 and pe_oi_change_pct > ce_oi_change_pct:
                sentiment = "BULLISH"
            elif ce_oi_change_pct > 5 and ce_oi_change_pct > pe_oi_change_pct:
                sentiment = "BEARISH"
            elif pcr > 1.3:
                sentiment = "BULLISH"
            elif pcr < 0.7:
                sentiment = "BEARISH"
            
            logger.info(f"{symbol}: OI Change - CE:{ce_oi_change_pct:+.1f}% PE:{pe_oi_change_pct:+.1f}% | Sentiment:{sentiment}")
            
            return AggregateOIAnalysis(
                total_ce_oi=total_ce_oi_new,
                total_pe_oi=total_pe_oi_new,
                total_ce_volume=total_ce_volume_new,
                total_pe_volume=total_pe_volume_new,
                ce_oi_change_pct=ce_oi_change_pct,
                pe_oi_change_pct=pe_oi_change_pct,
                ce_volume_change_pct=ce_volume_change_pct,
                pe_volume_change_pct=pe_volume_change_pct,
                pcr=pcr,
                overall_sentiment=sentiment
            )
            
        except Exception as e:
            logger.error(f"Redis comparison error: {e}")
            return self._calculate_aggregate_without_cache(current_oi)
    
    def _calculate_aggregate_without_cache(self, oi_data: List[OIData]) -> AggregateOIAnalysis:
        """Calculate aggregate without comparison"""
        total_ce_oi = sum(oi.ce_oi for oi in oi_data)
        total_pe_oi = sum(oi.pe_oi for oi in oi_data)
        total_ce_volume = sum(oi.ce_volume for oi in oi_data)
        total_pe_volume = sum(oi.pe_volume for oi in oi_data)
        
        pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
        
        sentiment = "NEUTRAL"
        if pcr > 1.3:
            sentiment = "BULLISH"
        elif pcr < 0.7:
            sentiment = "BEARISH"
        
        return AggregateOIAnalysis(
            total_ce_oi=total_ce_oi,
            total_pe_oi=total_pe_oi,
            total_ce_volume=total_ce_volume,
            total_pe_volume=total_pe_volume,
            ce_oi_change_pct=0,
            pe_oi_change_pct=0,
            ce_volume_change_pct=0,
            pe_volume_change_pct=0,
            pcr=pcr,
            overall_sentiment=sentiment
        )

class UpstoxDataFetcher:
    """Upstox API - Enhanced for 400+ candles"""
    
    @staticmethod
    def get_expiries(instrument_key):
        """Fetch all available expiries"""
        headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
        encoded_key = urllib.parse.quote(instrument_key, safe='')
        url = f"{BASE_URL}/v2/option/contract?instrument_key={encoded_key}"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                contracts = resp.json().get('data', [])
                return sorted(list(set(c['expiry'] for c in contracts if 'expiry' in c)))
        except Exception as e:
            logger.error(f"Expiry fetch error: {e}")
        return []
    
    @staticmethod
    def get_next_expiry(instrument_key, expiry_day=1):
        """Auto-select nearest valid expiry"""
        expiries = UpstoxDataFetcher.get_expiries(instrument_key)
        if not expiries:
            today = datetime.now(IST)
            days_ahead = expiry_day - today.weekday()
            if days_ahead <= 0:
                days_ahead += 7
            return (today + timedelta(days=days_ahead)).strftime('%Y-%m-%d')
        
        today = datetime.now(IST).date()
        now_time = datetime.now(IST).time()
        
        future_expiries = []
        for exp_str in expiries:
            exp_date = datetime.strptime(exp_str, '%Y-%m-%d').date()
            if exp_date > today or (exp_date == today and now_time < time(15, 30)):
                future_expiries.append(exp_str)
        
        return min(future_expiries) if future_expiries else expiries[0]
    
    @staticmethod
    def get_option_chain(instrument_key, expiry):
        """Fetch full option chain"""
        headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
        encoded_key = urllib.parse.quote(instrument_key, safe='')
        url = f"{BASE_URL}/v2/option/chain?instrument_key={encoded_key}&expiry_date={expiry}"
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                strikes = resp.json().get('data', [])
                return sorted(strikes, key=lambda x: x.get('strike_price', 0))
        except Exception as e:
            logger.error(f"Chain fetch error: {e}")
        return []
    
    @staticmethod
    def get_spot_price(instrument_key):
        """Fetch current spot price"""
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
                logger.error(f"Spot price error (attempt {attempt + 1}): {e}")
                time_sleep.sleep(2)
        return 0
    
    @staticmethod
    def get_multi_timeframe_data(instrument_key, symbol) -> Optional[MultiTimeframeData]:
        """
        🔥 FETCH 400+ CANDLES AND CREATE 3 TIMEFRAMES
        
        Returns:
        - df_5m: 5-minute candles (entry/exit precision)
        - df_15m: 15-minute candles (pattern analysis) 
        - df_1h: 1-hour candles (trend confirmation)
        """
        headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
        encoded_key = urllib.parse.quote(instrument_key, safe='')
        
        all_candles = []
        
        # 1️⃣ HISTORICAL DATA (30-min for last 10 days)
        try:
            to_date = (datetime.now(IST) - timedelta(days=1)).strftime('%Y-%m-%d')
            from_date = (datetime.now(IST) - timedelta(days=10)).strftime('%Y-%m-%d')
            url = f"{BASE_URL}/v2/historical-candle/{encoded_key}/30minute/{to_date}/{from_date}"
            resp = requests.get(url, headers=headers, timeout=20)
            
            if resp.status_code == 200 and resp.json().get('status') == 'success':
                candles_30min = resp.json().get('data', {}).get('candles', [])
                all_candles.extend(candles_30min)
                logger.info(f"  📊 Historical 30m: {len(candles_30min)} candles")
        except Exception as e:
            logger.error(f"Historical candle error: {e}")
        
        # 2️⃣ INTRADAY DATA (1-min for today)
        try:
            url = f"{BASE_URL}/v2/historical-candle/intraday/{encoded_key}/1minute"
            resp = requests.get(url, headers=headers, timeout=20)
            
            if resp.status_code == 200 and resp.json().get('status') == 'success':
                candles_1min = resp.json().get('data', {}).get('candles', [])
                all_candles.extend(candles_1min)
                logger.info(f"  📊 Intraday 1m: {len(candles_1min)} candles")
        except Exception as e:
            logger.error(f"Intraday candle error: {e}")
        
        if not all_candles:
            logger.warning(f"  ❌ No candle data for {symbol}")
            return None
        
        # 3️⃣ CONVERT TO DATAFRAME
        df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').astype(float)
        df = df.sort_index()
        
        logger.info(f"  📊 Total candles: {len(df)}")
        
        # 4️⃣ RESAMPLE TO 3 TIMEFRAMES
        try:
            # 5-minute TF (Entry/Exit precision)
            df_5m = df.resample('5min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'oi': 'last'
            }).dropna()
            
            # 15-minute TF (Pattern analysis + OI)
            df_15m = df.resample('15min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'oi': 'last'
            }).dropna()
            
            # 1-hour TF (Trend confirmation)
            df_1h = df.resample('1H').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'oi': 'last'
            }).dropna()
            
            logger.info(f"  📊 Resampled: 5m={len(df_5m)}, 15m={len(df_15m)}, 1h={len(df_1h)}")
            
            # Current prices
            current_5m = df_5m['close'].iloc[-1] if len(df_5m) > 0 else 0
            current_15m = df_15m['close'].iloc[-1] if len(df_15m) > 0 else 0
            current_1h = df_1h['close'].iloc[-1] if len(df_1h) > 0 else 0
            
            # Quick 1h trend analysis
            trend_1h = "NEUTRAL"
            if len(df_1h) >= 5:
                ma20_1h = df_1h['close'].rolling(20).mean().iloc[-1]
                if current_1h > ma20_1h:
                    trend_1h = "BULLISH"
                elif current_1h < ma20_1h:
                    trend_1h = "BEARISH"
            
            return MultiTimeframeData(
                df_5m=df_5m,
                df_15m=df_15m,
                df_1h=df_1h,
                current_5m_price=current_5m,
                current_15m_price=current_15m,
                current_1h_price=current_1h,
                trend_1h=trend_1h,
                pattern_15m="ANALYZING",
                entry_5m=current_5m
            )
            
        except Exception as e:
            logger.error(f"Resample error: {e}")
            return None

class ChartGenerator:
    """Generate PNG chart with multi-timeframe view"""
    
    @staticmethod
    def create_chart(mtf_data: MultiTimeframeData, symbol: str, spot_price: float,
                    analysis: DeepAnalysis, aggregate: AggregateOIAnalysis) -> io.BytesIO:
        """Create professional multi-TF chart"""
        try:
            # Use 15min TF for main chart (100 candles)
            df_plot = mtf_data.df_15m.tail(100).copy()
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), 
                                          gridspec_kw={'height_ratios': [3, 1]})
            
            # Price chart (15min candles)
            for idx, row in df_plot.iterrows():
                color = 'green' if row['close'] > row['open'] else 'red'
                ax1.plot([idx, idx], [row['low'], row['high']], color=color, linewidth=1)
                ax1.add_patch(Rectangle(
                    (idx, min(row['open'], row['close'])),
                    timedelta(minutes=15),
                    abs(row['close'] - row['open']),
                    facecolor=color, alpha=0.7
                ))
            
            # Support levels
            for support in analysis.support_levels[:3]:
                ax1.axhline(y=support, color='green', linestyle='--', linewidth=1.5, alpha=0.6)
                ax1.text(df_plot.index[-1], support, f'  S: {support:.1f}', 
                        va='center', color='green', fontweight='bold')
            
            # Resistance levels
            for resistance in analysis.resistance_levels[:3]:
                ax1.axhline(y=resistance, color='red', linestyle='--', linewidth=1.5, alpha=0.6)
                ax1.text(df_plot.index[-1], resistance, f'  R: {resistance:.1f}', 
                        va='center', color='red', fontweight='bold')
            
            # Current price
            ax1.axhline(y=spot_price, color='blue', linestyle='-', linewidth=2)
            ax1.text(df_plot.index[-1], spot_price, f'  CMP: {spot_price:.1f}', 
                    va='center', color='blue', fontweight='bold', fontsize=10)
            
            # Entry, SL, Targets
            if analysis.opportunity != "WAIT":
                ax1.axhline(y=analysis.entry_price, color='orange', linestyle=':', linewidth=1.5)
                ax1.axhline(y=analysis.stop_loss, color='red', linestyle=':', linewidth=1.5)
                ax1.axhline(y=analysis.target_1, color='green', linestyle=':', linewidth=1.5)
                ax1.axhline(y=analysis.target_2, color='darkgreen', linestyle=':', linewidth=1.5)
            
            # Title with multi-TF info
            title = f'{symbol} | 15min Chart | 1h:{analysis.tf_1h_trend} | Score:{analysis.total_score}/125'
            ax1.set_title(title, fontsize=14, fontweight='bold')
            ax1.set_ylabel('Price', fontsize=12)
            ax1.grid(True, alpha=0.3)
            ax1.legend(['Price', 'Support', 'Resistance', 'Current'], loc='upper left')
            
            # Volume chart
            colors = ['green' if df_plot.loc[idx, 'close'] > df_plot.loc[idx, 'open'] else 'red' 
                     for idx in df_plot.index]
            ax2.bar(df_plot.index, df_plot['volume'], color=colors, alpha=0.6)
            ax2.set_ylabel('Volume', fontsize=12)
            ax2.set_xlabel('Time (15min TF)', fontsize=12)
            ax2.grid(True, alpha=0.3)
            
            # Add multi-TF info box
            signal_emoji = "🟢" if analysis.opportunity == "PE_BUY" else "🔴" if analysis.opportunity == "CE_BUY" else "⚪"
            info_text = f"""
{signal_emoji} Signal: {analysis.opportunity}
Confidence: {analysis.confidence}%
TF Alignment: {analysis.tf_alignment}

1H Trend: {analysis.tf_1h_trend}
15M Pattern: {analysis.tf_15m_pattern}
5M Entry: ₹{analysis.tf_5m_entry:.1f}

PCR: {aggregate.pcr:.2f} | {aggregate.overall_sentiment}
Entry: {analysis.entry_price:.1f} | SL: {analysis.stop_loss:.1f}
T1: {analysis.target_1:.1f} | T2: {analysis.target_2:.1f}
RR: {analysis.risk_reward}
"""
            ax1.text(0.02, 0.98, info_text.strip(), 
                    transform=ax1.transAxes,
                    fontsize=9, verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))
            
            plt.tight_layout()
            
            # Save to bytes
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100, bbox_inches='tight')
            buf.seek(0)
            plt.close(fig)
            
            return buf
            
        except Exception as e:
            logger.error(f"Chart generation error: {e}")
            return None

class OIAnalyzer:
    """Option chain analysis with Redis tracking"""
    
    def __init__(self, redis_cache: RedisCache):
        self.redis = redis_cache
    
    def parse_option_chain(self, strikes, spot_price) -> List[OIData]:
        """Convert raw option chain to OIData"""
        if not strikes:
            return []
        
        atm_strike = min(strikes, key=lambda x: abs(x.get('strike_price', 0) - spot_price))
        atm_price = atm_strike.get('strike_price', 0)
        
        oi_list = []
        for s in strikes:
            sp = s.get('strike_price', 0)
            
            if abs(sp - atm_price) > (atm_price * 0.05):
                continue
            
            ce_data = s.get('call_options', {}).get('market_data', {})
            pe_data = s.get('put_options', {}).get('market_data', {})
            
            ce_oi = ce_data.get('oi', 0)
            pe_oi = pe_data.get('oi', 0)
            
            oi_list.append(OIData(
                strike=sp,
                ce_oi=ce_oi,
                pe_oi=pe_oi,
                ce_volume=ce_data.get('volume', 0),
                pe_volume=pe_data.get('volume', 0),
                ce_iv=ce_data.get('iv', 0.0),
                pe_iv=pe_data.get('iv', 0.0),
                pcr_at_strike=pe_oi / ce_oi if ce_oi > 0 else 0
            ))
        
        return oi_list

class ChartAnalyzer:
    """Multi-timeframe chart analysis"""
    
    @staticmethod
    def analyze_1h_trend(df_1h: pd.DataFrame) -> Dict:
        """1H TREND CONFIRMATION"""
        try:
            if len(df_1h) < 20:
                return {"trend": "NEUTRAL", "strength": 0, "bias": "NONE"}
            
            recent = df_1h.tail(50)
            current = recent['close'].iloc[-1]
            
            # Moving averages
            ma20 = recent['close'].rolling(20).mean().iloc[-1]
            ma50 = recent['close'].rolling(50).mean().iloc[-1] if len(recent) >= 50 else ma20
            
            # Trend determination
            if current > ma20 > ma50:
                trend = "BULLISH"
                strength = 80
            elif current < ma20 < ma50:
                trend = "BEARISH"
                strength = 80
            elif current > ma20:
                trend = "BULLISH"
                strength = 60
            elif current < ma20:
                trend = "BEARISH"
                strength = 60
            else:
                trend = "NEUTRAL"
                strength = 40
            
            # Higher highs / Lower lows
            highs = recent['high'].values[-10:]
            lows = recent['low'].values[-10:]
            
            hh_count = sum(1 for i in range(1, len(highs)) if highs[i] > highs[i-1])
            ll_count = sum(1 for i in range(1, len(lows)) if lows[i] < lows[i-1])
            
            if hh_count > 6:
                trend = "BULLISH"
                strength = min(strength + 10, 100)
            elif ll_count > 6:
                trend = "BEARISH"
                strength = min(strength + 10, 100)
            
            return {
                "trend": trend,
                "strength": strength,
                "bias": "LONG" if trend == "BULLISH" else "SHORT" if trend == "BEARISH" else "NONE",
                "ma20": ma20,
                "current": current
            }
            
        except Exception as e:
            logger.error(f"1H trend error: {e}")
            return {"trend": "NEUTRAL", "strength": 0, "bias": "NONE"}
    
    @staticmethod
    def analyze_15m_patterns(df_15m: pd.DataFrame) -> Dict:
        """15M PATTERN DETECTION"""
        try:
            if len(df_15m) < 30:
                return {"pattern": "NONE", "signal": "NEUTRAL", "confidence": 0}
            
            recent = df_15m.tail(100)
            
            # Candlestick patterns (last 20 candles)
            last_20 = recent.tail(20)
            
            patterns_found = []
            
            # Bullish engulfing
            for i in range(1, len(last_20)):
                prev = last_20.iloc[i-1]
                curr = last_20.iloc[i]
                
                if (prev['close'] < prev['open'] and  # Previous red
                    curr['close'] > curr['open'] and  # Current green
                    curr['open'] < prev['close'] and  # Opens below prev close
                    curr['close'] > prev['open']):    # Closes above prev open
                    patterns_found.append("BULLISH_ENGULFING")
            
            # Bearish engulfing
            for i in range(1, len(last_20)):
                prev = last_20.iloc[i-1]
                curr = last_20.iloc[i]
                
                if (prev['close'] > prev['open'] and  # Previous green
                    curr['close'] < curr['open'] and  # Current red
                    curr['open'] > prev['close'] and  # Opens above prev close
                    curr['close'] < prev['open']):    # Closes below prev open
                    patterns_found.append("BEARISH_ENGULFING")
            
            # Hammer/Doji
            last_candle = last_20.iloc[-1]
            body = abs(last_candle['close'] - last_candle['open'])
            total_range = last_candle['high'] - last_candle['low']
            
            if total_range > 0:
                if body / total_range < 0.1:  # Doji
                    patterns_found.append("DOJI")
                elif (last_candle['low'] < min(last_candle['open'], last_candle['close']) - body * 2):
                    patterns_found.append("HAMMER")
            
            # Breakout detection
            high_20 = recent['high'].rolling(20).max().iloc[-1]
            low_20 = recent['low'].rolling(20).min().iloc[-1]
            current = recent['close'].iloc[-1]
            
            if current > high_20 * 0.999:
                patterns_found.append("BREAKOUT_HIGH")
            elif current < low_20 * 1.001:
                patterns_found.append("BREAKDOWN_LOW")
            
            # Determine signal
            bullish_patterns = ["BULLISH_ENGULFING", "HAMMER", "BREAKOUT_HIGH"]
            bearish_patterns = ["BEARISH_ENGULFING", "BREAKDOWN_LOW"]
            
            bullish_count = sum(1 for p in patterns_found if p in bullish_patterns)
            bearish_count = sum(1 for p in patterns_found if p in bearish_patterns)
            
            if bullish_count > bearish_count:
                signal = "BULLISH"
                confidence = min(bullish_count * 30, 90)
            elif bearish_count > bullish_count:
                signal = "BEARISH"
                confidence = min(bearish_count * 30, 90)
            else:
                signal = "NEUTRAL"
                confidence = 50
            
            pattern_str = ", ".join(patterns_found[:3]) if patterns_found else "NONE"
            
            return {
                "pattern": pattern_str,
                "signal": signal,
                "confidence": confidence,
                "patterns_found": patterns_found
            }
            
        except Exception as e:
            logger.error(f"15M pattern error: {e}")
            return {"pattern": "NONE", "signal": "NEUTRAL", "confidence": 0}
    
    @staticmethod
    def analyze_5m_entry(df_5m: pd.DataFrame) -> Dict:
        """5M ENTRY LEVEL DETECTION"""
        try:
            if len(df_5m) < 20:
                return {"entry": 0, "type": "NONE", "confidence": 0}
            
            recent = df_5m.tail(50)
            current = recent['close'].iloc[-1]
            
            # Recent support/resistance (5m micro S/R)
            highs = recent['high'].values[-20:]
            lows = recent['low'].values[-20:]
            
            # Find pullback levels
            recent_high = max(highs)
            recent_low = min(lows)
            
            # Entry logic
            if current < recent_high * 0.995 and current > recent_low * 1.005:
                # Pullback near support
                entry = current
                entry_type = "PULLBACK_LONG"
                confidence = 70
            elif current > recent_low * 1.005 and current < recent_high * 0.995:
                # Pullback near resistance
                entry = current
                entry_type = "PULLBACK_SHORT"
                confidence = 70
            else:
                entry = current
                entry_type = "MARKET"
                confidence = 50
            
            return {
                "entry": entry,
                "type": entry_type,
                "confidence": confidence,
                "recent_high": recent_high,
                "recent_low": recent_low
            }
            
        except Exception as e:
            logger.error(f"5M entry error: {e}")
            return {"entry": 0, "type": "NONE", "confidence": 0}
    
    @staticmethod
    def calculate_support_resistance(df: pd.DataFrame) -> Dict:
        """Calculate S/R from 15min data"""
        try:
            if len(df) < 50:
                current = df['close'].iloc[-1]
                return {
                    'supports': [current * 0.98],
                    'resistances': [current * 1.02]
                }
            
            recent = df.tail(100)
            current = recent['close'].iloc[-1]
            
            highs = recent['high'].values
            lows = recent['low'].values
            
            resistance_levels = []
            support_levels = []
            
            window = 5
            for i in range(window, len(recent) - window):
                if all(highs[i] >= highs[i-j] for j in range(1, window+1)) and \
                   all(highs[i] >= highs[i+j] for j in range(1, window+1)):
                    resistance_levels.append(highs[i])
                
                if all(lows[i] <= lows[i-j] for j in range(1, window+1)) and \
                   all(lows[i] <= lows[i+j] for j in range(1, window+1)):
                    support_levels.append(lows[i])
            
            def cluster(levels):
                if not levels:
                    return []
                levels = sorted(levels)
                clustered = []
                current_cluster = [levels[0]]
                for level in levels[1:]:
                    if abs(level - current_cluster[-1]) / current_cluster[-1] < 0.005:
                        current_cluster.append(level)
                    else:
                        clustered.append(np.mean(current_cluster))
                        current_cluster = [level]
                clustered.append(np.mean(current_cluster))
                return clustered
            
            resistances = cluster(resistance_levels)
            supports = cluster(support_levels)
            
            resistances = [r for r in resistances if 0.001 <= (r - current)/current <= 0.05]
            supports = [s for s in supports if 0.001 <= (current - s)/current <= 0.05]
            
            return {
                'supports': supports[:3] if supports else [current * 0.98],
                'resistances': resistances[:3] if resistances else [current * 1.02]
            }
        except:
            current = df['close'].iloc[-1]
            return {
                'supports': [current * 0.98],
                'resistances': [current * 1.02]
            }

class AIAnalyzer:
    """DeepSeek AI with MULTI-TIMEFRAME analysis"""
    
    @staticmethod
    def extract_json(content: str) -> Optional[Dict]:
        """Extract JSON from AI response"""
        try:
            try:
                return json.loads(content)
            except:
                pass
            
            patterns = [
                r'```json\s*(\{.*?\})\s*```',
                r'```\s*(\{.*?\})\s*```',
                r'(\{[^{]*?"opportunity".*?\})',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, content, re.DOTALL)
                if match:
                    try:
                        return json.loads(match.group(1))
                    except:
                        continue
            
            start_idx = content.find('{')
            if start_idx != -1:
                brace_count = 0
                for i in range(start_idx, len(content)):
                    if content[i] == '{':
                        brace_count += 1
                    elif content[i] == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            try:
                                return json.loads(content[start_idx:i+1])
                            except:
                                break
            
            return None
        except:
            return None
    
    @staticmethod
    def deep_multi_tf_analysis(symbol: str, spot_price: float, mtf_data: MultiTimeframeData,
                               aggregate: AggregateOIAnalysis, trend_1h: Dict, pattern_15m: Dict,
                               entry_5m: Dict, sr_levels: Dict) -> Optional[DeepAnalysis]:
        """
        🔥 DEEP MULTI-TIMEFRAME ANALYSIS
        
        AI gets complete context:
        - 1H: Trend confirmation
        - 15M: Pattern detection + OI
        - 5M: Entry precision
        """
        try:
            url = "https://api.deepseek.com/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json"
            }
            
            prompt = f"""You are an expert F&O multi-timeframe trader. Analyze {symbol} across 3 timeframes.

SPOT PRICE: ₹{spot_price:.2f}

🕐 1-HOUR TIMEFRAME (TREND CONFIRMATION):
- Trend: {trend_1h['trend']} (Strength: {trend_1h['strength']}%)
- Bias: {trend_1h['bias']}
- MA20: ₹{trend_1h.get('ma20', spot_price):.2f}
- Current: ₹{trend_1h.get('current', spot_price):.2f}
📊 Analysis: Higher timeframe sets the directional bias. Only take trades aligned with 1H trend.

⏰ 15-MINUTE TIMEFRAME (PATTERN + OI):
- Patterns: {pattern_15m['pattern']}
- Signal: {pattern_15m['signal']} (Confidence: {pattern_15m['confidence']}%)
- Support: {', '.join([f"₹{s:.0f}" for s in sr_levels['supports'][:3]])}
- Resistance: {', '.join([f"₹{r:.0f}" for r in sr_levels['resistances'][:3]])}
📊 Analysis: Main signal timeframe. Look for breakouts, reversals, candlestick patterns.

⏱️ 5-MINUTE TIMEFRAME (ENTRY/EXIT):
- Entry Level: ₹{entry_5m['entry']:.2f}
- Entry Type: {entry_5m['type']}
- Recent High: ₹{entry_5m.get('recent_high', spot_price):.2f}
- Recent Low: ₹{entry_5m.get('recent_low', spot_price):.2f}
📊 Analysis: Precise entry on pullbacks. Wait for 5M confirmation before entry.

📈 OPTIONS DATA (15M ALIGNED):
- PCR: {aggregate.pcr:.2f}
- CE OI Change: {aggregate.ce_oi_change_pct:+.2f}% | Volume: {aggregate.ce_volume_change_pct:+.2f}%
- PE OI Change: {aggregate.pe_oi_change_pct:+.2f}% | Volume: {aggregate.pe_volume_change_pct:+.2f}%
- Sentiment: {aggregate.overall_sentiment}

🎯 MULTI-TIMEFRAME ALIGNMENT RULES:
1. 1H Trend MUST align with trade direction (Bullish 1H = PE_BUY only)
2. 15M Pattern confirms the setup (engulfing, breakout, S/R test)
3. 5M Entry gives precise trigger (pullback, breakout confirmation)
4. OI Flow supports the direction
5. ALL 4 must align for HIGH confidence trade

SCORING (Total /125):
- Chart Analysis: /50 (All 3 TF structure + patterns)
- Options Analysis: /50 (PCR, OI flow, sentiment)
- TF Alignment: /25 (How well all timeframes align)

Reply ONLY JSON:
{{
  "opportunity": "PE_BUY" or "CE_BUY" or "WAIT",
  "confidence": 82,
  "chart_score": 42,
  "option_score": 45,
  "alignment_score": 22,
  "total_score": 109,
  "entry_price": {entry_5m['entry']:.2f},
  "stop_loss": {entry_5m['entry'] * 0.995:.2f},
  "target_1": {entry_5m['entry'] * 1.015:.2f},
  "target_2": {entry_5m['entry'] * 1.025:.2f},
  "risk_reward": "1:2.5",
  "recommended_strike": {int(spot_price)},
  "pattern_signal": "Detailed explanation of 15M pattern + why it's valid",
  "oi_flow_signal": "OI analysis + how it confirms direction",
  "market_structure": "Multi-TF structure explanation",
  "support_levels": {sr_levels['supports'][:2]},
  "resistance_levels": {sr_levels['resistances'][:2]},
  "scenario_bullish": "If 1H stays bullish + 15M breaks R + 5M holds support = target X",
  "scenario_bearish": "If 1H turns bearish + 15M breaks S + 5M confirms = target Y",
  "risk_factors": ["Risk1", "Risk2", "Risk3"],
  "monitoring_checklist": ["Monitor 1H trend", "Watch 15M S/R", "5M entry trigger"],
  "tf_1h_trend": "{trend_1h['trend']}",
  "tf_15m_pattern": "{pattern_15m['pattern']}",
  "tf_5m_entry": {entry_5m['entry']:.2f},
  "tf_alignment": "STRONG" or "MODERATE" or "WEAK"
}}

Be brutally honest. If TF alignment weak, say "WAIT". Only trade when ALL timeframes agree."""

            payload = {
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "Expert multi-TF F&O trader. Reply JSON only."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_tokens": 1800
            }
            
            response = requests.post(url, json=payload, headers=headers, timeout=45)
            
            if response.status_code != 200:
                return None
            
            result = response.json()
            content = result['choices'][0]['message']['content'].strip()
            
            analysis_dict = AIAnalyzer.extract_json(content)
            
            if not analysis_dict:
                return None
            
            required = ['opportunity', 'confidence', 'chart_score', 'option_score', 'alignment_score']
            if not all(f in analysis_dict for f in required):
                return None
            
            return DeepAnalysis(
                opportunity=analysis_dict['opportunity'],
                confidence=analysis_dict['confidence'],
                chart_score=analysis_dict['chart_score'],
                option_score=analysis_dict['option_score'],
                alignment_score=analysis_dict['alignment_score'],
                total_score=analysis_dict.get('total_score',
                    analysis_dict['chart_score'] + analysis_dict['option_score'] + analysis_dict['alignment_score']),
                entry_price=analysis_dict.get('entry_price', spot_price),
                stop_loss=analysis_dict.get('stop_loss', spot_price * 0.995),
                target_1=analysis_dict.get('target_1', spot_price * 1.01),
                target_2=analysis_dict.get('target_2', spot_price * 1.02),
                risk_reward=analysis_dict.get('risk_reward', '1:2'),
                recommended_strike=analysis_dict.get('recommended_strike', int(spot_price)),
                pattern_signal=analysis_dict.get('pattern_signal', 'N/A'),
                oi_flow_signal=analysis_dict.get('oi_flow_signal', 'N/A'),
                market_structure=analysis_dict.get('market_structure', 'Multi-TF'),
                support_levels=analysis_dict.get('support_levels', sr_levels['supports'][:2]),
                resistance_levels=analysis_dict.get('resistance_levels', sr_levels['resistances'][:2]),
                scenario_bullish=analysis_dict.get('scenario_bullish', 'N/A'),
                scenario_bearish=analysis_dict.get('scenario_bearish', 'N/A'),
                risk_factors=analysis_dict.get('risk_factors', ['See analysis']),
                monitoring_checklist=analysis_dict.get('monitoring_checklist', ['Monitor price']),
                tf_1h_trend=analysis_dict.get('tf_1h_trend', trend_1h['trend']),
                tf_15m_pattern=analysis_dict.get('tf_15m_pattern', pattern_15m['pattern']),
                tf_5m_entry=analysis_dict.get('tf_5m_entry', entry_5m['entry']),
                tf_alignment=analysis_dict.get('tf_alignment', 'WEAK')
            )
            
        except Exception as e:
            logger.error(f"Deep multi-TF analysis error: {e}")
            return None

class TelegramNotifier:
    """Telegram with multi-TF alerts"""
    
    def __init__(self, redis_connected: bool):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.redis_connected = redis_connected
    
    async def send_startup_message(self):
        """Send bot startup notification"""
        try:
            redis_status = "🟢 Connected" if self.redis_connected else "🔴 Not Connected"
            redis_note = "(OI tracking enabled)" if self.redis_connected else "(OI tracking disabled)"
            
            sep = "=" * 40
            msg = f"""🔥 HYBRID BOT v13.0 - MULTI-TF BEAST 🔥

{sep}
✅ MULTI-TIMEFRAME STRATEGY:
   5min: Entry/Exit precision
   15min: Patterns + OI analysis
   1hr: Trend confirmation

✅ 400+ CANDLESTICKS:
   Historical: 30min (10 days)
   Intraday: 1min (today)
   Auto-resample: 5m/15m/1h

✅ AI GETS ALL 3 TIMEFRAMES:
   1h Trend → 15m Pattern → 5m Entry

{sep}
REDIS: {redis_status} {redis_note}
{sep}

📊 Monitoring:
   • {len(INDICES)} Indices
   • {len(SELECTED_STOCKS)} Stocks

⏰ Scan: 15 minutes

{sep}
ALERTS: TEXT + PNG CHART 📈
Multi-TF Analysis Enabled! 🚀
{sep}

Status: 🟢 RUNNING
Market: 9:15 AM - 3:30 PM IST
{sep}"""
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=msg
            )
            logger.info("✅ Startup message sent!")
        except Exception as e:
            logger.error(f"Startup message error: {e}")
    
    async def send_alert(self, symbol: str, spot_price: float, analysis: DeepAnalysis,
                        aggregate: AggregateOIAnalysis, expiry: str, mtf_data: MultiTimeframeData):
        """Send multi-TF alert with chart"""
        try:
            signal_map = {
                "PE_BUY": ("🟢", "PE BUY (Bullish)"),
                "CE_BUY": ("🔴", "CE BUY (Bearish)")
            }
            
            signal_emoji, signal_text = signal_map.get(analysis.opportunity, ("⚪", "WAIT"))
            
            ist_time = datetime.now(IST).strftime('%H:%M:%S')
            sep = "=" * 40
            
            # Multi-TF alert
            alert = f"""🎯 MULTI-TF SIGNAL - {symbol}

{signal_emoji} {signal_text}

{sep}
CONFIDENCE & SCORING
{sep}
Confidence: {analysis.confidence}%
Total Score: {analysis.total_score}/125
   Chart: {analysis.chart_score}/50
   Options: {analysis.option_score}/50
   TF Alignment: {analysis.alignment_score}/25

TF Alignment: {analysis.tf_alignment}

{sep}
MULTI-TIMEFRAME ANALYSIS
{sep}
🕐 1H Trend: {analysis.tf_1h_trend}
⏰ 15M Pattern: {analysis.tf_15m_pattern}
⏱️ 5M Entry: ₹{analysis.tf_5m_entry:.1f}

{sep}
TRADE SETUP
{sep}
💰 Spot: ₹{spot_price:.2f}
📍 Entry: ₹{analysis.entry_price:.2f}
🛑 Stop Loss: ₹{analysis.stop_loss:.2f}
🎯 Target 1: ₹{analysis.target_1:.2f}
🎯 Target 2: ₹{analysis.target_2:.2f}
📊 Risk:Reward: {analysis.risk_reward}
🎲 Strike: {analysis.recommended_strike}

{sep}
SUPPORT & RESISTANCE
{sep}
Support: {', '.join([f"₹{s:.1f}" for s in analysis.support_levels[:2]])}
Resistance: {', '.join([f"₹{r:.1f}" for r in analysis.resistance_levels[:2]])}

{sep}
OPTIONS DATA (REDIS TRACKED)
{sep}
PCR: {aggregate.pcr:.2f}
CE OI: {aggregate.ce_oi_change_pct:+.1f}% | Vol: {aggregate.ce_volume_change_pct:+.1f}%
PE OI: {aggregate.pe_oi_change_pct:+.1f}% | Vol: {aggregate.pe_volume_change_pct:+.1f}%
Sentiment: {aggregate.overall_sentiment}

{sep}
SIGNALS BREAKDOWN
{sep}
📊 Chart Pattern (15M):
{analysis.pattern_signal[:200]}

⛓️ OI Flow Analysis:
{analysis.oi_flow_signal[:200]}

{sep}
SCENARIOS
{sep}
🟢 Bullish Path:
{analysis.scenario_bullish[:180]}

🔴 Bearish Path:
{analysis.scenario_bearish[:180]}

{sep}
RISK FACTORS
{sep}"""
            
            for i, risk in enumerate(analysis.risk_factors[:3], 1):
                alert += f"\n⚠️ {risk[:120]}"
            
            alert += f"\n\n{sep}\nMONITORING CHECKLIST\n{sep}"
            
            for i, check in enumerate(analysis.monitoring_checklist[:3], 1):
                alert += f"\n✓ {check[:120]}"
            
            alert += f"\n\n{sep}"
            alert += f"\n📅 Expiry: {expiry}"
            alert += f"\n⏰ Time: {ist_time} IST"
            alert += f"\n🤖 AI: DeepSeek Multi-TF | v13.0"
            alert += f"\n{sep}"
            
            # Send text alert
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=alert
            )
            
            # Generate and send multi-TF chart
            chart_buf = ChartGenerator.create_chart(mtf_data, symbol, spot_price, analysis, aggregate)
            if chart_buf:
                await self.bot.send_photo(
                    chat_id=TELEGRAM_CHAT_ID,
                    photo=chart_buf,
                    caption=f"📈 {symbol} | 15min Chart | {signal_emoji} {signal_text}\n1H:{analysis.tf_1h_trend} | TF:{analysis.tf_alignment}"
                )
                logger.info(f"✅ Multi-TF chart sent for {symbol}")
            
            logger.info(f"✅ Multi-TF alert sent: {symbol} - {analysis.opportunity}")
            
        except Exception as e:
            logger.error(f"Alert error: {e}")
            logger.error(traceback.format_exc())
    
    async def send_cycle_summary(self, total_scanned: int, alerts_sent: int):
        """Send scan cycle summary"""
        try:
            msg = f"""📊 MULTI-TF SCAN COMPLETE

Deep Analysis: {total_scanned} instruments
Alerts Sent: {alerts_sent}

Strategy: 1H→15M→5M alignment
⏰ Next scan in 15 minutes..."""
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=msg
            )
        except Exception as e:
            logger.error(f"Summary error: {e}")

class HybridTradingBot:
    """Main bot with MULTI-TIMEFRAME strategy"""
    
    def __init__(self):
        logger.info("Initializing Hybrid Trading Bot v13.0 - Multi-TF...")
        
        self.redis = RedisCache()
        self.fetcher = UpstoxDataFetcher()
        self.oi_analyzer = OIAnalyzer(self.redis)
        self.chart_analyzer = ChartAnalyzer()
        self.ai_analyzer = AIAnalyzer()
        self.notifier = TelegramNotifier(self.redis.connected)
        
        self.total_scanned = 0
        self.alerts_sent = 0
        
        logger.info(f"✅ Multi-TF Bot initialized! Redis: {self.redis.connected}")
    
    def is_market_open(self) -> bool:
        """Check if market is open"""
        now_ist = datetime.now(IST)
        current_time = now_ist.strftime("%H:%M")
        
        if now_ist.weekday() >= 5:
            return False
        
        return "09:15" <= current_time <= "15:30"
    
    async def deep_multi_tf_scan(self, instruments: Dict):
        """
        🔥 DEEP MULTI-TIMEFRAME SCAN
        
        For each instrument:
        1. Fetch 400+ candles
        2. Create 3 timeframes (5m, 15m, 1h)
        3. Analyze each TF separately
        4. Check alignment
        5. AI makes final decision
        6. Send alert with multi-TF chart
        """
        
        logger.info("\n" + "="*70)
        logger.info(f"DEEP MULTI-TF SCAN ({len(instruments)} instruments)")
        logger.info("="*70)
        
        alerts_before = self.alerts_sent
        
        for idx, (key, info) in enumerate(instruments.items(), 1):
            try:
                self.total_scanned += 1
                
                if isinstance(info, dict) and 'name' in info:
                    symbol = info['name']
                    expiry_day = info.get('expiry_day', 3)
                else:
                    symbol = info
                    expiry_day = 3
                
                logger.info(f"\n[{idx}/{len(instruments)}] 🔍 Analyzing: {symbol}")
                
                # 1️⃣ GET SPOT PRICE
                spot_price = self.fetcher.get_spot_price(key)
                if spot_price == 0:
                    logger.warning(f"❌ {symbol}: Failed to get spot price")
                    continue
                
                logger.info(f"  ✅ Spot: ₹{spot_price:.2f}")
                
                # 2️⃣ GET EXPIRY
                expiry = self.fetcher.get_next_expiry(key, expiry_day)
                logger.info(f"  📅 Expiry: {expiry}")
                
                # 3️⃣ GET OPTION CHAIN
                strikes = self.fetcher.get_option_chain(key, expiry)
                if not strikes or len(strikes) < 10:
                    logger.warning(f"  ❌ {symbol}: Insufficient option data")
                    continue
                
                logger.info(f"  📤 Option chain: {len(strikes)} strikes")
                
                # 4️⃣ PARSE OI DATA
                oi_data = self.oi_analyzer.parse_option_chain(strikes, spot_price)
                if not oi_data:
                    logger.warning(f"  ❌ {symbol}: No OI data")
                    continue
                
                # 5️⃣ GET AGGREGATE WITH REDIS
                aggregate = self.redis.get_oi_comparison(symbol, oi_data, spot_price)
                if not aggregate:
                    logger.warning(f"  ❌ {symbol}: No aggregate data")
                    continue
                
                # Store current OI
                self.redis.store_option_chain(symbol, oi_data, spot_price)
                
                logger.info(f"  📊 PCR: {aggregate.pcr:.2f} | Sentiment: {aggregate.overall_sentiment}")
                
                # 6️⃣ GET MULTI-TIMEFRAME DATA (400+ candles)
                mtf_data = self.fetcher.get_multi_timeframe_data(key, symbol)
                if mtf_data is None:
                    logger.warning(f"  ❌ {symbol}: Failed to get multi-TF data")
                    continue
                
                logger.info(f"  📊 Multi-TF data: 5m={len(mtf_data.df_5m)}, 15m={len(mtf_data.df_15m)}, 1h={len(mtf_data.df_1h)}")
                
                # 7️⃣ ANALYZE EACH TIMEFRAME
                
                # 1H Trend
                trend_1h = self.chart_analyzer.analyze_1h_trend(mtf_data.df_1h)
                logger.info(f"  🕐 1H: {trend_1h['trend']} (Strength: {trend_1h['strength']}%)")
                
                # 15M Patterns
                pattern_15m = self.chart_analyzer.analyze_15m_patterns(mtf_data.df_15m)
                logger.info(f"  ⏰ 15M: {pattern_15m['signal']} | Pattern: {pattern_15m['pattern']}")
                
                # 5M Entry
                entry_5m = self.chart_analyzer.analyze_5m_entry(mtf_data.df_5m)
                logger.info(f"  ⏱️ 5M: Entry ₹{entry_5m['entry']:.2f} | Type: {entry_5m['type']}")
                
                # Support/Resistance (from 15M)
                sr_levels = self.chart_analyzer.calculate_support_resistance(mtf_data.df_15m)
                logger.info(f"  📊 S/R: {len(sr_levels['supports'])} supports, {len(sr_levels['resistances'])} resistances")
                
                # 8️⃣ CHECK BASIC ALIGNMENT
                tf_aligned = False
                
                # Bullish alignment: 1H bullish + 15M bullish + 5M entry valid
                if (trend_1h['trend'] == 'BULLISH' and 
                    pattern_15m['signal'] == 'BULLISH' and
                    aggregate.overall_sentiment in ['BULLISH', 'NEUTRAL']):
                    tf_aligned = True
                    logger.info(f"  ✅ Bullish alignment detected!")
                
                # Bearish alignment: 1H bearish + 15M bearish + 5M entry valid
                elif (trend_1h['trend'] == 'BEARISH' and 
                      pattern_15m['signal'] == 'BEARISH' and
                      aggregate.overall_sentiment in ['BEARISH', 'NEUTRAL']):
                    tf_aligned = True
                    logger.info(f"  ✅ Bearish alignment detected!")
                
                if not tf_aligned:
                    logger.info(f"  ❌ {symbol}: TF not aligned (1H:{trend_1h['trend']} | 15M:{pattern_15m['signal']} | OI:{aggregate.overall_sentiment})")
                    continue
                
                # 9️⃣ AI DEEP ANALYSIS (with all TF context)
                deep = self.ai_analyzer.deep_multi_tf_analysis(
                    symbol, spot_price, mtf_data, aggregate, 
                    trend_1h, pattern_15m, entry_5m, sr_levels
                )
                
                if not deep:
                    logger.warning(f"  ❌ {symbol}: AI analysis failed")
                    continue
                
                logger.info(f"  🤖 AI: {deep.opportunity} | Score={deep.total_score}/125 | Conf={deep.confidence}% | TF:{deep.tf_alignment}")
                
                # 🔟 APPLY FILTERS
                if deep.opportunity == "WAIT":
                    logger.info(f"  ❌ {symbol}: AI says WAIT")
                    continue
                
                if deep.confidence < CONFIDENCE_MIN:
                    logger.info(f"  ❌ {symbol}: Confidence {deep.confidence}% < {CONFIDENCE_MIN}%")
                    continue
                
                if deep.total_score < SCORE_MIN:
                    logger.info(f"  ❌ {symbol}: Score {deep.total_score} < {SCORE_MIN}")
                    continue
                
                if deep.alignment_score < ALIGNMENT_MIN:
                    logger.info(f"  ❌ {symbol}: Alignment {deep.alignment_score} < {ALIGNMENT_MIN}")
                    continue
                
                # TF alignment filter
                if deep.tf_alignment == "WEAK":
                    logger.info(f"  ❌ {symbol}: Weak TF alignment")
                    continue
                
                # Time filter
                now_ist = datetime.now(IST)
                hour = now_ist.hour
                minute = now_ist.minute
                
                if hour == 9 and minute < 25:
                    logger.info(f"  ❌ {symbol}: Opening period")
                    continue
                
                if hour == 15 or (hour == 14 and minute >= 40):
                    logger.info(f"  ❌ {symbol}: Closing period")
                    continue
                
                logger.info(f"  ✅ {symbol}: ALL FILTERS PASSED! 🚀")
                
                # 1️⃣1️⃣ SEND MULTI-TF ALERT
                await self.notifier.send_alert(symbol, spot_price, deep, aggregate, expiry, mtf_data)
                
                self.alerts_sent += 1
                
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"❌ Multi-TF scan error {key}: {e}")
                logger.error(traceback.format_exc())
            
            await asyncio.sleep(1)
        
        alerts_this_cycle = self.alerts_sent - alerts_before
        
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ MULTI-TF CYCLE COMPLETE: {alerts_this_cycle} alerts sent")
        logger.info(f"{'='*70}\n")
        
        return alerts_this_cycle
    
    async def run_scan_cycle(self):
        """Run complete multi-TF scan cycle"""
        logger.info(f"\n{'='*70}")
        logger.info(f"🔄 MULTI-TF SCAN START - {datetime.now(IST).strftime('%H:%M:%S IST')}")
        logger.info(f"{'='*70}")
        
        scan_count_before = self.total_scanned
        
        # Combine all instruments
        all_instruments = {**INDICES, **SELECTED_STOCKS}
        
        # Deep multi-TF analysis
        alerts_sent = await self.deep_multi_tf_scan(all_instruments)
        
        # Send summary
        instruments_scanned = self.total_scanned - scan_count_before
        await self.notifier.send_cycle_summary(instruments_scanned, alerts_sent)
        
        logger.info(f"\n{'='*70}")
        logger.info(f"✅ MULTI-TF CYCLE COMPLETE")
        logger.info(f"{'='*70}\n")
    
    async def run(self):
        """Main bot loop"""
        logger.info("="*70)
        logger.info("HYBRID TRADING BOT v13.0 - MULTI-TIMEFRAME BEAST")
        logger.info("="*70)
        
        # Check credentials
        missing = []
        for cred in ['UPSTOX_ACCESS_TOKEN', 'TELEGRAM_BOT_TOKEN',
                     'TELEGRAM_CHAT_ID', 'DEEPSEEK_API_KEY']:
            if not globals().get(cred):
                missing.append(cred)
        
        if missing:
            logger.error(f"Missing: {', '.join(missing)}")
            return
        
        await self.notifier.send_startup_message()
        
        logger.info("="*70)
        logger.info(f"🟢 Multi-TF Bot RUNNING - Redis: {self.redis.connected}")
        logger.info("="*70)
        
        while True:
            try:
                if not self.is_market_open():
                    logger.info("Market closed. Waiting...")
                    await asyncio.sleep(60)
                    continue
                
                await self.run_scan_cycle()
                
                logger.info(f"⏳ Next multi-TF scan in {SCAN_INTERVAL // 60} minutes...")
                await asyncio.sleep(SCAN_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("Stopped by user")
                break
            except Exception as e:
                logger.error(f"Loop error: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(60)

async def main():
    """Entry point"""
    try:
        bot = HybridTradingBot()
        await bot.run()
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    logger.info("="*70)
    logger.info("HYBRID BOT v13.0 - MULTI-TIMEFRAME BEAST STARTING...")
    logger.info("="*70)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n✅ Shutdown complete")
    except Exception as e:
        logger.error(f"\n❌ Critical error: {e}")
        logger.error(traceback.format_exc())
