#!/usr/bin/env python3
"""
HYBRID TRADING BOT v16.1 - FIXED SIGNAL LOGIC
==================================================================
✅ CRITICAL FIX: Timeframe alignment > OI priority
✅ Strict rules: BULLISH chart → only PE_BUY, BEARISH → only CE_BUY
✅ News filtering: Only relevant sector news
✅ Conflict handling: Chart + OI conflict → WAIT
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
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
BASE_URL = "https://api.upstox.com"
IST = pytz.timezone('Asia/Kolkata')

# ✅ ONLY BANK NIFTY & MIDCAP NIFTY
INDICES = {
    "NSE_INDEX|Nifty Bank": {"name": "BANK NIFTY", "expiry_day": 2, "oi_timeframe": "30min", "sector": "INDEX"},
    "NSE_INDEX|NIFTY MID SELECT": {"name": "MIDCAP NIFTY", "expiry_day": 0, "oi_timeframe": "30min", "sector": "INDEX"}
}

# ✅ F&O STOCKS with sector mapping for news filtering
SELECTED_STOCKS = {
    # 🔵 LIQUID STOCKS (2hr OI comparison)
    "NSE_EQ|INE002A01018": {"name": "RELIANCE", "oi_timeframe": "2hr", "sector": "OIL_GAS"},
    "NSE_EQ|INE040A01034": {"name": "HDFCBANK", "oi_timeframe": "2hr", "sector": "BANKING"},
    "NSE_EQ|INE009A01021": {"name": "INFY", "oi_timeframe": "2hr", "sector": "IT"},
    "NSE_EQ|INE090A01021": {"name": "ICICIBANK", "oi_timeframe": "2hr", "sector": "BANKING"},
    "NSE_EQ|INE854D01024": {"name": "TCS", "oi_timeframe": "2hr", "sector": "IT"},
    
    # 🟢 AUTO (30min)
    "NSE_EQ|INE467B01029": {"name": "TATAMOTORS", "oi_timeframe": "30min", "sector": "AUTO"},
    "NSE_EQ|INE585B01010": {"name": "MARUTI", "oi_timeframe": "30min", "sector": "AUTO"},
    "NSE_EQ|INE917I01010": {"name": "BAJAJ-AUTO", "oi_timeframe": "30min", "sector": "AUTO"},
    
    # 🟢 BANKS (30min)
    "NSE_EQ|INE062A01020": {"name": "SBIN", "oi_timeframe": "30min", "sector": "BANKING"},
    "NSE_EQ|INE238A01034": {"name": "AXISBANK", "oi_timeframe": "30min", "sector": "BANKING"},
    "NSE_EQ|INE237A01028": {"name": "KOTAKBANK", "oi_timeframe": "30min", "sector": "BANKING"},
    
    # 🔵 METALS (2hr)
    "NSE_EQ|INE155A01022": {"name": "TATASTEEL", "oi_timeframe": "2hr", "sector": "METALS"},
    "NSE_EQ|INE019A01038": {"name": "JSWSTEEL", "oi_timeframe": "2hr", "sector": "METALS"},
    
    # 🟢 OIL & GAS (30min)
    "NSE_EQ|INE213A01029": {"name": "ONGC", "oi_timeframe": "30min", "sector": "OIL_GAS"},
    
    # 🔵 IT (2hr)
    "NSE_EQ|INE075A01022": {"name": "WIPRO", "oi_timeframe": "2hr", "sector": "IT"},
    "NSE_EQ|INE047A01021": {"name": "HCLTECH", "oi_timeframe": "2hr", "sector": "IT"},
    
    # 🟢 PHARMA (30min)
    "NSE_EQ|INE044A01036": {"name": "SUNPHARMA", "oi_timeframe": "30min", "sector": "PHARMA"},
    
    # 🟢 FMCG (30min)
    "NSE_EQ|INE154A01025": {"name": "ITC", "oi_timeframe": "30min", "sector": "FMCG"},
    
    # 🟢 INFRA/POWER (30min)
    "NSE_EQ|INE742F01042": {"name": "ADANIPORTS", "oi_timeframe": "30min", "sector": "INFRA"},
    "NSE_EQ|INE018A01030": {"name": "LT", "oi_timeframe": "30min", "sector": "INFRA"},
    
    # 🟢 RETAIL/CONSUMER (30min)
    "NSE_EQ|INE280A01028": {"name": "TITAN", "oi_timeframe": "30min", "sector": "RETAIL"},
    
    # 🟢 INSURANCE (30min)
    "NSE_EQ|INE860A01027": {"name": "HDFCLIFE", "oi_timeframe": "30min", "sector": "INSURANCE"},
    
    # 🟢 OTHERS (30min)
    "NSE_EQ|INE397D01024": {"name": "BHARTIARTL", "oi_timeframe": "30min", "sector": "TELECOM"},
    "NSE_EQ|INE918I01026": {"name": "BAJAJFINSV", "oi_timeframe": "30min", "sector": "BANKING"}
}

# ✅ NEW: Sector keywords for news filtering
SECTOR_KEYWORDS = {
    "BANKING": ["bank", "hdfc", "icici", "sbi", "axis", "kotak", "rbi", "lending", "npa"],
    "IT": ["tech", "software", "it", "tcs", "infosys", "wipro", "hcl", "outsourcing"],
    "AUTO": ["auto", "car", "vehicle", "tata motors", "maruti", "bajaj"],
    "FMCG": ["fmcg", "consumer", "itc", "cigarette", "tobacco", "food"],
    "PHARMA": ["pharma", "drug", "medicine", "sun pharma", "healthcare"],
    "METALS": ["steel", "metal", "tata steel", "jsw", "iron", "copper"],
    "OIL_GAS": ["oil", "gas", "reliance", "ongc", "petroleum", "crude"],
    "INFRA": ["infra", "construction", "adani", "l&t", "ports"],
    "TELECOM": ["telecom", "bharti", "airtel", "vodafone", "spectrum"],
    "RETAIL": ["retail", "titan", "jewellery", "consumer goods"],
    "INSURANCE": ["insurance", "life", "hdfc life", "policy"],
    "INDEX": ["nifty", "sensex", "market", "india", "stocks"]
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
    timeframe: str = "30min"

@dataclass
class MultiTimeframeData:
    """Container for all timeframe data"""
    df_5m: pd.DataFrame
    df_15m: pd.DataFrame
    df_1h: pd.DataFrame
    current_5m_price: float
    current_15m_price: float
    current_1h_price: float
    trend_1h: str
    pattern_15m: str
    entry_5m: float

@dataclass
class NewsData:
    """Container for news analysis"""
    headline: str
    summary: str
    sentiment: str
    impact_score: int
    source: str
    url: str
    published_time: str
    is_relevant: bool = False  # ✅ NEW

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
    tf_1h_trend: str = "NEUTRAL"
    tf_15m_pattern: str = "NONE"
    tf_5m_entry: float = 0.0
    tf_alignment: str = "WEAK"
    news_sentiment: str = "NEUTRAL"
    news_impact: int = 0
    oi_timeframe_used: str = "30min"
    signal_reason: str = ""  # ✅ NEW: Why this signal was chosen

class FinnhubNewsAPI:
    """Finnhub API for real-time news"""
    
    def __init__(self):
        self.api_key = FINNHUB_API_KEY
        self.base_url = "https://finnhub.io/api/v1"
        self.connected = self.test_connection()
    
    def test_connection(self) -> bool:
        """Test Finnhub API connection"""
        try:
            if not self.api_key:
                logger.warning("⚠️ Finnhub API key not found")
                return False
            
            url = f"{self.base_url}/news?category=general&token={self.api_key}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                logger.info("✅ Finnhub API connected successfully!")
                return True
            else:
                logger.error(f"❌ Finnhub API error: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ Finnhub connection failed: {e}")
            return False
    
    def get_market_news(self, limit: int = 10) -> List[Dict]:
        """Get general market news"""
        try:
            if not self.connected:
                return []
            
            url = f"{self.base_url}/news?category=general&token={self.api_key}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                news_list = response.json()[:limit]
                logger.info(f"  📰 Fetched {len(news_list)} market news items")
                return news_list
            else:
                logger.error(f"Market news error: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"Market news fetch error: {e}")
            return []

class RedisCache:
    """Redis cache for OI data with smart timeframe tracking"""
    
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
    
    def store_option_chain(self, symbol: str, oi_data: List[OIData], spot_price: float, 
                          oi_timeframe: str = "30min") -> bool:
        """Store option chain in appropriate timeframe key"""
        try:
            if not self.redis_client or not self.connected:
                return False
            
            data_json = json.dumps({
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
            
            key = f"oi_{oi_timeframe}:{symbol}"
            expiry_seconds = 1800 if oi_timeframe == "30min" else 7200
            
            self.redis_client.setex(key, expiry_seconds, data_json)
            
            logger.info(f"  💾 Redis: Stored {symbol} ({oi_timeframe})")
            return True
        except Exception as e:
            logger.error(f"Redis store error: {e}")
            return False
    
    def get_oi_comparison(self, symbol: str, current_oi: List[OIData], 
                         current_price: float, oi_timeframe: str = "30min") -> Optional[AggregateOIAnalysis]:
        """Compare current OI with cached data for specified timeframe"""
        try:
            if not self.redis_client or not self.connected:
                return self._calculate_aggregate_without_cache(current_oi, oi_timeframe)
            
            key = f"oi_{oi_timeframe}:{symbol}"
            cached = self.redis_client.get(key)
            
            if not cached:
                logger.info(f"  📊 {symbol}: First scan ({oi_timeframe})")
                return self._calculate_aggregate_without_cache(current_oi, oi_timeframe)
            
            old_data = json.loads(cached)
            
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
            
            # Smart sentiment detection
            sentiment = "NEUTRAL"
            if pe_oi_change_pct > 5 and pe_oi_change_pct > ce_oi_change_pct:
                sentiment = "BULLISH"
            elif ce_oi_change_pct > 5 and ce_oi_change_pct > pe_oi_change_pct:
                sentiment = "BEARISH"
            elif pcr > 1.3:
                sentiment = "BULLISH"
            elif pcr < 0.7:
                sentiment = "BEARISH"
            
            logger.info(f"  📊 {symbol} OI ({oi_timeframe}): CE:{ce_oi_change_pct:+.1f}% PE:{pe_oi_change_pct:+.1f}% | {sentiment}")
            
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
                overall_sentiment=sentiment,
                timeframe=oi_timeframe
            )
            
        except Exception as e:
            logger.error(f"OI comparison error ({oi_timeframe}): {e}")
            return self._calculate_aggregate_without_cache(current_oi, oi_timeframe)
    
    def _calculate_aggregate_without_cache(self, oi_data: List[OIData], timeframe: str) -> AggregateOIAnalysis:
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
            overall_sentiment=sentiment,
            timeframe=timeframe
        )

class UpstoxDataFetcher:
    """Upstox API - Enhanced for 400+ candles"""
    
    def __init__(self):
        self.connected = self.test_connection()
    
    def test_connection(self) -> bool:
        """Test Upstox API connection"""
        try:
            if not UPSTOX_ACCESS_TOKEN:
                logger.error("❌ Upstox access token not found")
                return False
            
            headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
            url = f"{BASE_URL}/v2/user/profile"
            response = requests.get(url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                logger.info("✅ Upstox API connected successfully!")
                return True
            else:
                logger.error(f"❌ Upstox API error: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"❌ Upstox connection failed: {e}")
            return False
    
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
        """Fetch 400+ candles and create 3 timeframes"""
        headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
        encoded_key = urllib.parse.quote(instrument_key, safe='')
        
        all_candles = []
        
        # Historical data (30-min)
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
        
        # Intraday data (1-min)
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
        
        # Convert to DataFrame
        df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').astype(float)
        df = df.sort_index()
        
        logger.info(f"  📊 Total candles: {len(df)}")
        
        # Resample to 3 timeframes
        try:
            df_5m = df.resample('5min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'oi': 'last'
            }).dropna()
            
            df_15m = df.resample('15min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'oi': 'last'
            }).dropna()
            
            df_1h = df.resample('1H').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum',
                'oi': 'last'
            }).dropna()
            
            logger.info(f"  📊 Resampled: 5m={len(df_5m)}, 15m={len(df_15m)}, 1h={len(df_1h)}")
            
            current_5m = df_5m['close'].iloc[-1] if len(df_5m) > 0 else 0
            current_15m = df_15m['close'].iloc[-1] if len(df_15m) > 0 else 0
            current_1h = df_1h['close'].iloc[-1] if len(df_1h) > 0 else 0
            
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

class NewsAnalyzer:
    """Analyze news sentiment using DeepSeek with relevance filtering"""
    
    @staticmethod
    def is_news_relevant(symbol: str, sector: str, headline: str, summary: str) -> bool:
        """✅ NEW: Check if news is relevant to the symbol/sector"""
        try:
            text = (headline + " " + summary).lower()
            
            # Check symbol name
            if symbol.lower() in text:
                return True
            
            # Check sector keywords
            keywords = SECTOR_KEYWORDS.get(sector, [])
            for keyword in keywords:
                if keyword.lower() in text:
                    return True
            
            return False
        except:
            return False
    
    @staticmethod
    def analyze_news_sentiment(symbol: str, sector: str, news_list: List[Dict]) -> Optional[NewsData]:
        """Use DeepSeek to analyze news sentiment - only relevant news"""
        try:
            if not news_list or not DEEPSEEK_API_KEY:
                return None
            
            # ✅ Filter relevant news only
            relevant_news = []
            for news in news_list[:10]:
                headline = news.get('headline', '')
                summary = news.get('summary', '')
                
                if NewsAnalyzer.is_news_relevant(symbol, sector, headline, summary):
                    relevant_news.append(news)
            
            if not relevant_news:
                logger.info(f"  📰 No relevant news found for {symbol}")
                return None
            
            logger.info(f"  📰 Found {len(relevant_news)} relevant news for {symbol}")
            
            news_summary = ""
            for idx, news in enumerate(relevant_news[:3], 1):
                headline = news.get('headline', '')
                summary = news.get('summary', '')
                news_summary += f"{idx}. {headline}\n   {summary[:200]}...\n\n"
            
            url = "https://api.deepseek.com/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json"
            }
            
            prompt = f"""Analyze these news about {symbol} ({sector}):

{news_summary}

Reply ONLY JSON:
{{
  "sentiment": "BULLISH" or "BEARISH" or "NEUTRAL",
  "impact_score": 75,
  "key_insight": "Brief 1-line summary",
  "trading_bias": "LONG" or "SHORT" or "NEUTRAL"
}}

Impact score: 0-100. Be concise."""

            payload = {
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "Expert news analyst. Reply JSON only."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_tokens": 300
            }
            
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            
            if response.status_code != 200:
                return None
            
            result = response.json()
            content = result['choices'][0]['message']['content'].strip()
            
            try:
                analysis = json.loads(content)
            except:
                match = re.search(r'\{.*?\}', content, re.DOTALL)
                if match:
                    analysis = json.loads(match.group(0))
                else:
                    return None
            
            return NewsData(
                headline=relevant_news[0].get('headline', '')[:100],
                summary=analysis.get('key_insight', 'News analysis'),
                sentiment=analysis.get('sentiment', 'NEUTRAL'),
                impact_score=analysis.get('impact_score', 50),
                source='Finnhub',
                url=relevant_news[0].get('url', ''),
                published_time=datetime.fromtimestamp(relevant_news[0].get('datetime', 0)).strftime('%H:%M'),
                is_relevant=True
            )
            
        except Exception as e:
            logger.error(f"News sentiment error: {e}")
            return None

class ChartGenerator:
    """Generate PNG chart"""
    
    @staticmethod
    def create_chart(mtf_data: MultiTimeframeData, symbol: str, spot_price: float,
                    analysis: DeepAnalysis, aggregate: AggregateOIAnalysis) -> io.BytesIO:
        """Create professional chart"""
        try:
            df_plot = mtf_data.df_15m.tail(100).copy().reset_index(drop=True)
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 10), 
                                          gridspec_kw={'height_ratios': [3, 1]})
            
            # Candlesticks
            for i in range(len(df_plot)):
                row = df_plot.iloc[i]
                color = '#26a69a' if row['close'] >= row['open'] else '#ef5350'
                
                ax1.plot([i, i], [row['low'], row['high']], 
                        color=color, linewidth=1.2)
                
                body_height = abs(row['close'] - row['open'])
                body_bottom = min(row['open'], row['close'])
                
                ax1.add_patch(Rectangle(
                    (i - 0.35, body_bottom),
                    0.7,
                    body_height if body_height > 0 else row['high'] * 0.0001,
                    facecolor=color,
                    edgecolor=color,
                    linewidth=0.8,
                    alpha=0.9
                ))
            
            # Support/Resistance
            for support in analysis.support_levels[:3]:
                ax1.axhline(y=support, color='#26a69a', linestyle='--', linewidth=1.5, alpha=0.7)
                ax1.text(len(df_plot) - 1, support, f'  S: ₹{support:.1f}', 
                        va='center', color='#26a69a', fontweight='bold', fontsize=9)
            
            for resistance in analysis.resistance_levels[:3]:
                ax1.axhline(y=resistance, color='#ef5350', linestyle='--', linewidth=1.5, alpha=0.7)
                ax1.text(len(df_plot) - 1, resistance, f'  R: ₹{resistance:.1f}', 
                        va='center', color='#ef5350', fontweight='bold', fontsize=9)
            
            # Current price
            ax1.axhline(y=spot_price, color='#2962ff', linestyle='-', linewidth=2.5, alpha=0.9)
            ax1.text(len(df_plot) - 1, spot_price, f'  CMP: ₹{spot_price:.1f}', 
                    va='center', color='white', fontweight='bold', fontsize=10,
                    bbox=dict(boxstyle='round,pad=0.4', facecolor='#2962ff', alpha=0.9))
            
            # Targets
            if analysis.opportunity != "WAIT":
                ax1.axhline(y=analysis.entry_price, color='#ff9800', linestyle=':', linewidth=2, alpha=0.8)
                ax1.axhline(y=analysis.stop_loss, color='#f44336', linestyle=':', linewidth=2, alpha=0.8)
                ax1.axhline(y=analysis.target_1, color='#4caf50', linestyle=':', linewidth=2, alpha=0.8)
                ax1.axhline(y=analysis.target_2, color='#1b5e20', linestyle=':', linewidth=2, alpha=0.8)
            
            # Styling
            title = f'{symbol} | 15min | Score:{analysis.total_score}/125 | {analysis.signal_reason}'
            ax1.set_title(title, fontsize=14, fontweight='bold', pad=15)
            ax1.set_ylabel('Price (₹)', fontsize=12, fontweight='bold')
            ax1.grid(True, alpha=0.2, linestyle='--', linewidth=0.5)
            ax1.set_facecolor('#fafafa')
            ax1.set_xticks([])
            
            # Volume
            colors = ['#26a69a' if df_plot.iloc[i]['close'] >= df_plot.iloc[i]['open'] else '#ef5350' 
                     for i in range(len(df_plot))]
            
            ax2.bar(range(len(df_plot)), df_plot['volume'], color=colors, alpha=0.7, width=0.8)
            ax2.set_ylabel('Volume', fontsize=12, fontweight='bold')
            ax2.set_xlabel('Candlestick Index', fontsize=12, fontweight='bold')
            ax2.grid(True, alpha=0.2)
            ax2.set_facecolor('#fafafa')
            
            # Info box
            signal_emoji = "🟢" if analysis.opportunity == "PE_BUY" else "🔴" if analysis.opportunity == "CE_BUY" else "⚪"
            news_emoji = "📰✅" if analysis.news_impact > 60 else "📰❌" if analysis.news_impact > 0 else ""
            
            info_text = f"""
{signal_emoji} {analysis.opportunity} | Conf: {analysis.confidence}%
Reason: {analysis.signal_reason[:30]}
TF: {analysis.tf_alignment}
{news_emoji} News: {analysis.news_sentiment} ({analysis.news_impact})

─────────────────────
1H: {analysis.tf_1h_trend}
15M: {analysis.tf_15m_pattern[:25]}
5M: ₹{analysis.tf_5m_entry:.1f}

─────────────────────
OI ({analysis.oi_timeframe_used}):
PCR: {aggregate.pcr:.2f}
Sentiment: {aggregate.overall_sentiment}
CE: {aggregate.ce_oi_change_pct:+.1f}%
PE: {aggregate.pe_oi_change_pct:+.1f}%

─────────────────────
Entry: ₹{analysis.entry_price:.1f}
SL: ₹{analysis.stop_loss:.1f}
T1: ₹{analysis.target_1:.1f}
T2: ₹{analysis.target_2:.1f}
RR: {analysis.risk_reward}
"""
            
            ax1.text(0.02, 0.98, info_text.strip(), 
                    transform=ax1.transAxes,
                    fontsize=8, 
                    verticalalignment='top',
                    family='monospace',
                    bbox=dict(boxstyle='round,pad=0.6', 
                             facecolor='white', 
                             alpha=0.95,
                             edgecolor='#424242',
                             linewidth=2))
            
            plt.tight_layout()
            
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=120, bbox_inches='tight', 
                       facecolor='white', edgecolor='none')
            buf.seek(0)
            plt.close(fig)
            
            return buf
            
        except Exception as e:
            logger.error(f"Chart error: {e}")
            return None

class OIAnalyzer:
    """Option chain analysis"""
    
    def __init__(self, redis_cache: RedisCache):
        self.redis = redis_cache
    
    def parse_option_chain(self, strikes, spot_price) -> List[OIData]:
        """Convert option chain to OIData"""
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
            
            oi_list.append(OIData(
                strike=sp,
                ce_oi=ce_data.get('oi', 0),
                pe_oi=pe_data.get('oi', 0),
                ce_volume=ce_data.get('volume', 0),
                pe_volume=pe_data.get('volume', 0),
                ce_iv=ce_data.get('iv', 0.0),
                pe_iv=pe_data.get('iv', 0.0),
                pcr_at_strike=pe_data.get('oi', 0) / ce_data.get('oi', 1) if ce_data.get('oi', 0) > 0 else 0
            ))
        
        return oi_list

class ChartAnalyzer:
    """Multi-timeframe analysis"""
    
    @staticmethod
    def analyze_1h_trend(df_1h: pd.DataFrame) -> Dict:
        try:
            if len(df_1h) < 20:
                return {"trend": "NEUTRAL", "strength": 0, "bias": "NONE"}
            
            recent = df_1h.tail(50)
            current = recent['close'].iloc[-1]
            ma20 = recent['close'].rolling(20).mean().iloc[-1]
            
            if current > ma20:
                trend, strength = "BULLISH", 70
            elif current < ma20:
                trend, strength = "BEARISH", 70
            else:
                trend, strength = "NEUTRAL", 40
            
            return {"trend": trend, "strength": strength, "bias": "LONG" if trend == "BULLISH" else "SHORT" if trend == "BEARISH" else "NONE"}
        except:
            return {"trend": "NEUTRAL", "strength": 0, "bias": "NONE"}
    
    @staticmethod
    def analyze_15m_patterns(df_15m: pd.DataFrame) -> Dict:
        try:
            if len(df_15m) < 30:
                return {"pattern": "NONE", "signal": "NEUTRAL", "confidence": 0}
            
            recent = df_15m.tail(100)
            patterns = []
            
            # Breakout detection
            high_20 = recent['high'].rolling(20).max().iloc[-1]
            low_20 = recent['low'].rolling(20).min().iloc[-1]
            current = recent['close'].iloc[-1]
            
            if current > high_20 * 0.999:
                patterns.append("BREAKOUT")
            elif current < low_20 * 1.001:
                patterns.append("BREAKDOWN")
            
            if "BREAKOUT" in patterns:
                return {"pattern": "BREAKOUT", "signal": "BULLISH", "confidence": 75}
            elif "BREAKDOWN" in patterns:
                return {"pattern": "BREAKDOWN", "signal": "BEARISH", "confidence": 75}
            else:
                return {"pattern": "RANGING", "signal": "NEUTRAL", "confidence": 50}
        except:
            return {"pattern": "NONE", "signal": "NEUTRAL", "confidence": 0}
    
    @staticmethod
    def analyze_5m_entry(df_5m: pd.DataFrame) -> Dict:
        try:
            if len(df_5m) < 20:
                return {"entry": 0, "type": "NONE"}
            
            current = df_5m['close'].iloc[-1]
            return {"entry": current, "type": "MARKET"}
        except:
            return {"entry": 0, "type": "NONE"}
    
    @staticmethod
    def calculate_support_resistance(df: pd.DataFrame) -> Dict:
        try:
            if len(df) < 50:
                current = df['close'].iloc[-1]
                return {'supports': [current * 0.98], 'resistances': [current * 1.02]}
            
            recent = df.tail(100)
            current = recent['close'].iloc[-1]
            
            return {'supports': [current * 0.98, current * 0.96], 'resistances': [current * 1.02, current * 1.04]}
        except:
            current = df['close'].iloc[-1] if len(df) > 0 else 0
            return {'supports': [current * 0.98], 'resistances': [current * 1.02]}

class AIAnalyzer:
    """DeepSeek AI analysis with FIXED signal logic"""
    
    @staticmethod
    def extract_json(content: str) -> Optional[Dict]:
        try:
            try:
                return json.loads(content)
            except:
                pass
            
            match = re.search(r'\{.*?\}', content, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except:
                    pass
            return None
        except:
            return None
    
    @staticmethod
    def deep_analysis(symbol: str, spot_price: float, mtf_data: MultiTimeframeData,
                     aggregate: AggregateOIAnalysis, trend_1h: Dict, pattern_15m: Dict,
                     entry_5m: Dict, sr_levels: Dict, news_data: Optional[NewsData],
                     oi_timeframe: str) -> Optional[DeepAnalysis]:
        try:
            # ✅ CRITICAL FIX: Pre-validate signal logic BEFORE AI
            chart_bias = "NEUTRAL"
            if trend_1h['trend'] == "BULLISH" and pattern_15m['signal'] == "BULLISH":
                chart_bias = "BULLISH"
            elif trend_1h['trend'] == "BEARISH" and pattern_15m['signal'] == "BEARISH":
                chart_bias = "BEARISH"
            
            oi_bias = aggregate.overall_sentiment
            
            # ✅ CONFLICT DETECTION
            if chart_bias == "BULLISH" and oi_bias == "BEARISH":
                conflict = True
                logger.warning(f"  ⚠️ CONFLICT: Chart BULLISH but OI BEARISH - Checking news...")
            elif chart_bias == "BEARISH" and oi_bias == "BULLISH":
                conflict = True
                logger.warning(f"  ⚠️ CONFLICT: Chart BEARISH but OI BULLISH - Checking news...")
            else:
                conflict = False
            
            # ✅ NEWS TIE-BREAKER
            final_bias = chart_bias  # Chart takes priority by default
            
            if conflict:
                if news_data and news_data.is_relevant and news_data.impact_score > 70:
                    logger.info(f"  📰 Using NEWS to break tie: {news_data.sentiment}")
                    if news_data.sentiment == oi_bias:
                        final_bias = oi_bias  # News supports OI
                        logger.info(f"  ✅ News confirms OI sentiment")
                    else:
                        logger.info(f"  ⚠️ Conflicting signals persist - WAIT recommended")
                        final_bias = "WAIT"
                else:
                    # No strong news - chart priority but reduce confidence
                    logger.info(f"  ⚠️ No strong news - Chart priority but low confidence")
            
            url = "https://api.deepseek.com/v1/chat/completions"
            headers = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}
            
            news_section = ""
            if news_data and news_data.is_relevant:
                news_section = f"\n📰 RELEVANT NEWS: {news_data.sentiment} | Impact: {news_data.impact_score}/100\n"
            
            # ✅ STRICT RULES IN PROMPT
            prompt = f"""Analyze {symbol} with STRICT SIGNAL RULES.

SPOT: ₹{spot_price:.2f}
1H: {trend_1h['trend']} ({trend_1h['strength']}%)
15M: {pattern_15m['pattern']} | {pattern_15m['signal']}
5M: Entry ₹{entry_5m['entry']:.2f}

OI ANALYSIS ({oi_timeframe}):
━━━━━━━━━━━━━━━━━━━━━━━━━━
Sentiment: {aggregate.overall_sentiment}
CE OI Change: {aggregate.ce_oi_change_pct:+.1f}%
PE OI Change: {aggregate.pe_oi_change_pct:+.1f}%
PCR: {aggregate.pcr:.2f}
━━━━━━━━━━━━━━━━━━━━━━━━━━
{news_section}
⚠️ CRITICAL RULES (MUST FOLLOW):
━━━━━━━━━━━━━━━━━━━━━━━━━━
1. IF 1H BULLISH + 15M BULLISH → ONLY "PE_BUY"
2. IF 1H BEARISH + 15M BEARISH → ONLY "CE_BUY"
3. IF 1H and 15M conflict → "WAIT"
4. IF Chart vs OI conflict + no strong news (>70) → "WAIT"
5. Chart structure > OI sentiment (unless news breaks tie)

PRE-CALCULATED BIAS: {final_bias}
Use this bias for opportunity field.

Reply JSON only:
{{
  "opportunity": "{final_bias}_BUY" if {final_bias} != "WAIT" else "WAIT",
  "signal_reason": "Brief explanation why this signal",
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
  "pattern_signal": "Pattern explanation",
  "oi_flow_signal": "OI analysis",
  "market_structure": "Multi-TF structure",
  "support_levels": {sr_levels['supports'][:2]},
  "resistance_levels": {sr_levels['resistances'][:2]},
  "scenario_bullish": "Bullish scenario",
  "scenario_bearish": "Bearish scenario",
  "risk_factors": ["Risk1", "Risk2"],
  "monitoring_checklist": ["Monitor1", "Monitor2"],
  "tf_1h_trend": "{trend_1h['trend']}",
  "tf_15m_pattern": "{pattern_15m['pattern']}",
  "tf_5m_entry": {entry_5m['entry']:.2f},
  "tf_alignment": "STRONG" or "MODERATE" or "WEAK",
  "news_sentiment": "{news_data.sentiment if news_data else 'NEUTRAL'}",
  "news_impact": {news_data.impact_score if news_data and news_data.is_relevant else 0}
}}"""

            payload = {
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "F&O trader. STRICTLY follow signal rules. Reply JSON only."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_tokens": 1500
            }
            
            response = requests.post(url, json=payload, headers=headers, timeout=40)
            
            if response.status_code != 200:
                return None
            
            content = response.json()['choices'][0]['message']['content'].strip()
            analysis_dict = AIAnalyzer.extract_json(content)
            
            if not analysis_dict:
                return None
            
            # ✅ POST-VALIDATION: Double-check AI didn't violate rules
            ai_opportunity = analysis_dict.get('opportunity', 'WAIT')
            
            if chart_bias == "BULLISH" and ai_opportunity == "CE_BUY":
                logger.error(f"  ❌ AI VIOLATED RULE: Chart BULLISH but AI said CE_BUY - FORCING PE_BUY")
                analysis_dict['opportunity'] = "PE_BUY"
                analysis_dict['signal_reason'] = "CORRECTED: Chart bullish structure"
            elif chart_bias == "BEARISH" and ai_opportunity == "PE_BUY":
                logger.error(f"  ❌ AI VIOLATED RULE: Chart BEARISH but AI said PE_BUY - FORCING CE_BUY")
                analysis_dict['opportunity'] = "CE_BUY"
                analysis_dict['signal_reason'] = "CORRECTED: Chart bearish structure"
            elif conflict and final_bias == "WAIT" and ai_opportunity != "WAIT":
                logger.error(f"  ❌ AI VIOLATED RULE: Conflict detected but AI gave trade - FORCING WAIT")
                analysis_dict['opportunity'] = "WAIT"
                analysis_dict['signal_reason'] = "CORRECTED: Chart-OI conflict"
            
            return DeepAnalysis(
                opportunity=analysis_dict.get('opportunity', 'WAIT'),
                confidence=analysis_dict.get('confidence', 0),
                chart_score=analysis_dict.get('chart_score', 0),
                option_score=analysis_dict.get('option_score', 0),
                alignment_score=analysis_dict.get('alignment_score', 0),
                total_score=analysis_dict.get('total_score', 0),
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
                tf_alignment=analysis_dict.get('tf_alignment', 'WEAK'),
                news_sentiment=analysis_dict.get('news_sentiment', 'NEUTRAL'),
                news_impact=analysis_dict.get('news_impact', 0),
                oi_timeframe_used=oi_timeframe,
                signal_reason=analysis_dict.get('signal_reason', 'Multi-TF analysis')
            )
        except Exception as e:
            logger.error(f"AI analysis error: {e}")
            traceback.print_exc()
            return None

class TelegramNotifier:
    """Telegram alerts"""
    
    def __init__(self, api_status: Dict):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.api_status = api_status
    
    async def send_startup_message(self):
        try:
            upstox = "🟢" if self.api_status['upstox'] else "🔴"
            redis = "🟢" if self.api_status['redis'] else "🔴"
            finnhub = "🟢" if self.api_status['finnhub'] else "🔴"
            deepseek = "🟢" if self.api_status['deepseek'] else "🔴"
            
            msg = f"""🔥 HYBRID BOT v16.1 - FIXED 🔥

════════════════════════
🛠️ CRITICAL FIXES:
════════════════════════
✅ Chart Structure > OI Priority
✅ BULLISH Chart → Only PE_BUY
✅ BEARISH Chart → Only CE_BUY
✅ Conflicts → WAIT or News Tie-breaker
✅ Relevant News Filtering Only

════════════════════════
API STATUS:
════════════════════════
{upstox} Upstox
{redis} Redis
{finnhub} Finnhub News (Filtered)
{deepseek} DeepSeek AI
🟢 Telegram

════════════════════════
BOT INFO:
════════════════════════
📊 Monitoring:
   • BANK NIFTY & MIDCAP NIFTY
   • {len(SELECTED_STOCKS)} F&O Stocks
   
⏰ Scan: Every 15 minutes
🎯 Market: 9:15 AM - 3:30 PM

✅ Multi-Timeframe (1H→15M→5M)
✅ 400+ Candlesticks
✅ SMART OI Tracking
✅ Sector-Relevant News Only
✅ AI with Signal Validation

════════════════════════
🟢 BOT RUNNING
Waiting for market hours...
════════════════════════"""
            
            await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg)
            logger.info("✅ Startup message sent!")
        except Exception as e:
            logger.error(f"Startup error: {e}")
    
    async def send_alert(self, symbol: str, spot: float, analysis: DeepAnalysis,
                        aggregate: AggregateOIAnalysis, expiry: str, mtf: MultiTimeframeData,
                        news: Optional[NewsData]):
        try:
            sig_map = {"PE_BUY": ("🟢", "PE BUY"), "CE_BUY": ("🔴", "CE BUY")}
            emoji, sig = sig_map.get(analysis.opportunity, ("⚪", "WAIT"))
            
            news_sec = ""
            if news and news.is_relevant and analysis.news_impact > 50:
                news_sec = f"\n📰✅ RELEVANT: {news.headline}\nSentiment: {news.sentiment} | Impact: {news.impact_score}/100\n"
            elif news and not news.is_relevant:
                news_sec = f"\n📰❌ Irrelevant news ignored\n"
            
            alert = f"""🎯 {symbol} SIGNAL

{emoji} {sig}

════════════════════════
CONFIDENCE: {analysis.confidence}%
SCORE: {analysis.total_score}/125
TF ALIGNMENT: {analysis.tf_alignment}
════════════════════════
💡 REASON: {analysis.signal_reason}
════════════════════════
{news_sec}
1H: {analysis.tf_1h_trend}
15M: {analysis.tf_15m_pattern}
5M: ₹{analysis.tf_5m_entry:.1f}

════════════════════════
OI ANALYSIS ({analysis.oi_timeframe_used}):
════════════════════════
Sentiment: {aggregate.overall_sentiment}
CE OI: {aggregate.ce_oi_change_pct:+.1f}%
PE OI: {aggregate.pe_oi_change_pct:+.1f}%
PCR: {aggregate.pcr:.2f}

════════════════════════
TRADE SETUP:
════════════════════════
Spot: ₹{spot:.2f}
Entry: ₹{analysis.entry_price:.2f}
SL: ₹{analysis.stop_loss:.2f}
T1: ₹{analysis.target_1:.1f}
T2: ₹{analysis.target_2:.1f}
RR: {analysis.risk_reward}
Strike: {analysis.recommended_strike}

════════════════════════
Expiry: {expiry}
Time: {datetime.now(IST).strftime('%H:%M:%S')} IST
════════════════════════"""
            
            await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=alert)
            
            # Chart
            chart = ChartGenerator.create_chart(mtf, symbol, spot, analysis, aggregate)
            if chart:
                await self.bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=chart,
                                         caption=f"📈 {symbol} | {emoji} {sig} | {analysis.signal_reason[:40]}")
            
            logger.info(f"✅ Alert sent: {symbol}")
        except Exception as e:
            logger.error(f"Alert error: {e}")
    
    async def send_summary(self, scanned: int, alerts: int):
        try:
            msg = f"""📊 SCAN COMPLETE

Analyzed: {scanned}
Alerts: {alerts}

⏰ Next scan in 15 minutes..."""
            await self.bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=msg)
        except:
            pass

class HybridBot:
    """Main bot - BANK NIFTY & MIDCAP NIFTY + Fixed Signal Logic"""
    
    def __init__(self):
        logger.info("Initializing Bot v16.1 - FIXED VERSION...")
        
        self.redis = RedisCache()
        self.fetcher = UpstoxDataFetcher()
        self.finnhub = FinnhubNewsAPI()
        self.oi_analyzer = OIAnalyzer(self.redis)
        self.chart_analyzer = ChartAnalyzer()
        self.news_analyzer = NewsAnalyzer()
        self.ai_analyzer = AIAnalyzer()
        
        api_status = {
            'upstox': self.fetcher.connected,
            'redis': self.redis.connected,
            'finnhub': self.finnhub.connected,
            'deepseek': bool(DEEPSEEK_API_KEY),
            'telegram': bool(TELEGRAM_BOT_TOKEN)
        }
        
        self.notifier = TelegramNotifier(api_status)
        self.total_scanned = 0
        self.alerts_sent = 0
        
        logger.info("✅ Bot initialized with FIXED signal logic!")
    
    def is_market_open(self) -> bool:
        now = datetime.now(IST)
        if now.weekday() >= 5:
            return False
        return "09:15" <= now.strftime("%H:%M") <= "15:30"
    
    async def scan_instruments(self):
        logger.info("\n" + "="*50)
        logger.info(f"SCAN START - {datetime.now(IST).strftime('%H:%M:%S')}")
        logger.info("="*50)
        
        # Market news once per cycle
        market_news = self.finnhub.get_market_news(10)
        
        alerts = 0
        
        # Combine indices and stocks
        all_instruments = {**INDICES, **SELECTED_STOCKS}
        
        for key, info in all_instruments.items():
            try:
                self.total_scanned += 1
                symbol = info['name']
                expiry_day = info.get('expiry_day', 3)
                oi_timeframe = info.get('oi_timeframe', '30min')
                sector = info.get('sector', 'INDEX')
                
                logger.info(f"\n🔍 {symbol} ({sector}) - OI: {oi_timeframe}")
                
                # Get data
                spot = self.fetcher.get_spot_price(key)
                if spot == 0:
                    continue
                
                expiry = self.fetcher.get_next_expiry(key, expiry_day)
                strikes = self.fetcher.get_option_chain(key, expiry)
                if not strikes:
                    continue
                
                oi_data = self.oi_analyzer.parse_option_chain(strikes, spot)
                if not oi_data:
                    continue
                
                # Smart OI comparison with appropriate timeframe
                aggregate = self.redis.get_oi_comparison(symbol, oi_data, spot, oi_timeframe)
                self.redis.store_option_chain(symbol, oi_data, spot, oi_timeframe)
                
                mtf = self.fetcher.get_multi_timeframe_data(key, symbol)
                if not mtf:
                    continue
                
                # Analyze
                trend_1h = self.chart_analyzer.analyze_1h_trend(mtf.df_1h)
                pattern_15m = self.chart_analyzer.analyze_15m_patterns(mtf.df_15m)
                entry_5m = self.chart_analyzer.analyze_5m_entry(mtf.df_5m)
                sr = self.chart_analyzer.calculate_support_resistance(mtf.df_15m)
                
                # ✅ News with relevance filtering
                news_data = None
                if market_news:
                    news_data = self.news_analyzer.analyze_news_sentiment(symbol, sector, market_news)
                    if news_data:
                        logger.info(f"  📰✅ Relevant news found: {news_data.sentiment} ({news_data.impact_score})")
                
                # ✅ NEW: Enhanced alignment check
                chart_aligned = False
                if trend_1h['trend'] == 'BULLISH' and pattern_15m['signal'] == 'BULLISH':
                    chart_aligned = True
                    logger.info(f"  ✅ Chart aligned: BULLISH")
                elif trend_1h['trend'] == 'BEARISH' and pattern_15m['signal'] == 'BEARISH':
                    chart_aligned = True
                    logger.info(f"  ✅ Chart aligned: BEARISH")
                
                # Check conflicts
                oi_sentiment = aggregate.overall_sentiment
                conflict_detected = False
                
                if chart_aligned:
                    if (trend_1h['trend'] == 'BULLISH' and oi_sentiment == 'BEARISH'):
                        conflict_detected = True
                        logger.warning(f"  ⚠️ CONFLICT: Chart BULLISH vs OI BEARISH")
                    elif (trend_1h['trend'] == 'BEARISH' and oi_sentiment == 'BULLISH'):
                        conflict_detected = True
                        logger.warning(f"  ⚠️ CONFLICT: Chart BEARISH vs OI BULLISH")
                
                # Skip if not aligned and no strong news
                if not chart_aligned:
                    if not news_data or news_data.impact_score < 75:
                        logger.info(f"  ❌ Not aligned, skipping")
                        continue
                    else:
                        logger.info(f"  📰 Strong news overrides alignment requirement")
                
                # AI analysis with FIXED logic
                deep = self.ai_analyzer.deep_analysis(symbol, spot, mtf, aggregate, trend_1h, 
                                                      pattern_15m, entry_5m, sr, news_data, oi_timeframe)
                if not deep:
                    continue
                
                logger.info(f"  💡 Signal: {deep.opportunity} | Reason: {deep.signal_reason}")
                
                # Filters
                if deep.opportunity == "WAIT":
                    logger.info(f"  ⚪ WAIT signal")
                    continue
                if deep.confidence < CONFIDENCE_MIN:
                    logger.info(f"  ❌ Low confidence: {deep.confidence}%")
                    continue
                if deep.total_score < SCORE_MIN:
                    logger.info(f"  ❌ Low score: {deep.total_score}")
                    continue
                
                # ✅ FINAL VALIDATION: Ensure signal matches chart structure
                if trend_1h['trend'] == 'BULLISH' and deep.opportunity == 'CE_BUY':
                    logger.error(f"  ❌ FATAL: Signal CE_BUY contradicts BULLISH chart - SKIPPING")
                    continue
                elif trend_1h['trend'] == 'BEARISH' and deep.opportunity == 'PE_BUY':
                    logger.error(f"  ❌ FATAL: Signal PE_BUY contradicts BEARISH chart - SKIPPING")
                    continue
                
                # Send alert
                await self.notifier.send_alert(symbol, spot, deep, aggregate, expiry, mtf, news_data)
                self.alerts_sent += 1
                alerts += 1
                
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"Error scanning {symbol}: {e}")
                traceback.print_exc()
            
            await asyncio.sleep(1)
        
        await self.notifier.send_summary(len(all_instruments), alerts)
        logger.info(f"\n✅ SCAN DONE: {alerts} alerts\n")
    
    async def run(self):
        logger.info("="*50)
        logger.info("BOT v16.1 - FIXED SIGNAL LOGIC")
        logger.info("="*50)
        
        await self.notifier.send_startup_message()
        
        while True:
            try:
                if not self.is_market_open():
                    await asyncio.sleep(60)
                    continue
                
                await self.scan_instruments()
                
                logger.info(f"⏳ Next scan in 15 minutes...")
                await asyncio.sleep(SCAN_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("Bot stopped by user")
                break
            except Exception as e:
                logger.error(f"Loop error: {e}")
                traceback.print_exc()
                await asyncio.sleep(60)

async def main():
    bot = HybridBot()
    await bot.run()

if __name__ == "__main__":
    logger.info("="*50)
    logger.info("STARTING BOT v16.1 - FIXED VERSION...")
    logger.info("="*50)
    asyncio.run(main())
