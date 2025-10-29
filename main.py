#!/usr/bin/env python3
"""
HYBRID TRADING BOT v11.0
- Upstox data fetching (indices + stocks)
- DeepSeek AI analysis with advanced strategies
- Two-phase filtering: Quick scan → Deep analysis
- Chart + Option chain combined analysis
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

# Setup logging
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# CONFIG
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
BASE_URL = "https://api.upstox.com"
IST = pytz.timezone('Asia/Kolkata')

# INDICES - 4 Selected
INDICES = {
    "NSE_INDEX|Nifty 50": {"name": "NIFTY 50", "expiry_day": 1},
    "NSE_INDEX|Nifty Bank": {"name": "BANK NIFTY", "expiry_day": 2},
    "NSE_INDEX|Nifty Midcap Select": {"name": "MIDCAP NIFTY", "expiry_day": 0},
    "NSE_INDEX|Sensex": {"name": "SENSEX", "expiry_day": 4}
}

# STOCKS - Organized by Sector
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
    "NSE_EQ|INE849A01020": "TRENT",
    "NSE_EQ|INE021A01026": "ASIANPAINT",
    
    # Others
    "NSE_EQ|INE397D01024": "BHARTIARTL",
    "NSE_EQ|INE296A01024": "BAJFINANCE"
}

# Analysis thresholds
PHASE1_CONFIDENCE_MIN = 70
PHASE1_OI_DIVERGENCE_MIN = 2.5
PHASE1_VOLUME_MIN = 25.0

PHASE2_CONFIDENCE_MIN = 75
PHASE2_SCORE_MIN = 90
PHASE2_ALIGNMENT_MIN = 18

SCAN_INTERVAL = 900  # 15 minutes
OI_CACHE = {}  # Store previous OI

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
class QuickAnalysis:
    opportunity: str
    confidence: int
    oi_divergence: float
    volume_surge: float
    pcr: float
    passed_phase1: bool
    reason: str

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

class UpstoxDataFetcher:
    """Upstox API for data fetching"""
    
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
    def get_candle_data(instrument_key, symbol):
        """Fetch historical + intraday candles"""
        headers = {"Accept": "application/json", "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}"}
        encoded_key = urllib.parse.quote(instrument_key, safe='')
        
        all_candles = []
        
        # Historical 30-min data
        try:
            to_date = (datetime.now(IST) - timedelta(days=1)).strftime('%Y-%m-%d')
            from_date = (datetime.now(IST) - timedelta(days=10)).strftime('%Y-%m-%d')
            url = f"{BASE_URL}/v2/historical-candle/{encoded_key}/30minute/{to_date}/{from_date}"
            resp = requests.get(url, headers=headers, timeout=20)
            
            if resp.status_code == 200 and resp.json().get('status') == 'success':
                candles_30min = resp.json().get('data', {}).get('candles', [])
                for candle in candles_30min:
                    all_candles.append(candle)
        except Exception as e:
            logger.error(f"Historical candle error: {e}")
        
        # Today's 1-min intraday
        try:
            url = f"{BASE_URL}/v2/historical-candle/intraday/{encoded_key}/1minute"
            resp = requests.get(url, headers=headers, timeout=20)
            
            if resp.status_code == 200 and resp.json().get('status') == 'success':
                candles_1min = resp.json().get('data', {}).get('candles', [])
                all_candles.extend(candles_1min)
        except Exception as e:
            logger.error(f"Intraday candle error: {e}")
        
        if not all_candles:
            return None
        
        # Convert to DataFrame
        df = pd.DataFrame(all_candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp').astype(float)
        
        # Resample to 15-min
        df_15m = df.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'oi': 'last'
        }).dropna()
        
        return df_15m

class OIAnalyzer:
    """Option chain analysis with OI tracking"""
    
    @staticmethod
    def parse_option_chain(strikes, spot_price) -> List[OIData]:
        """Convert raw option chain to OIData"""
        if not strikes:
            return []
        
        # Find ATM
        atm_strike = min(strikes, key=lambda x: abs(x.get('strike_price', 0) - spot_price))
        atm_price = atm_strike.get('strike_price', 0)
        
        oi_list = []
        for s in strikes:
            sp = s.get('strike_price', 0)
            
            # Filter strikes within 5% of ATM
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
    
    @staticmethod
    def get_aggregate_analysis(symbol: str, current_oi: List[OIData]) -> Optional[AggregateOIAnalysis]:
        """Compare OI with cached data"""
        if not current_oi:
            return None
        
        cache_key = symbol
        prev_oi_data = OI_CACHE.get(cache_key, {})
        
        # Calculate totals
        total_ce_oi = sum(oi.ce_oi for oi in current_oi)
        total_pe_oi = sum(oi.pe_oi for oi in current_oi)
        total_ce_volume = sum(oi.ce_volume for oi in current_oi)
        total_pe_volume = sum(oi.pe_volume for oi in current_oi)
        
        # Get old totals
        old_ce_oi = prev_oi_data.get('total_ce_oi', total_ce_oi)
        old_pe_oi = prev_oi_data.get('total_pe_oi', total_pe_oi)
        old_ce_volume = prev_oi_data.get('total_ce_volume', total_ce_volume)
        old_pe_volume = prev_oi_data.get('total_pe_volume', total_pe_volume)
        
        # Calculate changes
        ce_oi_change_pct = ((total_ce_oi - old_ce_oi) / old_ce_oi * 100) if old_ce_oi > 0 else 0
        pe_oi_change_pct = ((total_pe_oi - old_pe_oi) / old_pe_oi * 100) if old_pe_oi > 0 else 0
        ce_volume_change_pct = ((total_ce_volume - old_ce_volume) / old_ce_volume * 100) if old_ce_volume > 0 else 0
        pe_volume_change_pct = ((total_pe_volume - old_pe_volume) / old_pe_volume * 100) if old_pe_volume > 0 else 0
        
        pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
        
        # Sentiment
        sentiment = "NEUTRAL"
        if pe_oi_change_pct > 3 and pe_oi_change_pct > ce_oi_change_pct:
            sentiment = "BULLISH"
        elif ce_oi_change_pct > 3 and ce_oi_change_pct > pe_oi_change_pct:
            sentiment = "BEARISH"
        elif pcr > 1.2:
            sentiment = "BULLISH"
        elif pcr < 0.8:
            sentiment = "BEARISH"
        
        # Update cache
        OI_CACHE[cache_key] = {
            'total_ce_oi': total_ce_oi,
            'total_pe_oi': total_pe_oi,
            'total_ce_volume': total_ce_volume,
            'total_pe_volume': total_pe_volume
        }
        
        return AggregateOIAnalysis(
            total_ce_oi=total_ce_oi,
            total_pe_oi=total_pe_oi,
            total_ce_volume=total_ce_volume,
            total_pe_volume=total_pe_volume,
            ce_oi_change_pct=ce_oi_change_pct,
            pe_oi_change_pct=pe_oi_change_pct,
            ce_volume_change_pct=ce_volume_change_pct,
            pe_volume_change_pct=pe_volume_change_pct,
            pcr=pcr,
            overall_sentiment=sentiment
        )

class ChartAnalyzer:
    """Advanced chart pattern analysis"""
    
    @staticmethod
    def identify_market_structure(df: pd.DataFrame) -> Dict:
        """Identify market structure (HH-HL, LH-LL, Sideways)"""
        try:
            if len(df) < 20:
                return {"structure": "INSUFFICIENT", "bias": "NEUTRAL"}
            
            recent = df.tail(50)
            highs = recent['high'].values
            lows = recent['low'].values
            
            swing_highs = []
            swing_lows = []
            
            for i in range(5, len(recent) - 5):
                if all(highs[i] >= highs[i-j] for j in range(1, 6)) and \
                   all(highs[i] >= highs[i+j] for j in range(1, 6)):
                    swing_highs.append(highs[i])
                
                if all(lows[i] <= lows[i-j] for j in range(1, 6)) and \
                   all(lows[i] <= lows[i+j] for j in range(1, 6)):
                    swing_lows.append(lows[i])
            
            if len(swing_highs) >= 2 and len(swing_lows) >= 2:
                if swing_highs[-1] > swing_highs[-2] and swing_lows[-1] > swing_lows[-2]:
                    return {"structure": "HH_HL", "bias": "BULLISH"}
                elif swing_highs[-1] < swing_highs[-2] and swing_lows[-1] < swing_lows[-2]:
                    return {"structure": "LH_LL", "bias": "BEARISH"}
            
            return {"structure": "SIDEWAYS", "bias": "NEUTRAL"}
        except:
            return {"structure": "ERROR", "bias": "NEUTRAL"}
    
    @staticmethod
    def calculate_support_resistance(df: pd.DataFrame) -> Dict:
        """Calculate multi-touch support/resistance levels"""
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
    """DeepSeek AI analysis - Two phase"""
    
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
    def phase1_quick_scan(symbol: str, spot_price: float, aggregate: AggregateOIAnalysis) -> Optional[QuickAnalysis]:
        """Phase 1: Quick analysis"""
        try:
            url = "https://api.deepseek.com/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json"
            }
            
            prompt = f"""Quick scan for {symbol} options.

Spot: {spot_price:.2f}
PCR: {aggregate.pcr:.2f}
CE OI: {aggregate.ce_oi_change_pct:+.2f}%
PE OI: {aggregate.pe_oi_change_pct:+.2f}%
CE Vol: {aggregate.ce_volume_change_pct:+.2f}%
PE Vol: {aggregate.pe_volume_change_pct:+.2f}%
Sentiment: {aggregate.overall_sentiment}

Reply JSON only:
{{
  "opportunity": "PE_BUY or CE_BUY or WAIT",
  "confidence": 75,
  "reason": "Brief reason"
}}"""

            payload = {
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "Quick trader. Reply JSON only."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.2,
                "max_tokens": 300
            }
            
            response = requests.post(url, json=payload, headers=headers, timeout=10)
            
            if response.status_code != 200:
                return None
            
            result = response.json()
            content = result['choices'][0]['message']['content'].strip()
            
            analysis_dict = AIAnalyzer.extract_json(content)
            
            if not analysis_dict:
                return None
            
            opportunity = analysis_dict.get('opportunity', 'WAIT')
            confidence = analysis_dict.get('confidence', 0)
            
            # Calculate metrics
            oi_divergence = abs(aggregate.pe_oi_change_pct - aggregate.ce_oi_change_pct)
            
            if opportunity == "PE_BUY":
                volume_surge = aggregate.pe_volume_change_pct
            elif opportunity == "CE_BUY":
                volume_surge = aggregate.ce_volume_change_pct
            else:
                volume_surge = 0
            
            # Phase 1 filter
            passed = (
                confidence >= PHASE1_CONFIDENCE_MIN and
                oi_divergence >= PHASE1_OI_DIVERGENCE_MIN and
                volume_surge >= PHASE1_VOLUME_MIN and
                opportunity != "WAIT"
            )
            
            return QuickAnalysis(
                opportunity=opportunity,
                confidence=confidence,
                oi_divergence=oi_divergence,
                volume_surge=volume_surge,
                pcr=aggregate.pcr,
                passed_phase1=passed,
                reason=analysis_dict.get('reason', 'N/A')
            )
            
        except Exception as e:
            logger.error(f"Phase 1 error: {e}")
            return None
    
    @staticmethod
    def phase2_deep_analysis(symbol: str, spot_price: float, df: pd.DataFrame,
                            aggregate: AggregateOIAnalysis, structure: Dict, sr_levels: Dict) -> Optional[DeepAnalysis]:
        """Phase 2: Deep analysis"""
        try:
            url = "https://api.deepseek.com/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
                "Content-Type": "application/json"
            }
            
            prompt = f"""DEEP analysis for {symbol} F&O.

Spot: {spot_price:.2f}

STRUCTURE: {structure['structure']} | {structure['bias']}

SUPPORT: {', '.join([f"{s:.0f}" for s in sr_levels['supports'][:3]])}
RESISTANCE: {', '.join([f"{r:.0f}" for r in sr_levels['resistances'][:3]])}

OPTIONS:
PCR: {aggregate.pcr:.2f}
CE: {aggregate.ce_oi_change_pct:+.2f}% | Vol: {aggregate.ce_volume_change_pct:+.2f}%
PE: {aggregate.pe_oi_change_pct:+.2f}% | Vol: {aggregate.pe_volume_change_pct:+.2f}%

Score /125:
- Chart: /50
- Options: /50
- Alignment: /25

Reply JSON:
{{
  "opportunity": "PE_BUY or CE_BUY",
  "confidence": 78,
  "chart_score": 40,
  "option_score": 42,
  "alignment_score": 20,
  "total_score": 102,
  "entry_price": {spot_price:.2f},
  "stop_loss": {spot_price * 0.995:.2f},
  "target_1": {spot_price * 1.01:.2f},
  "target_2": {spot_price * 1.02:.2f},
  "risk_reward": "1:2",
  "recommended_strike": {int(spot_price)},
  "pattern_signal": "Pattern detail",
  "oi_flow_signal": "OI flow detail",
  "market_structure": "{structure['structure']}",
  "support_levels": {sr_levels['supports'][:2]},
  "resistance_levels": {sr_levels['resistances'][:2]},
  "scenario_bullish": "If breaks X",
  "scenario_bearish": "If breaks Y",
  "risk_factors": ["Risk1", "Risk2"],
  "monitoring_checklist": ["Check1", "Check2"]
}}"""

            payload = {
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": "Expert F&O trader. Reply JSON only with detailed analysis."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3,
                "max_tokens": 1500
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
                market_structure=analysis_dict.get('market_structure', structure['structure']),
                support_levels=analysis_dict.get('support_levels', sr_levels['supports'][:2]),
                resistance_levels=analysis_dict.get('resistance_levels', sr_levels['resistances'][:2]),
                scenario_bullish=analysis_dict.get('scenario_bullish', 'N/A'),
                scenario_bearish=analysis_dict.get('scenario_bearish', 'N/A'),
                risk_factors=analysis_dict.get('risk_factors', ['See analysis']),
                monitoring_checklist=analysis_dict.get('monitoring_checklist', ['Monitor price'])
            )
            
        except Exception as e:
            logger.error(f"Phase 2 error: {e}")
            return None

class TelegramNotifier:
    """Telegram message sender"""
    
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
    
    async def send_startup_message(self):
        """Send bot startup notification"""
        try:
            msg = f"""🔥 HYBRID TRADING BOT v11.0 - ACTIVE 🔥

{'='*40}
DATA SOURCE: Upstox API
AI ENGINE: DeepSeek V3
{'='*40}

📊 Monitoring:
   • {len(INDICES)} Indices
   • {len(SELECTED_STOCKS)} Stocks

⏰ Scan Interval: 15 minutes

{'='*40}
TWO-PHASE ANALYSIS
{'='*40}

PHASE 1: QUICK SCAN
✅ All instruments (5 sec each)
✅ Filters:
   - Confidence ≥70%
   - OI Divergence ≥2.5%
   - Volume ≥25%

PHASE 2: DEEP ANALYSIS
✅ Promising only
✅ Advanced analysis:
   - Market structure
   - Multi-touch S/R
   - Confluence scoring
✅ Filters:
   - Confidence ≥75%
   - Score ≥90/125
   - Alignment ≥18/25

{'='*40}
Status: 🟢 RUNNING
Market: 9:15 AM - 3:30 PM IST
{'='*40}"""
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=msg
            )
            logger.info("Startup message sent!")
        except Exception as e:
            logger.error(f"Startup message error: {e}")
    
    async def send_alert(self, symbol: str, spot_price: float, analysis: DeepAnalysis, 
                        aggregate: AggregateOIAnalysis, expiry: str):
        """Send trading alert"""
        try:
            signal_map = {
                "PE_BUY": ("🟢", "PE BUY (Bullish)"),
                "CE_BUY": ("🔴", "CE BUY (Bearish)")
            }
            
            signal_emoji, signal_text = signal_map.get(analysis.opportunity, ("⚪", "WAIT"))
            
            ist_time = datetime.now(IST).strftime('%H:%M:%S')
            
            # Main alert
            alert = f"""🎯 TRADING SIGNAL - {symbol}

{signal_emoji} {signal_text}

{'='*40}
CONFIDENCE: {analysis.confidence}%
SCORE: {analysis.total_score}/125
   Chart: {analysis.chart_score}/50
   Options: {analysis.option_score}/50
   Alignment: {analysis.alignment_score}/25

{'='*40}
TRADE SETUP
{'='*40}
💰 Spot: ₹{spot_price:.2f}
📍 Entry: ₹{analysis.entry_price:.2f}
🛑 Stop Loss: ₹{analysis.stop_loss:.2f}
🎯 Target 1: ₹{analysis.target_1:.2f}
🎯 Target 2: ₹{analysis.target_2:.2f}
📊 Risk:Reward: {analysis.risk_reward}
🎲 Strike: {analysis.recommended_strike}

{'='*40}
MARKET STRUCTURE
{'='*40}
{analysis.market_structure}

Support: {', '.join([f"₹{s:.1f}" for s in analysis.support_levels[:2]])}
Resistance: {', '.join([f"₹{r:.1f}" for r in analysis.resistance_levels[:2]])}

{'='*40}
OPTIONS DATA
{'='*40}
PCR: {aggregate.pcr:.2f}
CE OI: {aggregate.ce_oi_change_pct:+.1f}% | Vol: {aggregate.ce_volume_change_pct:+.1f}%
PE OI: {aggregate.pe_oi_change_pct:+.1f}% | Vol: {aggregate.pe_volume_change_pct:+.1f}%

{'='*40}
SIGNALS
{'='*40}
📊 Chart: {analysis.pattern_signal[:150]}

⛓️ OI Flow: {analysis.oi_flow_signal[:150]}

{'='*40}
SCENARIOS
{'='*40}
🟢 Bullish: {analysis.scenario_bullish[:150]}

🔴 Bearish: {analysis.scenario_bearish[:150]}

{'='*40}
RISK FACTORS
{'='*40}"""
            
            for i, risk in enumerate(analysis.risk_factors[:3], 1):
                alert += f"\n⚠️ {risk[:100]}"
            
            alert += f"\n\n{'='*40}\nMONITORING CHECKLIST\n{'='*40}"
            
            for i, check in enumerate(analysis.monitoring_checklist[:3], 1):
                alert += f"\n✓ {check[:100]}"
            
            alert += f"\n\n{'='*40}"
            alert += f"\n📅 Expiry: {expiry}"
            alert += f"\n⏰ Time: {ist_time} IST"
            alert += f"\n🤖 AI: DeepSeek V3 | v11.0"
            alert += f"\n{'='*40}"
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=alert
            )
            
            logger.info(f"Alert sent: {symbol} - {analysis.opportunity}")
            
        except Exception as e:
            logger.error(f"Alert error: {e}")
    
    async def send_cycle_summary(self, phase1_count: int, phase1_passed: int, 
                                 phase2_count: int, alerts_sent: int):
        """Send scan cycle summary"""
        try:
            msg = f"""📊 SCAN CYCLE COMPLETE

Phase 1: {phase1_passed}/{phase1_count} passed
Phase 2: {phase2_count} analyzed
Alerts: {alerts_sent} sent

⏰ Next scan in 15 minutes..."""
            
            await self.bot.send_message(
                chat_id=TELEGRAM_CHAT_ID,
                text=msg
            )
        except Exception as e:
            logger.error(f"Summary error: {e}")

class HybridTradingBot:
    """Main bot orchestrator"""
    
    def __init__(self):
        logger.info("Initializing Hybrid Trading Bot v11.0...")
        self.fetcher = UpstoxDataFetcher()
        self.oi_analyzer = OIAnalyzer()
        self.chart_analyzer = ChartAnalyzer()
        self.ai_analyzer = AIAnalyzer()
        self.notifier = TelegramNotifier()
        
        self.phase1_scanned = 0
        self.phase1_passed = 0
        self.phase2_analyzed = 0
        self.alerts_sent = 0
        
        logger.info("Bot v11.0 initialized!")
    
    def is_market_open(self) -> bool:
        """Check if market is open"""
        now_ist = datetime.now(IST)
        current_time = now_ist.strftime("%H:%M")
        
        if now_ist.weekday() >= 5:  # Weekend
            return False
        
        return "09:15" <= current_time <= "15:30"
    
    async def phase1_quick_scan(self, instruments: Dict) -> List[Tuple[str, str, QuickAnalysis, AggregateOIAnalysis, str]]:
        """Phase 1: Quick scan all instruments"""
        promising = []
        
        logger.info("\n" + "="*70)
        logger.info(f"PHASE 1: QUICK SCAN ({len(instruments)} instruments)")
        logger.info("="*70)
        
        for idx, (key, info) in enumerate(instruments.items(), 1):
            try:
                self.phase1_scanned += 1
                
                if isinstance(info, dict) and 'name' in info:
                    symbol = info['name']
                    expiry_day = info.get('expiry_day', 3)
                else:
                    symbol = info
                    expiry_day = 3
                
                logger.info(f"[{idx}/{len(instruments)}] Quick scan: {symbol}")
                
                # Get spot price
                spot_price = self.fetcher.get_spot_price(key)
                if spot_price == 0:
                    logger.warning(f"{symbol}: No spot price")
                    continue
                
                # Get expiry
                expiry = self.fetcher.get_next_expiry(key, expiry_day)
                
                # Get option chain
                strikes = self.fetcher.get_option_chain(key, expiry)
                if not strikes or len(strikes) < 10:
                    logger.warning(f"{symbol}: Insufficient option data")
                    continue
                
                # Parse OI data
                oi_data = self.oi_analyzer.parse_option_chain(strikes, spot_price)
                if not oi_data:
                    continue
                
                # Get aggregate analysis
                aggregate = self.oi_analyzer.get_aggregate_analysis(symbol, oi_data)
                if not aggregate:
                    continue
                
                # Quick AI analysis
                quick = self.ai_analyzer.phase1_quick_scan(symbol, spot_price, aggregate)
                
                if quick and quick.passed_phase1:
                    self.phase1_passed += 1
                    promising.append((key, symbol, quick, aggregate, expiry))
                    logger.info(f"✅ {symbol}: PASSED (Conf: {quick.confidence}%, Div: {quick.oi_divergence:.1f}%)")
                else:
                    logger.info(f"❌ {symbol}: Failed Phase 1")
                
                await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"Phase 1 error {key}: {e}")
        
        logger.info(f"\nPhase 1 Complete: {self.phase1_passed}/{self.phase1_scanned} passed")
        
        return promising
    
    async def phase2_deep_analysis(self, promising: List[Tuple[str, str, QuickAnalysis, AggregateOIAnalysis, str]]):
        """Phase 2: Deep analysis on promising instruments"""
        
        if not promising:
            logger.info("No instruments passed Phase 1")
            return
        
        logger.info("\n" + "="*70)
        logger.info(f"PHASE 2: DEEP ANALYSIS ({len(promising)} promising)")
        logger.info("="*70)
        
        for idx, (key, symbol, quick, aggregate, expiry) in enumerate(promising, 1):
            try:
                self.phase2_analyzed += 1
                
                logger.info(f"\n[{idx}/{len(promising)}] Deep analysis: {symbol}")
                
                # Get candle data
                df = self.fetcher.get_candle_data(key, symbol)
                if df is None or len(df) < 30:
                    logger.warning(f"{symbol}: Insufficient chart data")
                    continue
                
                spot_price = df['close'].iloc[-1]
                
                # Chart analysis
                structure = self.chart_analyzer.identify_market_structure(df)
                sr_levels = self.chart_analyzer.calculate_support_resistance(df)
                
                logger.info(f"{symbol}: {structure['structure']} | {structure['bias']}")
                
                # Deep AI analysis
                deep = self.ai_analyzer.phase2_deep_analysis(
                    symbol, spot_price, df, aggregate, structure, sr_levels
                )
                
                if not deep:
                    logger.warning(f"{symbol}: No deep analysis")
                    continue
                
                logger.info(f"{symbol}: Score={deep.total_score}/125")
                
                # Phase 2 filters
                if deep.confidence < PHASE2_CONFIDENCE_MIN:
                    logger.info(f"❌ {symbol}: Confidence {deep.confidence}% < {PHASE2_CONFIDENCE_MIN}%")
                    continue
                
                if deep.total_score < PHASE2_SCORE_MIN:
                    logger.info(f"❌ {symbol}: Score {deep.total_score} < {PHASE2_SCORE_MIN}")
                    continue
                
                if deep.alignment_score < PHASE2_ALIGNMENT_MIN:
                    logger.info(f"❌ {symbol}: Alignment {deep.alignment_score} < {PHASE2_ALIGNMENT_MIN}")
                    continue
                
                # Time filter (skip opening/closing)
                now_ist = datetime.now(IST)
                hour = now_ist.hour
                minute = now_ist.minute
                
                if hour == 9 and minute < 25:
                    logger.info(f"❌ {symbol}: Opening period")
                    continue
                
                if hour == 15 or (hour == 14 and minute >= 40):
                    logger.info(f"❌ {symbol}: Closing period")
                    continue
                
                logger.info(f"✅ {symbol}: PASSED Phase 2 - Sending alert!")
                
                # Send alert
                await self.notifier.send_alert(symbol, spot_price, deep, aggregate, expiry)
                
                self.alerts_sent += 1
                
                await asyncio.sleep(2)
                
            except Exception as e:
                logger.error(f"Phase 2 error {symbol}: {e}")
                logger.error(traceback.format_exc())
    
    async def run_scan_cycle(self):
        """Run complete scan cycle"""
        logger.info(f"\n{'='*70}")
        logger.info(f"SCAN CYCLE START - {datetime.now(IST).strftime('%H:%M:%S IST')}")
        logger.info(f"{'='*70}")
        
        # Reset counters
        self.phase1_scanned = 0
        self.phase1_passed = 0
        self.phase2_analyzed = 0
        alerts_before = self.alerts_sent
        
        # Combine all instruments
        all_instruments = {**INDICES, **SELECTED_STOCKS}
        
        # Phase 1: Quick scan
        promising = await self.phase1_quick_scan(all_instruments)
        
        # Phase 2: Deep analysis
        await self.phase2_deep_analysis(promising)
        
        # Send summary
        alerts_this_cycle = self.alerts_sent - alerts_before
        await self.notifier.send_cycle_summary(
            self.phase1_scanned, self.phase1_passed,
            self.phase2_analyzed, alerts_this_cycle
        )
        
        logger.info(f"\n{'='*70}")
        logger.info(f"CYCLE COMPLETE")
        logger.info(f"Phase 1: {self.phase1_passed}/{self.phase1_scanned}")
        logger.info(f"Phase 2: {self.phase2_analyzed}")
        logger.info(f"Alerts: {alerts_this_cycle}")
        logger.info(f"{'='*70}\n")
    
    async def run(self):
        """Main bot loop"""
        logger.info("="*70)
        logger.info("HYBRID TRADING BOT v11.0")
        logger.info("="*70)
        
        # Check credentials
        missing = []
        for cred in ['UPSTOX_ACCESS_TOKEN', 'TELEGRAM_BOT_TOKEN', 
                     'TELEGRAM_CHAT_ID', 'DEEPSEEK_API_KEY']:
            if not globals().get(cred):
                missing.append(cred)
        
        if missing:
            logger.error(f"Missing credentials: {', '.join(missing)}")
            return
        
        # Send startup message
        await self.notifier.send_startup_message()
        
        logger.info("="*70)
        logger.info("Bot RUNNING - Hybrid two-phase analysis")
        logger.info("="*70)
        
        while True:
            try:
                if not self.is_market_open():
                    logger.info("Market closed. Waiting...")
                    await asyncio.sleep(60)
                    continue
                
                # Run scan cycle
                await self.run_scan_cycle()
                
                # Wait for next cycle
                logger.info(f"Next scan in {SCAN_INTERVAL // 60} minutes...")
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
    logger.info("HYBRID TRADING BOT v11.0 STARTING...")
    logger.info("Upstox + DeepSeek AI + Two-Phase Filter")
    logger.info("="*70)
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nShutdown (Ctrl+C)")
    except Exception as e:
        logger.error(f"\nCritical error: {e}")
        logger.error(traceback.format_exc())
