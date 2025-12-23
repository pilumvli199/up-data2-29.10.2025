import asyncio
import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime, time as dt_time, timedelta
import json
import logging
import gzip
from typing import Dict, List, Optional, Tuple
from telegram import Bot
from telegram.error import TelegramError
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.gridspec import GridSpec
import mplfinance as mpf
from io import BytesIO
import pytz
from collections import deque
from dataclasses import dataclass, field
from enum import Enum

# ======================== CONFIGURATION ========================
import os

UPSTOX_API_URL = "https://api.upstox.com/v2"
UPSTOX_API_V3_URL = "https://api.upstox.com/v3"
UPSTOX_INSTRUMENTS_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN", "YOUR_ACCESS_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "YOUR_TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "YOUR_CHAT_ID")

# Trading params
ANALYSIS_INTERVAL = 5 * 60  # 5 minutes
CANDLES_COUNT = 200
ATM_RANGE = 3  # ✅ CHANGED: ±3 strikes (was ±2)

# ✅ NEW: Signal Thresholds - Only alert when these thresholds crossed
SIGNAL_THRESHOLDS = {
    "MIN_OI_CHANGE_PCT": 5.0,      # Minimum 5% OI change to consider significant
    "MIN_PCR_CHANGE": 0.15,         # Minimum PCR change to consider significant
    "MIN_PRICE_CHANGE_PCT": 0.1,    # Minimum 0.1% price change
    "MIN_CONFIDENCE_SCORE": 3,      # Minimum score to generate signal (out of 10)
    "STRONG_PCR_BULLISH": 2.5,      # PCR > 2.5 = Strong Support
    "STRONG_PCR_BEARISH": 0.5,      # PCR < 0.5 = Strong Resistance
    "MODERATE_PCR_BULLISH": 1.5,    # PCR > 1.5 = Moderate Support
    "MODERATE_PCR_BEARISH": 0.7,    # PCR < 0.7 = Moderate Resistance
}

# Market hours (IST)
MARKET_START = dt_time(9, 15)
MARKET_END = dt_time(15, 30)
IST = pytz.timezone('Asia/Kolkata')

# ✅ ALL MAJOR INDICES
INDICES = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]

# ✅ EXPIRY DAY MAPPING (All on TUESDAY)
EXPIRY_DAYS = {
    "NIFTY": 1,
    "BANKNIFTY": 1,
    "FINNIFTY": 1,
    "MIDCPNIFTY": 1,
}

# ✅ STRIKE INTERVALS
STRIKE_INTERVALS = {
    "NIFTY": 50,
    "BANKNIFTY": 100,
    "FINNIFTY": 50,
    "MIDCPNIFTY": 25,
}

# ✅ LOT SIZES
LOT_SIZES = {
    "NIFTY": 25,
    "BANKNIFTY": 15,
    "FINNIFTY": 40,
    "MIDCPNIFTY": 75,
}

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ======================== ENUMS FOR SIGNALS ========================
class SignalType(Enum):
    STRONG_BULLISH = "🟢🟢 STRONG BULLISH"
    BULLISH = "🟢 BULLISH"
    WEAK_BULLISH = "🟡 WEAK BULLISH"
    NEUTRAL = "⚪ NEUTRAL"
    WEAK_BEARISH = "🟡 WEAK BEARISH"
    BEARISH = "🔴 BEARISH"
    STRONG_BEARISH = "🔴🔴 STRONG BEARISH"


class ActionType(Enum):
    BUY_AGGRESSIVE = "BUY AGGRESSIVELY"
    BUY = "BUY"
    BUY_DIP = "BUY DIP"
    HOLD = "HOLD"
    EXIT_LONGS = "EXIT LONGS"
    SELL = "SELL"
    SELL_AGGRESSIVE = "SELL AGGRESSIVELY"
    WAIT = "WAIT"


# ======================== IN-MEMORY CACHE ========================
@dataclass
class StrikeData:
    """Data for a single strike"""
    strike: int
    ce_oi: int = 0
    pe_oi: int = 0
    ce_ltp: float = 0.0
    pe_ltp: float = 0.0
    ce_volume: int = 0
    pe_volume: int = 0
    pcr: float = 0.0
    timestamp: datetime = None


@dataclass
class SymbolSnapshot:
    """Complete snapshot of a symbol at a point in time"""
    symbol: str
    timestamp: datetime
    spot_price: float
    atm_strike: int
    total_ce_oi: int = 0
    total_pe_oi: int = 0
    overall_pcr: float = 0.0
    strikes_data: Dict[int, StrikeData] = field(default_factory=dict)
    
    # Multi-timeframe data
    oi_5min: Dict[int, Dict] = field(default_factory=dict)
    oi_15min: Dict[int, Dict] = field(default_factory=dict)
    oi_30min: Dict[int, Dict] = field(default_factory=dict)


class InMemoryCache:
    """
    ✅ IN-MEMORY CACHE for storing previous snapshots
    Stores last N snapshots for each symbol for OI change calculation
    """
    
    def __init__(self, max_snapshots: int = 50):
        self.max_snapshots = max_snapshots
        self._cache: Dict[str, deque] = {}
        self._lock = asyncio.Lock()
    
    async def add_snapshot(self, snapshot: SymbolSnapshot):
        """Add a new snapshot to cache"""
        async with self._lock:
            if snapshot.symbol not in self._cache:
                self._cache[snapshot.symbol] = deque(maxlen=self.max_snapshots)
            self._cache[snapshot.symbol].append(snapshot)
            logger.debug(f"📦 Cached snapshot for {snapshot.symbol} at {snapshot.timestamp}")
    
    async def get_previous_snapshot(self, symbol: str, minutes_ago: int = 5) -> Optional[SymbolSnapshot]:
        """Get snapshot from N minutes ago"""
        async with self._lock:
            if symbol not in self._cache or len(self._cache[symbol]) < 2:
                return None
            
            target_time = datetime.now(IST) - timedelta(minutes=minutes_ago)
            
            # Find closest snapshot to target time
            for snapshot in reversed(self._cache[symbol]):
                if snapshot.timestamp <= target_time:
                    return snapshot
            
            # If no exact match, return second-to-last snapshot
            if len(self._cache[symbol]) >= 2:
                return self._cache[symbol][-2]
            
            return None
    
    async def get_snapshot_at_interval(self, symbol: str, minutes_ago: int) -> Optional[SymbolSnapshot]:
        """Get snapshot closest to specified minutes ago (for multi-timeframe)"""
        async with self._lock:
            if symbol not in self._cache:
                return None
            
            target_time = datetime.now(IST) - timedelta(minutes=minutes_ago)
            closest_snapshot = None
            min_diff = float('inf')
            
            for snapshot in self._cache[symbol]:
                diff = abs((snapshot.timestamp - target_time).total_seconds())
                if diff < min_diff:
                    min_diff = diff
                    closest_snapshot = snapshot
            
            # Only return if within 2 minutes of target
            if closest_snapshot and min_diff <= 120:
                return closest_snapshot
            
            return None
    
    async def get_latest_snapshot(self, symbol: str) -> Optional[SymbolSnapshot]:
        """Get most recent snapshot"""
        async with self._lock:
            if symbol not in self._cache or len(self._cache[symbol]) == 0:
                return None
            return self._cache[symbol][-1]
    
    async def get_all_snapshots(self, symbol: str) -> List[SymbolSnapshot]:
        """Get all cached snapshots for a symbol"""
        async with self._lock:
            if symbol not in self._cache:
                return []
            return list(self._cache[symbol])
    
    def get_cache_size(self, symbol: str) -> int:
        """Get number of cached snapshots for a symbol"""
        if symbol not in self._cache:
            return 0
        return len(self._cache[symbol])


# ======================== OI CHANGE ANALYZER ========================
class OIChangeAnalyzer:
    """
    ✅ OI CHANGE ANALYSIS based on PDF logic
    Tracks OI changes and generates signals based on 9 scenarios
    """
    
    def __init__(self, cache: InMemoryCache):
        self.cache = cache
    
    async def calculate_oi_changes(self, symbol: str, current: SymbolSnapshot) -> Dict:
        """Calculate OI changes from previous snapshot"""
        previous = await self.cache.get_previous_snapshot(symbol, minutes_ago=5)
        
        if not previous:
            logger.info(f"⚠️ No previous snapshot for {symbol}, first run")
            return {
                "has_previous": False,
                "price_change": 0,
                "price_change_pct": 0,
                "total_ce_oi_change": 0,
                "total_pe_oi_change": 0,
                "pcr_change": 0,
                "strike_changes": {}
            }
        
        # Calculate overall changes
        price_change = current.spot_price - previous.spot_price
        price_change_pct = (price_change / previous.spot_price * 100) if previous.spot_price > 0 else 0
        
        total_ce_oi_change = current.total_ce_oi - previous.total_ce_oi
        total_pe_oi_change = current.total_pe_oi - previous.total_pe_oi
        
        ce_oi_change_pct = (total_ce_oi_change / previous.total_ce_oi * 100) if previous.total_ce_oi > 0 else 0
        pe_oi_change_pct = (total_pe_oi_change / previous.total_pe_oi * 100) if previous.total_pe_oi > 0 else 0
        
        pcr_change = current.overall_pcr - previous.overall_pcr
        
        # Calculate per-strike changes
        strike_changes = {}
        for strike, curr_data in current.strikes_data.items():
            prev_data = previous.strikes_data.get(strike)
            
            if prev_data:
                ce_change = curr_data.ce_oi - prev_data.ce_oi
                pe_change = curr_data.pe_oi - prev_data.pe_oi
                pcr_strike_change = curr_data.pcr - prev_data.pcr
                
                ce_change_pct = (ce_change / prev_data.ce_oi * 100) if prev_data.ce_oi > 0 else 0
                pe_change_pct = (pe_change / prev_data.pe_oi * 100) if prev_data.pe_oi > 0 else 0
                
                strike_changes[strike] = {
                    "ce_oi_change": ce_change,
                    "pe_oi_change": pe_change,
                    "ce_oi_change_pct": ce_change_pct,
                    "pe_oi_change_pct": pe_change_pct,
                    "pcr_change": pcr_strike_change,
                    "prev_ce_oi": prev_data.ce_oi,
                    "prev_pe_oi": prev_data.pe_oi,
                    "curr_ce_oi": curr_data.ce_oi,
                    "curr_pe_oi": curr_data.pe_oi,
                }
            else:
                strike_changes[strike] = {
                    "ce_oi_change": 0,
                    "pe_oi_change": 0,
                    "ce_oi_change_pct": 0,
                    "pe_oi_change_pct": 0,
                    "pcr_change": 0,
                    "prev_ce_oi": 0,
                    "prev_pe_oi": 0,
                    "curr_ce_oi": curr_data.ce_oi,
                    "curr_pe_oi": curr_data.pe_oi,
                }
        
        return {
            "has_previous": True,
            "time_diff_seconds": (current.timestamp - previous.timestamp).total_seconds(),
            "price_change": price_change,
            "price_change_pct": price_change_pct,
            "total_ce_oi_change": total_ce_oi_change,
            "total_pe_oi_change": total_pe_oi_change,
            "ce_oi_change_pct": ce_oi_change_pct,
            "pe_oi_change_pct": pe_oi_change_pct,
            "pcr_change": pcr_change,
            "prev_pcr": previous.overall_pcr,
            "curr_pcr": current.overall_pcr,
            "strike_changes": strike_changes
        }
    
    def analyze_scenario(self, oi_changes: Dict) -> Dict:
        """
        ✅ IMPLEMENT 9 SCENARIOS FROM PDF
        Based on Price Movement + Put OI Change + Call OI Change
        """
        if not oi_changes.get("has_previous"):
            return {
                "scenario": 0,
                "signal": SignalType.NEUTRAL,
                "action": ActionType.WAIT,
                "description": "Waiting for data...",
                "confidence": 0
            }
        
        price_change = oi_changes["price_change"]
        price_pct = oi_changes["price_change_pct"]
        pe_oi_change = oi_changes["total_pe_oi_change"]
        ce_oi_change = oi_changes["total_ce_oi_change"]
        pcr_change = oi_changes["pcr_change"]
        
        # Thresholds
        price_up = price_pct > SIGNAL_THRESHOLDS["MIN_PRICE_CHANGE_PCT"]
        price_down = price_pct < -SIGNAL_THRESHOLDS["MIN_PRICE_CHANGE_PCT"]
        price_sideways = not price_up and not price_down
        
        pe_oi_up = oi_changes["pe_oi_change_pct"] > SIGNAL_THRESHOLDS["MIN_OI_CHANGE_PCT"]
        pe_oi_down = oi_changes["pe_oi_change_pct"] < -SIGNAL_THRESHOLDS["MIN_OI_CHANGE_PCT"]
        pe_oi_same = not pe_oi_up and not pe_oi_down
        
        ce_oi_up = oi_changes["ce_oi_change_pct"] > SIGNAL_THRESHOLDS["MIN_OI_CHANGE_PCT"]
        ce_oi_down = oi_changes["ce_oi_change_pct"] < -SIGNAL_THRESHOLDS["MIN_OI_CHANGE_PCT"]
        ce_oi_same = not ce_oi_up and not ce_oi_down
        
        pcr_up = pcr_change > SIGNAL_THRESHOLDS["MIN_PCR_CHANGE"]
        pcr_down = pcr_change < -SIGNAL_THRESHOLDS["MIN_PCR_CHANGE"]
        
        # ========== 9 SCENARIOS FROM PDF ==========
        
        # Scenario 1: Price ⬆️ + Put OI ⬇️ + Call OI Same = STRONG BULLISH (Put Unwinding)
        if price_up and pe_oi_down and ce_oi_same:
            return {
                "scenario": 1,
                "signal": SignalType.STRONG_BULLISH,
                "action": ActionType.BUY_AGGRESSIVE,
                "description": "PUT UNWINDING - Bulls winning, shorts covering",
                "confidence": 9,
                "details": f"Price +{price_pct:.2f}% | Put OI {oi_changes['pe_oi_change_pct']:.1f}%"
            }
        
        # Scenario 2: Price ⬆️ + Put OI Same + Call OI ⬇️ = STRONG BULLISH (Call Unwinding)
        if price_up and pe_oi_same and ce_oi_down:
            return {
                "scenario": 2,
                "signal": SignalType.STRONG_BULLISH,
                "action": ActionType.BUY,
                "description": "CALL UNWINDING - Resistance broken, bears exiting",
                "confidence": 8,
                "details": f"Price +{price_pct:.2f}% | Call OI {oi_changes['ce_oi_change_pct']:.1f}%"
            }
        
        # Scenario 3: Price ⬆️ + Put OI Same + Call OI ⬆️ = BEARISH (Resistance Building)
        if price_up and pe_oi_same and ce_oi_up:
            return {
                "scenario": 3,
                "signal": SignalType.BEARISH,
                "action": ActionType.EXIT_LONGS,
                "description": "CALL WRITING - Resistance building as price rises",
                "confidence": 6,
                "details": f"Price +{price_pct:.2f}% | Call OI +{oi_changes['ce_oi_change_pct']:.1f}%"
            }
        
        # Scenario 4: Price ⬇️ + Put OI ⬆️ + Call OI Same = BULLISH (Support Building)
        if price_down and pe_oi_up and ce_oi_same:
            return {
                "scenario": 4,
                "signal": SignalType.BULLISH,
                "action": ActionType.BUY_DIP,
                "description": "PUT WRITING - Support building at lower levels",
                "confidence": 7,
                "details": f"Price {price_pct:.2f}% | Put OI +{oi_changes['pe_oi_change_pct']:.1f}%"
            }
        
        # Scenario 5: Price ⬇️ + Put OI Same + Call OI ⬇️ = STRONG BEARISH (Call Unwinding on fall)
        if price_down and pe_oi_same and ce_oi_down:
            return {
                "scenario": 5,
                "signal": SignalType.STRONG_BEARISH,
                "action": ActionType.SELL,
                "description": "CALL UNWINDING - Bulls losing, more downside expected",
                "confidence": 8,
                "details": f"Price {price_pct:.2f}% | Call OI {oi_changes['ce_oi_change_pct']:.1f}%"
            }
        
        # Scenario 6: Price ⬇️ + Put OI ⬇️ + Call OI Same = STRONG BEARISH (Put Unwinding - Panic)
        if price_down and pe_oi_down and ce_oi_same:
            return {
                "scenario": 6,
                "signal": SignalType.STRONG_BEARISH,
                "action": ActionType.SELL_AGGRESSIVE,
                "description": "PUT UNWINDING - Panic selling, bears winning",
                "confidence": 9,
                "details": f"Price {price_pct:.2f}% | Put OI {oi_changes['pe_oi_change_pct']:.1f}%"
            }
        
        # Scenario 7: Price ⬆️ + Put OI ⬆️ + Call OI Same = WEAK BULLISH (Protection buying)
        if price_up and pe_oi_up and ce_oi_same:
            return {
                "scenario": 7,
                "signal": SignalType.WEAK_BULLISH,
                "action": ActionType.HOLD,
                "description": "PUT BUYING - Rise with doubt, protection being bought",
                "confidence": 4,
                "details": f"Price +{price_pct:.2f}% | Put OI +{oi_changes['pe_oi_change_pct']:.1f}%"
            }
        
        # Scenario 8: Price Sideways + Put OI ⬆️⬆️ = SUPPORT ZONE
        if price_sideways and pe_oi_up and oi_changes["pe_oi_change_pct"] > 10:
            return {
                "scenario": 8,
                "signal": SignalType.BULLISH,
                "action": ActionType.BUY,
                "description": "MAJOR SUPPORT ZONE - Heavy put writing",
                "confidence": 7,
                "details": f"Put OI +{oi_changes['pe_oi_change_pct']:.1f}% (Support Building)"
            }
        
        # Scenario 9: Price Sideways + Call OI ⬆️⬆️ = RESISTANCE ZONE
        if price_sideways and ce_oi_up and oi_changes["ce_oi_change_pct"] > 10:
            return {
                "scenario": 9,
                "signal": SignalType.BEARISH,
                "action": ActionType.SELL,
                "description": "MAJOR RESISTANCE ZONE - Heavy call writing",
                "confidence": 7,
                "details": f"Call OI +{oi_changes['ce_oi_change_pct']:.1f}% (Resistance Building)"
            }
        
        # ========== ADDITIONAL SCENARIOS ==========
        
        # Both OI increasing with price up - Mixed signal
        if price_up and pe_oi_up and ce_oi_up:
            return {
                "scenario": 10,
                "signal": SignalType.NEUTRAL,
                "action": ActionType.HOLD,
                "description": "MIXED - Both sides adding positions",
                "confidence": 3,
                "details": "Wait for clarity"
            }
        
        # Both OI decreasing - Expiry unwinding
        if pe_oi_down and ce_oi_down:
            direction = SignalType.BULLISH if price_up else (SignalType.BEARISH if price_down else SignalType.NEUTRAL)
            return {
                "scenario": 11,
                "signal": direction,
                "action": ActionType.HOLD,
                "description": "UNWINDING - Both sides closing, follow price",
                "confidence": 5,
                "details": f"Price {price_pct:+.2f}% | Overall unwinding"
            }
        
        # Default - No clear signal
        return {
            "scenario": 0,
            "signal": SignalType.NEUTRAL,
            "action": ActionType.WAIT,
            "description": "No clear signal - Wait for confirmation",
            "confidence": 2,
            "details": f"Price {price_pct:+.2f}%"
        }


# ======================== MULTI-TIMEFRAME ANALYZER ========================
class MultiTimeframeAnalyzer:
    """
    ✅ MULTI-TIMEFRAME OI ANALYSIS
    Compare OI across 5min, 15min, 30min intervals
    """
    
    def __init__(self, cache: InMemoryCache):
        self.cache = cache
    
    async def analyze_timeframes(self, symbol: str, current: SymbolSnapshot) -> Dict:
        """Analyze OI changes across multiple timeframes"""
        
        # Get snapshots at different intervals
        snapshot_5min = await self.cache.get_snapshot_at_interval(symbol, 5)
        snapshot_15min = await self.cache.get_snapshot_at_interval(symbol, 15)
        snapshot_30min = await self.cache.get_snapshot_at_interval(symbol, 30)
        
        result = {
            "5min": self._calculate_tf_change(current, snapshot_5min, "5min"),
            "15min": self._calculate_tf_change(current, snapshot_15min, "15min"),
            "30min": self._calculate_tf_change(current, snapshot_30min, "30min"),
            "trend_alignment": "NEUTRAL",
            "trend_strength": 0
        }
        
        # Analyze trend alignment across timeframes
        bullish_count = 0
        bearish_count = 0
        
        for tf in ["5min", "15min", "30min"]:
            if result[tf]["available"]:
                if result[tf]["pcr_trend"] == "BULLISH":
                    bullish_count += 1
                elif result[tf]["pcr_trend"] == "BEARISH":
                    bearish_count += 1
        
        if bullish_count >= 2:
            result["trend_alignment"] = "BULLISH"
            result["trend_strength"] = bullish_count
        elif bearish_count >= 2:
            result["trend_alignment"] = "BEARISH"
            result["trend_strength"] = bearish_count
        else:
            result["trend_alignment"] = "MIXED"
            result["trend_strength"] = 0
        
        return result
    
    def _calculate_tf_change(self, current: SymbolSnapshot, previous: Optional[SymbolSnapshot], tf_name: str) -> Dict:
        """Calculate changes for a specific timeframe"""
        if not previous:
            return {
                "available": False,
                "timeframe": tf_name,
                "price_change": 0,
                "ce_oi_change_pct": 0,
                "pe_oi_change_pct": 0,
                "pcr_change": 0,
                "pcr_trend": "UNKNOWN"
            }
        
        price_change = current.spot_price - previous.spot_price
        price_change_pct = (price_change / previous.spot_price * 100) if previous.spot_price > 0 else 0
        
        ce_change_pct = ((current.total_ce_oi - previous.total_ce_oi) / previous.total_ce_oi * 100) if previous.total_ce_oi > 0 else 0
        pe_change_pct = ((current.total_pe_oi - previous.total_pe_oi) / previous.total_pe_oi * 100) if previous.total_pe_oi > 0 else 0
        
        pcr_change = current.overall_pcr - previous.overall_pcr
        
        # Determine trend
        if pcr_change > 0.1 and pe_change_pct > ce_change_pct:
            pcr_trend = "BULLISH"
        elif pcr_change < -0.1 and ce_change_pct > pe_change_pct:
            pcr_trend = "BEARISH"
        else:
            pcr_trend = "NEUTRAL"
        
        return {
            "available": True,
            "timeframe": tf_name,
            "price_change": price_change,
            "price_change_pct": price_change_pct,
            "ce_oi_change_pct": ce_change_pct,
            "pe_oi_change_pct": pe_change_pct,
            "pcr_change": pcr_change,
            "prev_pcr": previous.overall_pcr,
            "curr_pcr": current.overall_pcr,
            "pcr_trend": pcr_trend
        }


# ======================== PCR MOMENTUM ANALYZER ========================
class PCRMomentumAnalyzer:
    """
    ✅ PCR CHANGE MOMENTUM from PDF Part 7
    Track PCR trend and momentum
    """
    
    def __init__(self, cache: InMemoryCache):
        self.cache = cache
    
    async def analyze_pcr_momentum(self, symbol: str, current_pcr: float) -> Dict:
        """Analyze PCR momentum over time"""
        snapshots = await self.cache.get_all_snapshots(symbol)
        
        if len(snapshots) < 3:
            return {
                "momentum": "UNKNOWN",
                "direction": "NEUTRAL",
                "strength": 0,
                "pcr_history": []
            }
        
        # Get last N PCR values
        pcr_history = [(s.timestamp, s.overall_pcr) for s in snapshots[-10:]]
        
        # Calculate momentum (rate of change)
        if len(pcr_history) >= 2:
            recent_pcr = pcr_history[-1][1]
            older_pcr = pcr_history[-3][1] if len(pcr_history) >= 3 else pcr_history[0][1]
            
            pcr_change = recent_pcr - older_pcr
            
            # Determine momentum
            if pcr_change > 0.3:
                momentum = "STRONG_RISING"
                direction = "BEARISH"  # Rising PCR = More puts = Bearish momentum
                strength = min(10, int(abs(pcr_change) * 10))
            elif pcr_change > 0.1:
                momentum = "RISING"
                direction = "BEARISH"
                strength = min(7, int(abs(pcr_change) * 10))
            elif pcr_change < -0.3:
                momentum = "STRONG_FALLING"
                direction = "BULLISH"  # Falling PCR = More calls = Bullish momentum
                strength = min(10, int(abs(pcr_change) * 10))
            elif pcr_change < -0.1:
                momentum = "FALLING"
                direction = "BULLISH"
                strength = min(7, int(abs(pcr_change) * 10))
            else:
                momentum = "STABLE"
                direction = "NEUTRAL"
                strength = 3
            
            # Check for extremes
            if current_pcr > 3.0:
                momentum = "OVERSOLD_EXTREME"
                direction = "BULLISH"  # Extreme high PCR = Reversal expected
                strength = 8
            elif current_pcr < 0.3:
                momentum = "OVERBOUGHT_EXTREME"
                direction = "BEARISH"  # Extreme low PCR = Correction expected
                strength = 8
            
            return {
                "momentum": momentum,
                "direction": direction,
                "strength": strength,
                "pcr_change": pcr_change,
                "current_pcr": current_pcr,
                "pcr_history": pcr_history[-5:]  # Last 5 readings
            }
        
        return {
            "momentum": "UNKNOWN",
            "direction": "NEUTRAL",
            "strength": 0,
            "pcr_history": pcr_history
        }


# ======================== SIGNAL GENERATOR ========================
class SignalGenerator:
    """
    ✅ COMPREHENSIVE SIGNAL GENERATOR
    Combines OI changes, Multi-TF, PCR momentum for final signal
    """
    
    def __init__(self):
        pass
    
    def generate_signal(
        self,
        oi_scenario: Dict,
        mtf_analysis: Dict,
        pcr_momentum: Dict,
        current_pcr: float,
        current_price: float,
        atm_strike: int,
        symbol: str
    ) -> Dict:
        """Generate final trading signal with confidence score"""
        
        confidence_score = 0
        reasons = []
        
        # 1. OI Scenario contribution (0-3 points)
        scenario_confidence = oi_scenario.get("confidence", 0)
        if scenario_confidence >= 7:
            confidence_score += 3
            reasons.append(f"Strong OI signal: {oi_scenario['description']}")
        elif scenario_confidence >= 5:
            confidence_score += 2
            reasons.append(f"Moderate OI signal: {oi_scenario['description']}")
        elif scenario_confidence >= 3:
            confidence_score += 1
            reasons.append(f"Weak OI signal: {oi_scenario['description']}")
        
        # 2. Multi-timeframe alignment (0-3 points)
        if mtf_analysis["trend_alignment"] != "MIXED":
            tf_strength = mtf_analysis["trend_strength"]
            if tf_strength >= 3:
                confidence_score += 3
                reasons.append(f"All timeframes {mtf_analysis['trend_alignment']}")
            elif tf_strength >= 2:
                confidence_score += 2
                reasons.append(f"2/3 timeframes {mtf_analysis['trend_alignment']}")
        
        # 3. PCR Zone contribution (0-2 points)
        if current_pcr > SIGNAL_THRESHOLDS["STRONG_PCR_BULLISH"]:
            confidence_score += 2
            reasons.append(f"Strong Support Zone (PCR {current_pcr:.2f})")
        elif current_pcr > SIGNAL_THRESHOLDS["MODERATE_PCR_BULLISH"]:
            confidence_score += 1
            reasons.append(f"Support Zone (PCR {current_pcr:.2f})")
        elif current_pcr < SIGNAL_THRESHOLDS["STRONG_PCR_BEARISH"]:
            confidence_score += 2
            reasons.append(f"Strong Resistance Zone (PCR {current_pcr:.2f})")
        elif current_pcr < SIGNAL_THRESHOLDS["MODERATE_PCR_BEARISH"]:
            confidence_score += 1
            reasons.append(f"Resistance Zone (PCR {current_pcr:.2f})")
        
        # 4. PCR Momentum contribution (0-2 points)
        if pcr_momentum["strength"] >= 7:
            confidence_score += 2
            reasons.append(f"Strong PCR momentum: {pcr_momentum['momentum']}")
        elif pcr_momentum["strength"] >= 4:
            confidence_score += 1
            reasons.append(f"PCR momentum: {pcr_momentum['momentum']}")
        
        # Determine final signal type
        base_signal = oi_scenario.get("signal", SignalType.NEUTRAL)
        base_action = oi_scenario.get("action", ActionType.WAIT)
        
        # Check for confluence
        oi_direction = "BULLISH" if "BULLISH" in base_signal.value else ("BEARISH" if "BEARISH" in base_signal.value else "NEUTRAL")
        mtf_direction = mtf_analysis["trend_alignment"]
        pcr_direction = pcr_momentum["direction"]
        
        # Strong confluence bonus
        directions = [oi_direction, mtf_direction, pcr_direction]
        bullish_count = directions.count("BULLISH")
        bearish_count = directions.count("BEARISH")
        
        if bullish_count >= 2:
            final_bias = "BULLISH"
            if confidence_score >= 7:
                final_signal = SignalType.STRONG_BULLISH
                final_action = ActionType.BUY_AGGRESSIVE
            elif confidence_score >= 5:
                final_signal = SignalType.BULLISH
                final_action = ActionType.BUY
            else:
                final_signal = SignalType.WEAK_BULLISH
                final_action = ActionType.HOLD
        elif bearish_count >= 2:
            final_bias = "BEARISH"
            if confidence_score >= 7:
                final_signal = SignalType.STRONG_BEARISH
                final_action = ActionType.SELL_AGGRESSIVE
            elif confidence_score >= 5:
                final_signal = SignalType.BEARISH
                final_action = ActionType.SELL
            else:
                final_signal = SignalType.WEAK_BEARISH
                final_action = ActionType.EXIT_LONGS
        else:
            final_bias = "NEUTRAL"
            final_signal = base_signal
            final_action = base_action
        
        # Calculate entry/exit levels
        interval = STRIKE_INTERVALS.get(symbol, 50)
        
        if final_bias == "BULLISH":
            entry_strike = atm_strike
            sl_strike = atm_strike - interval
            target_strike = atm_strike + (2 * interval)
            option_type = "CE"
        elif final_bias == "BEARISH":
            entry_strike = atm_strike
            sl_strike = atm_strike + interval
            target_strike = atm_strike - (2 * interval)
            option_type = "PE"
        else:
            entry_strike = atm_strike
            sl_strike = atm_strike
            target_strike = atm_strike
            option_type = "WAIT"
        
        # Check if signal meets threshold
        should_alert = confidence_score >= SIGNAL_THRESHOLDS["MIN_CONFIDENCE_SCORE"]
        
        return {
            "symbol": symbol,
            "timestamp": datetime.now(IST),
            "spot_price": current_price,
            "atm_strike": atm_strike,
            
            # Signal details
            "signal": final_signal,
            "action": final_action,
            "bias": final_bias,
            "confidence_score": confidence_score,
            "max_score": 10,
            
            # Trade setup
            "option_type": option_type,
            "entry_strike": entry_strike,
            "stop_loss_strike": sl_strike,
            "target_strike": target_strike,
            
            # Analysis breakdown
            "oi_scenario": oi_scenario,
            "mtf_analysis": mtf_analysis,
            "pcr_momentum": pcr_momentum,
            "current_pcr": current_pcr,
            
            # Reasons
            "reasons": reasons,
            
            # Alert decision
            "should_alert": should_alert,
            "alert_reason": "Confidence score meets threshold" if should_alert else f"Low confidence ({confidence_score}/10)"
        }


# ======================== UPSTOX CLIENT ========================
class UpstoxClient:
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.session = None
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        self.instruments_cache = None
        self.futures_keys = {}
        
    async def create_session(self):
        if not self.session:
            self.session = aiohttp.ClientSession(headers=self.headers)
    
    async def close_session(self):
        if self.session:
            await self.session.close()
    
    def get_instrument_key(self, symbol: str) -> str:
        mapping = {
            "NIFTY": "NSE_INDEX|Nifty 50",
            "BANKNIFTY": "NSE_INDEX|Nifty Bank",
            "MIDCPNIFTY": "NSE_INDEX|NIFTY MID SELECT",
            "FINNIFTY": "NSE_INDEX|Nifty Fin Service"
        }
        return mapping.get(symbol, f"NSE_EQ|{symbol}")
    
    async def download_instruments(self):
        try:
            logger.info("📡 Downloading instruments...")
            url = UPSTOX_INSTRUMENTS_URL
            
            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.error(f"❌ Download failed: {response.status}")
                    return None
                
                content = await response.read()
                json_text = gzip.decompress(content).decode('utf-8')
                instruments = json.loads(json_text)
                
                logger.info(f"✅ Downloaded {len(instruments)} instruments")
                self.instruments_cache = instruments
                
                now = datetime.now(IST)
                
                for symbol in INDICES:
                    for instrument in instruments:
                        if instrument.get('segment') != 'NSE_FO':
                            continue
                        if instrument.get('instrument_type') != 'FUT':
                            continue
                        if instrument.get('name') != symbol:
                            continue
                        
                        expiry = instrument.get('expiry')
                        if expiry:
                            expiry_dt = datetime.fromtimestamp(expiry / 1000, tz=IST)
                            if expiry_dt > now:
                                self.futures_keys[symbol] = instrument.get('instrument_key')
                                logger.info(f"✅ {symbol} Futures: {self.futures_keys[symbol]}")
                                break
                
                return instruments
                
        except Exception as e:
            logger.error(f"❌ Error: {e}")
            return None
    
    async def get_available_expiries(self, symbol: str) -> List[str]:
        try:
            if not self.instruments_cache:
                instruments = await self.download_instruments()
                if not instruments:
                    return []
            else:
                instruments = self.instruments_cache
            
            now = datetime.now(IST)
            expiries_set = set()
            
            for instrument in instruments:
                if instrument.get('segment') != 'NSE_FO':
                    continue
                if instrument.get('instrument_type') not in ['CE', 'PE']:
                    continue
                if instrument.get('name', '') != symbol:
                    continue
                
                expiry_ms = instrument.get('expiry')
                if not expiry_ms:
                    continue
                
                try:
                    expiry_dt = datetime.fromtimestamp(expiry_ms / 1000, tz=IST)
                    if expiry_dt > now:
                        expiry_str = expiry_dt.strftime('%Y-%m-%d')
                        expiries_set.add(expiry_str)
                except:
                    continue
            
            if expiries_set:
                expiries = sorted(list(expiries_set))
                return expiries
            return []
                
        except Exception as e:
            logger.error(f"❌ Error getting expiries for {symbol}: {e}")
            return []
    
    def get_nearest_expiry_for_symbol(self, symbol: str, expiries: List[str]) -> Optional[str]:
        if not expiries:
            return None
        
        now = datetime.now(IST)
        preferred_weekday = EXPIRY_DAYS.get(symbol, 1)
        
        for expiry_str in expiries:
            try:
                expiry_dt = datetime.strptime(expiry_str, '%Y-%m-%d')
                expiry_dt = IST.localize(expiry_dt)
                
                if expiry_dt > now and expiry_dt.weekday() == preferred_weekday:
                    return expiry_str
            except:
                continue
        
        return expiries[0] if expiries else None
    
    async def get_full_quote(self, instrument_key: str) -> Dict:
        try:
            url = f"{UPSTOX_API_URL}/market-quote/quotes"
            params = {"instrument_key": instrument_key}
            
            async with self.session.get(url, params=params) as response:
                data = await response.json()
                
                if data.get("status") == "success" and data.get("data"):
                    for key, value in data["data"].items():
                        return {
                            "ltp": value.get("last_price", 0.0),
                            "volume": value.get("volume", 0),
                            "oi": value.get("oi", 0)
                        }
                
                return {"ltp": 0.0, "volume": 0, "oi": 0}
        except Exception as e:
            return {"ltp": 0.0, "volume": 0, "oi": 0}
    
    async def get_ltp(self, instrument_key: str) -> float:
        quote = await self.get_full_quote(instrument_key)
        return quote["ltp"]
    
    async def get_historical_candles_combined(self, instrument_key: str) -> pd.DataFrame:
        try:
            all_candles = []
            
            url_intraday = f"{UPSTOX_API_V3_URL}/historical-candle/intraday/{instrument_key}/minutes/5"
            
            async with self.session.get(url_intraday) as response:
                data = await response.json()
                
                if data.get("status") == "success" and data.get("data", {}).get("candles"):
                    intraday_candles = data["data"]["candles"]
                    
                    for candle in intraday_candles:
                        all_candles.append({
                            'timestamp': pd.to_datetime(candle[0]),
                            'open': candle[1],
                            'high': candle[2],
                            'low': candle[3],
                            'close': candle[4],
                            'volume': candle[5] if len(candle) > 5 else 0,
                            'oi': candle[6] if len(candle) > 6 else 0
                        })
                    
                    logger.info(f"✅ Fetched {len(intraday_candles)} intraday candles")
            
            if not all_candles:
                return pd.DataFrame()
            
            df = pd.DataFrame(all_candles)
            df.set_index('timestamp', inplace=True)
            df = df.sort_index()
            df = df[~df.index.duplicated(keep='last')]
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error fetching candles: {e}")
            return pd.DataFrame()
    
    async def get_option_contracts(self, symbol: str, expiry: str) -> List[Dict]:
        try:
            instrument_key = self.get_instrument_key(symbol)
            url = f"{UPSTOX_API_URL}/option/contract"
            
            params = {
                "instrument_key": instrument_key,
                "expiry_date": expiry
            }
            
            async with self.session.get(url, params=params) as response:
                response_text = await response.text()
                data = json.loads(response_text)
                
                if data.get("status") == "success":
                    contracts = data.get("data", [])
                    if contracts:
                        logger.info(f"✅ Fetched {len(contracts)} option contracts for {symbol}")
                        return contracts
                    return []
                return []
                    
        except Exception as e:
            logger.error(f"❌ Error fetching contracts for {symbol}: {e}")
            return []


# ======================== OPTION ANALYZER (Enhanced) ========================
class OptionAnalyzer:
    def __init__(self, client: UpstoxClient, cache: InMemoryCache):
        self.client = client
        self.cache = cache
        self.oi_analyzer = OIChangeAnalyzer(cache)
        self.mtf_analyzer = MultiTimeframeAnalyzer(cache)
        self.pcr_momentum = PCRMomentumAnalyzer(cache)
        self.signal_generator = SignalGenerator()
    
    def get_strike_interval(self, symbol: str) -> int:
        return STRIKE_INTERVALS.get(symbol, 50)
    
    async def filter_atm_strikes(self, contracts: List[Dict], current_price: float, symbol: str) -> Dict:
        interval = self.get_strike_interval(symbol)
        atm = round(current_price / interval) * interval
        
        # ✅ CHANGED: ATM_RANGE = 3 (±3 strikes)
        min_strike = atm - (ATM_RANGE * interval)
        max_strike = atm + (ATM_RANGE * interval)
        
        logger.info(f"🎯 ATM: {atm}, Range: {min_strike} to {max_strike} (±{ATM_RANGE} strikes)")
        
        ce_contracts = {}
        pe_contracts = {}
        
        for contract in contracts:
            strike = contract.get("strike_price")
            if min_strike <= strike <= max_strike:
                instrument_key = contract.get("instrument_key")
                option_type = contract.get("instrument_type")
                
                contract_data = {
                    "strike": strike,
                    "instrument_key": instrument_key,
                    "trading_symbol": contract.get("trading_symbol"),
                    "ltp": 0, "oi": 0, "volume": 0
                }
                
                if option_type == "CE":
                    ce_contracts[strike] = contract_data
                elif option_type == "PE":
                    pe_contracts[strike] = contract_data
        
        return {
            "ce": ce_contracts,
            "pe": pe_contracts,
            "strikes": sorted(set(list(ce_contracts.keys()) + list(pe_contracts.keys())))
        }
    
    async def fetch_option_prices(self, contracts_data: Dict):
        for strike, contract in contracts_data["ce"].items():
            quote = await self.client.get_full_quote(contract["instrument_key"])
            contract["ltp"] = quote["ltp"]
            contract["oi"] = quote["oi"]
            contract["volume"] = quote["volume"]
            await asyncio.sleep(0.05)
        
        for strike, contract in contracts_data["pe"].items():
            quote = await self.client.get_full_quote(contract["instrument_key"])
            contract["ltp"] = quote["ltp"]
            contract["oi"] = quote["oi"]
            contract["volume"] = quote["volume"]
            await asyncio.sleep(0.05)
    
    async def analyze_symbol(self, symbol: str) -> Optional[Dict]:
        try:
            logger.info(f"\n📊 Analyzing {symbol}...")
            
            instrument_key = self.client.get_instrument_key(symbol)
            current_price = await self.client.get_ltp(instrument_key)
            
            if current_price == 0:
                logger.warning(f"⚠️ Could not fetch price for {symbol}")
                return None
            
            logger.info(f"💰 {symbol} Spot: ₹{current_price:,.2f}")
            
            # Get candles
            candles = await self.client.get_historical_candles_combined(instrument_key)
            
            if candles.empty:
                logger.warning(f"⚠️ No candle data for {symbol}")
                return None
            
            # Get expiry
            expiries = await self.client.get_available_expiries(symbol)
            if not expiries:
                logger.warning(f"⚠️ No expiries found for {symbol}")
                return None
            
            expiry = self.client.get_nearest_expiry_for_symbol(symbol, expiries)
            
            if not expiry:
                return None
            
            contracts = await self.client.get_option_contracts(symbol, expiry)
            
            if not contracts:
                return None
            
            contracts_data = await self.filter_atm_strikes(contracts, current_price, symbol)
            
            if not contracts_data["strikes"]:
                return None
            
            await self.fetch_option_prices(contracts_data)
            
            # Calculate totals and PCR
            total_ce_oi = 0
            total_pe_oi = 0
            
            strikes_data = {}
            
            for strike in contracts_data["strikes"]:
                ce = contracts_data["ce"].get(strike, {"oi": 0, "ltp": 0, "volume": 0})
                pe = contracts_data["pe"].get(strike, {"oi": 0, "ltp": 0, "volume": 0})
                
                total_ce_oi += ce.get("oi", 0)
                total_pe_oi += pe.get("oi", 0)
                
                strike_pcr = pe.get("oi", 0) / ce.get("oi", 1) if ce.get("oi", 0) > 0 else 0
                
                strikes_data[strike] = StrikeData(
                    strike=strike,
                    ce_oi=ce.get("oi", 0),
                    pe_oi=pe.get("oi", 0),
                    ce_ltp=ce.get("ltp", 0),
                    pe_ltp=pe.get("ltp", 0),
                    ce_volume=ce.get("volume", 0),
                    pe_volume=pe.get("volume", 0),
                    pcr=strike_pcr,
                    timestamp=datetime.now(IST)
                )
            
            overall_pcr = total_pe_oi / total_ce_oi if total_ce_oi > 0 else 0
            atm_strike = round(current_price / self.get_strike_interval(symbol)) * self.get_strike_interval(symbol)
            
            # ✅ CREATE SNAPSHOT AND CACHE IT
            current_snapshot = SymbolSnapshot(
                symbol=symbol,
                timestamp=datetime.now(IST),
                spot_price=current_price,
                atm_strike=atm_strike,
                total_ce_oi=total_ce_oi,
                total_pe_oi=total_pe_oi,
                overall_pcr=overall_pcr,
                strikes_data=strikes_data
            )
            
            # ✅ OI CHANGE ANALYSIS
            oi_changes = await self.oi_analyzer.calculate_oi_changes(symbol, current_snapshot)
            oi_scenario = self.oi_analyzer.analyze_scenario(oi_changes)
            
            # ✅ MULTI-TIMEFRAME ANALYSIS
            mtf_analysis = await self.mtf_analyzer.analyze_timeframes(symbol, current_snapshot)
            
            # ✅ PCR MOMENTUM ANALYSIS
            pcr_momentum_data = await self.pcr_momentum.analyze_pcr_momentum(symbol, overall_pcr)
            
            # ✅ GENERATE FINAL SIGNAL
            final_signal = self.signal_generator.generate_signal(
                oi_scenario=oi_scenario,
                mtf_analysis=mtf_analysis,
                pcr_momentum=pcr_momentum_data,
                current_pcr=overall_pcr,
                current_price=current_price,
                atm_strike=atm_strike,
                symbol=symbol
            )
            
            # ✅ SAVE SNAPSHOT TO CACHE
            await self.cache.add_snapshot(current_snapshot)
            
            logger.info(f"✅ {symbol}: Signal={final_signal['signal'].value}, Confidence={final_signal['confidence_score']}/10")
            
            # Prepare response
            ce_data = []
            pe_data = []
            
            for s in contracts_data["strikes"]:
                ce = contracts_data["ce"].get(s, {"strike": s, "ltp": 0, "oi": 0, "volume": 0})
                pe = contracts_data["pe"].get(s, {"strike": s, "ltp": 0, "oi": 0, "volume": 0})
                
                strike_data = strikes_data.get(s)
                ce["pcr"] = strike_data.pcr if strike_data else 0
                pe["pcr"] = strike_data.pcr if strike_data else 0
                
                ce_data.append(ce)
                pe_data.append(pe)
            
            return {
                "symbol": symbol,
                "current_price": current_price,
                "expiry": expiry,
                "candles": candles,
                "strikes": contracts_data["strikes"],
                "ce_data": ce_data,
                "pe_data": pe_data,
                "atm_strike": atm_strike,
                "total_ce_oi": total_ce_oi,
                "total_pe_oi": total_pe_oi,
                "overall_pcr": overall_pcr,
                "lot_size": LOT_SIZES.get(symbol, 25),
                
                # ✅ NEW ANALYSIS DATA
                "oi_changes": oi_changes,
                "oi_scenario": oi_scenario,
                "mtf_analysis": mtf_analysis,
                "pcr_momentum": pcr_momentum_data,
                "final_signal": final_signal,
                
                # Cache info
                "cache_size": self.cache.get_cache_size(symbol)
            }
            
        except Exception as e:
            logger.error(f"❌ Error analyzing {symbol}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None


# ======================== CHART GENERATOR (Enhanced) ========================
class ChartGenerator:
    @staticmethod
    def create_combined_chart(analysis: Dict) -> BytesIO:
        """Create enhanced chart with OI changes and signals"""
        symbol = analysis["symbol"]
        candles = analysis["candles"]
        current_price = analysis["current_price"]
        overall_pcr = analysis["overall_pcr"]
        final_signal = analysis["final_signal"]
        oi_changes = analysis.get("oi_changes", {})
        mtf_analysis = analysis.get("mtf_analysis", {})
        pcr_momentum = analysis.get("pcr_momentum", {})
        lot_size = analysis.get("lot_size", 25)
        
        now_time = datetime.now(IST).strftime('%H:%M:%S IST')
        
        # Determine trend color
        signal_type = final_signal["signal"]
        if "BULLISH" in signal_type.value:
            trend_color = "#26a69a"
        elif "BEARISH" in signal_type.value:
            trend_color = "#ef5350"
        else:
            trend_color = "#757575"
        
        # Create figure - 28x20 inches
        fig = plt.figure(figsize=(28, 20), facecolor='white')
        gs = GridSpec(6, 2, height_ratios=[2.5, 0.8, 0.8, 0.8, 0.8, 1.2], width_ratios=[1.5, 1], hspace=0.4, wspace=0.3)
        
        # ========== CANDLESTICK CHART ==========
        ax1 = fig.add_subplot(gs[0, :])
        
        mc = mpf.make_marketcolors(
            up='#26a69a', down='#ef5350',
            edge='inherit',
            wick={'up': '#26a69a', 'down': '#ef5350'},
            volume='in', alpha=0.9
        )
        
        s = mpf.make_mpf_style(
            marketcolors=mc, gridstyle='--', gridcolor='#e0e0e0',
            facecolor='white', figcolor='white', y_on_right=False
        )
        
        candles_display = candles.tail(100)
        
        mpf.plot(
            candles_display, type='candle', style=s, ax=ax1,
            volume=False, show_nontrading=False
        )
        
        # Title with signal
        title_text = f"{symbol} | Spot: ₹{current_price:,.2f} | {signal_type.value} | Confidence: {final_signal['confidence_score']}/10 | PCR: {overall_pcr:.2f} | ⏰ {now_time}"
        ax1.set_title(title_text, fontsize=18, fontweight='bold', pad=20, color='#1a1a1a')
        ax1.grid(True, alpha=0.3)
        
        # ========== SIGNAL BOX (LEFT) ==========
        ax_signal = fig.add_subplot(gs[1, 0])
        ax_signal.axis('off')
        
        signal_text = f"🎯 TRADING SIGNAL\n" + "="*50 + "\n\n"
        signal_text += f"Signal: {final_signal['signal'].value}\n"
        signal_text += f"Action: {final_signal['action'].value}\n"
        signal_text += f"Confidence: {final_signal['confidence_score']}/10\n\n"
        
        if final_signal['option_type'] != "WAIT":
            signal_text += f"📊 Trade Setup:\n"
            signal_text += f"   Option: {final_signal['entry_strike']} {final_signal['option_type']}\n"
            signal_text += f"   SL: {final_signal['stop_loss_strike']}\n"
            signal_text += f"   Target: {final_signal['target_strike']}\n"
        
        signal_bg = '#c8e6c9' if "BULLISH" in signal_type.value else ('#ffcdd2' if "BEARISH" in signal_type.value else '#e0e0e0')
        
        ax_signal.text(0.02, 0.95, signal_text, transform=ax_signal.transAxes,
                      fontsize=11, verticalalignment='top', fontfamily='monospace',
                      bbox=dict(boxstyle='round,pad=0.8', facecolor=signal_bg, edgecolor=trend_color, alpha=0.95, linewidth=3))
        
        # ========== OI CHANGE ANALYSIS (RIGHT) ==========
        ax_oi = fig.add_subplot(gs[1, 1])
        ax_oi.axis('off')
        
        oi_text = f"📈 OI CHANGE ANALYSIS\n" + "="*40 + "\n\n"
        
        if oi_changes.get("has_previous"):
            oi_text += f"Price Change: {oi_changes['price_change']:+.2f} ({oi_changes['price_change_pct']:+.2f}%)\n"
            oi_text += f"CE OI Change: {oi_changes['ce_oi_change_pct']:+.1f}%\n"
            oi_text += f"PE OI Change: {oi_changes['pe_oi_change_pct']:+.1f}%\n"
            oi_text += f"PCR Change: {oi_changes['pcr_change']:+.3f}\n\n"
            
            scenario = analysis.get("oi_scenario", {})
            oi_text += f"Scenario #{scenario.get('scenario', 0)}:\n"
            oi_text += f"{scenario.get('description', 'N/A')[:40]}\n"
        else:
            oi_text += "⏳ Waiting for previous data...\n"
            oi_text += "(First cycle - no comparison yet)\n"
        
        ax_oi.text(0.02, 0.95, oi_text, transform=ax_oi.transAxes,
                  fontsize=10, verticalalignment='top', fontfamily='monospace',
                  bbox=dict(boxstyle='round,pad=0.7', facecolor='#e3f2fd', edgecolor='#2196f3', alpha=0.95, linewidth=2))
        
        # ========== MULTI-TIMEFRAME (LEFT) ==========
        ax_mtf = fig.add_subplot(gs[2, 0])
        ax_mtf.axis('off')
        
        mtf_text = f"⏱️ MULTI-TIMEFRAME ANALYSIS\n" + "="*50 + "\n\n"
        mtf_text += f"Trend Alignment: {mtf_analysis.get('trend_alignment', 'N/A')}\n\n"
        
        for tf in ["5min", "15min", "30min"]:
            tf_data = mtf_analysis.get(tf, {})
            if tf_data.get("available"):
                emoji = "🟢" if tf_data['pcr_trend'] == "BULLISH" else ("🔴" if tf_data['pcr_trend'] == "BEARISH" else "⚪")
                mtf_text += f"{tf}: {emoji} {tf_data['pcr_trend']} | PCR Δ: {tf_data['pcr_change']:+.2f}\n"
            else:
                mtf_text += f"{tf}: ⏳ No data\n"
        
        ax_mtf.text(0.02, 0.95, mtf_text, transform=ax_mtf.transAxes,
                   fontsize=10, verticalalignment='top', fontfamily='monospace',
                   bbox=dict(boxstyle='round,pad=0.7', facecolor='#fff3e0', edgecolor='#ff9800', alpha=0.95, linewidth=2))
        
        # ========== PCR MOMENTUM (RIGHT) ==========
        ax_pcr = fig.add_subplot(gs[2, 1])
        ax_pcr.axis('off')
        
        pcr_text = f"📊 PCR MOMENTUM\n" + "="*40 + "\n\n"
        pcr_text += f"Current PCR: {overall_pcr:.3f}\n"
        pcr_text += f"Momentum: {pcr_momentum.get('momentum', 'N/A')}\n"
        pcr_text += f"Direction: {pcr_momentum.get('direction', 'N/A')}\n"
        pcr_text += f"Strength: {pcr_momentum.get('strength', 0)}/10\n\n"
        
        # PCR Zone
        if overall_pcr > 2.5:
            pcr_text += "🟢🟢 STRONG SUPPORT ZONE\n"
        elif overall_pcr > 1.5:
            pcr_text += "🟢 Support Zone\n"
        elif overall_pcr < 0.5:
            pcr_text += "🔴🔴 STRONG RESISTANCE ZONE\n"
        elif overall_pcr < 0.7:
            pcr_text += "🔴 Resistance Zone\n"
        else:
            pcr_text += "⚪ Neutral Zone\n"
        
        ax_pcr.text(0.02, 0.95, pcr_text, transform=ax_pcr.transAxes,
                   fontsize=10, verticalalignment='top', fontfamily='monospace',
                   bbox=dict(boxstyle='round,pad=0.7', facecolor='#f3e5f5', edgecolor='#9c27b0', alpha=0.95, linewidth=2))
        
        # ========== REASONS (LEFT) ==========
        ax_reasons = fig.add_subplot(gs[3, 0])
        ax_reasons.axis('off')
        
        reasons_text = f"💡 SIGNAL REASONS\n" + "="*50 + "\n\n"
        for reason in final_signal.get("reasons", [])[:5]:
            reasons_text += f"✓ {reason}\n"
        
        if not final_signal.get("should_alert"):
            reasons_text += f"\n⚠️ {final_signal.get('alert_reason', 'Low confidence')}"
        
        ax_reasons.text(0.02, 0.95, reasons_text, transform=ax_reasons.transAxes,
                       fontsize=10, verticalalignment='top', fontfamily='monospace',
                       bbox=dict(boxstyle='round,pad=0.7', facecolor='#e8f5e9', edgecolor='#4caf50', alpha=0.95, linewidth=2))
        
        # ========== CACHE INFO (RIGHT) ==========
        ax_cache = fig.add_subplot(gs[3, 1])
        ax_cache.axis('off')
        
        cache_text = f"💾 SYSTEM INFO\n" + "="*40 + "\n\n"
        cache_text += f"Cached Snapshots: {analysis.get('cache_size', 0)}\n"
        cache_text += f"Expiry: {analysis['expiry']}\n"
        cache_text += f"Lot Size: {lot_size}\n"
        cache_text += f"ATM Range: ±{ATM_RANGE} strikes\n"
        cache_text += f"Min Confidence: {SIGNAL_THRESHOLDS['MIN_CONFIDENCE_SCORE']}/10\n"
        
        ax_cache.text(0.02, 0.95, cache_text, transform=ax_cache.transAxes,
                     fontsize=10, verticalalignment='top', fontfamily='monospace',
                     bbox=dict(boxstyle='round,pad=0.7', facecolor='#fce4ec', edgecolor='#e91e63', alpha=0.95, linewidth=2))
        
        # ========== OPTION CHAIN TABLE (FULL WIDTH) ==========
        ax_table = fig.add_subplot(gs[4:, :])
        ax_table.axis('tight')
        ax_table.axis('off')
        
        # Build table with OI changes
        strike_changes = oi_changes.get("strike_changes", {})
        
        table_data = [["Strike", "PE OI", "PE Δ%", "CE OI", "CE Δ%", "PCR", "CE ₹", "PE ₹", "Zone"]]
        
        atm_strike = analysis['atm_strike']
        total_ce_oi = analysis["total_ce_oi"]
        total_pe_oi = analysis["total_pe_oi"]
        
        for i, strike in enumerate(analysis["strikes"]):
            ce = analysis["ce_data"][i]
            pe = analysis["pe_data"][i]
            
            pcr = pe.get("pcr", 0)
            
            # Get OI changes for this strike
            s_change = strike_changes.get(strike, {})
            ce_change_pct = s_change.get("ce_oi_change_pct", 0)
            pe_change_pct = s_change.get("pe_oi_change_pct", 0)
            
            # Format OI change with color indicator
            ce_delta = f"{ce_change_pct:+.1f}%" if ce_change_pct != 0 else "—"
            pe_delta = f"{pe_change_pct:+.1f}%" if pe_change_pct != 0 else "—"
            
            # Zone determination
            if pcr > 2.5:
                zone = "🟢🟢 SUPPORT"
            elif pcr > 1.5:
                zone = "🟢 Support"
            elif pcr < 0.5:
                zone = "🔴🔴 RESIST"
            elif pcr < 0.7:
                zone = "🔴 Resist"
            else:
                zone = "⚪ Neutral"
            
            # Format numbers
            pe_oi_str = f"{pe['oi']/1000:.1f}K" if pe['oi'] < 1000000 else f"{pe['oi']/1000000:.2f}M"
            ce_oi_str = f"{ce['oi']/1000:.1f}K" if ce['oi'] < 1000000 else f"{ce['oi']/1000000:.2f}M"
            
            row = [
                f"₹{strike:,.0f}{'*' if strike == atm_strike else ''}",
                pe_oi_str,
                pe_delta,
                ce_oi_str,
                ce_delta,
                f"{pcr:.2f}",
                f"₹{ce['ltp']:.1f}",
                f"₹{pe['ltp']:.1f}",
                zone
            ]
            table_data.append(row)
        
        # Overall row
        overall_ce_change = oi_changes.get("ce_oi_change_pct", 0)
        overall_pe_change = oi_changes.get("pe_oi_change_pct", 0)
        
        table_data.append([
            "OVERALL",
            f"{total_pe_oi/1000000:.2f}M",
            f"{overall_pe_change:+.1f}%",
            f"{total_ce_oi/1000000:.2f}M",
            f"{overall_ce_change:+.1f}%",
            f"{overall_pcr:.2f}",
            "", "",
            final_signal['signal'].value[:15]
        ])
        
        table = ax_table.table(
            cellText=table_data, loc='center', cellLoc='center',
            colWidths=[0.11, 0.10, 0.08, 0.10, 0.08, 0.08, 0.10, 0.10, 0.14]
        )
        
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 2.8)
        
        # Style header
        for i in range(9):
            table[(0, i)].set_facecolor('#37474f')
            table[(0, i)].set_text_props(weight='bold', color='white', fontsize=11)
        
        # Style summary row
        summary_row = len(table_data) - 1
        for i in range(9):
            table[(summary_row, i)].set_facecolor('#ffd54f')
            table[(summary_row, i)].set_text_props(weight='bold', fontsize=11)
        
        # Highlight ATM row
        for i, strike in enumerate(analysis["strikes"], 1):
            if strike == atm_strike:
                for j in range(9):
                    table[(i, j)].set_facecolor('#bbdefb')
                    table[(i, j)].set_text_props(weight='bold')
        
        plt.tight_layout()
        
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=200, facecolor='white', bbox_inches='tight')
        buf.seek(0)
        plt.close()
        
        return buf


# ======================== TELEGRAM ALERTER (Enhanced) ========================
class TelegramAlerter:
    def __init__(self, token: str, chat_id: str):
        self.bot = Bot(token=token)
        self.chat_id = chat_id
    
    async def send_chart(self, chart_buffer: BytesIO, symbol: str, analysis: Dict):
        try:
            final_signal = analysis.get("final_signal", {})
            oi_changes = analysis.get("oi_changes", {})
            mtf_analysis = analysis.get("mtf_analysis", {})
            
            signal_type = final_signal.get("signal", SignalType.NEUTRAL)
            confidence = final_signal.get("confidence_score", 0)
            
            # Build caption
            caption = f"""📊 {symbol} Analysis v6.0

💰 Spot: ₹{analysis['current_price']:,.2f}
📅 Expiry: {analysis['expiry']}

🎯 SIGNAL: {signal_type.value}
📈 Confidence: {confidence}/10
🎬 Action: {final_signal.get('action', ActionType.WAIT).value}

📊 PCR: {analysis['overall_pcr']:.3f}
📈 CE OI: {analysis['total_ce_oi']:,.0f}
📉 PE OI: {analysis['total_pe_oi']:,.0f}"""
            
            # Add OI change info if available
            if oi_changes.get("has_previous"):
                caption += f"""

🔄 OI Changes:
   Price: {oi_changes['price_change']:+.2f} ({oi_changes['price_change_pct']:+.2f}%)
   CE OI: {oi_changes['ce_oi_change_pct']:+.1f}%
   PE OI: {oi_changes['pe_oi_change_pct']:+.1f}%
   PCR Δ: {oi_changes['pcr_change']:+.3f}"""
            
            # Add MTF info
            caption += f"""

⏱️ Multi-TF: {mtf_analysis.get('trend_alignment', 'N/A')}"""
            
            # Trade setup if applicable
            if final_signal.get("option_type") != "WAIT":
                caption += f"""

💼 Trade Setup:
   {final_signal['entry_strike']} {final_signal['option_type']}
   SL: {final_signal['stop_loss_strike']}
   TGT: {final_signal['target_strike']}"""
            
            caption += f"""

⏰ {datetime.now(IST).strftime('%d-%b %H:%M IST')}
💾 Cache: {analysis.get('cache_size', 0)} snapshots

✅ Enhanced Bot v6.0"""
            
            await self.bot.send_photo(
                chat_id=self.chat_id,
                photo=chart_buffer,
                caption=caption
            )
            
            logger.info(f"✅ Alert sent for {symbol}")
            
        except TelegramError as e:
            logger.error(f"❌ Telegram error for {symbol}: {e}")
        except Exception as e:
            logger.error(f"❌ Error sending alert for {symbol}: {e}")
    
    async def send_signal_alert(self, symbol: str, final_signal: Dict):
        """Send quick signal alert (without chart) for high-confidence signals"""
        try:
            if not final_signal.get("should_alert"):
                return
            
            signal_type = final_signal.get("signal", SignalType.NEUTRAL)
            
            message = f"""🚨 {symbol} SIGNAL ALERT 🚨

{signal_type.value}
Action: {final_signal.get('action', ActionType.WAIT).value}
Confidence: {final_signal['confidence_score']}/10

📊 Trade: {final_signal['entry_strike']} {final_signal['option_type']}
⛔ SL: {final_signal['stop_loss_strike']}
🎯 TGT: {final_signal['target_strike']}

Reasons:
""" + "\n".join([f"• {r}" for r in final_signal.get("reasons", [])[:3]])
            
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message
            )
            
        except Exception as e:
            logger.error(f"❌ Error sending signal alert: {e}")


# ======================== MAIN BOT ========================
class UpstoxOptionsBot:
    def __init__(self):
        self.client = UpstoxClient(UPSTOX_ACCESS_TOKEN)
        self.cache = InMemoryCache(max_snapshots=50)  # ✅ IN-MEMORY CACHE
        self.analyzer = OptionAnalyzer(self.client, self.cache)
        self.chart_gen = ChartGenerator()
        self.alerter = TelegramAlerter(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
    
    def is_market_open(self) -> bool:
        now = datetime.now(IST).time()
        today = datetime.now(IST).date()
        
        if today.weekday() >= 5:
            return False
            
        return MARKET_START <= now <= MARKET_END
    
    async def process_symbols(self):
        now_time = datetime.now(IST)
        
        logger.info("\n" + "="*70)
        logger.info(f"🔍 ENHANCED ANALYSIS CYCLE v6.0 - {now_time.strftime('%H:%M:%S IST')}")
        logger.info("="*70)
        
        for symbol in INDICES:
            try:
                analysis = await self.analyzer.analyze_symbol(symbol)
                
                if analysis:
                    final_signal = analysis.get("final_signal", {})
                    
                    # ✅ SIGNAL THRESHOLD CHECK - Only alert if significant
                    if final_signal.get("should_alert"):
                        chart = self.chart_gen.create_combined_chart(analysis)
                        await self.alerter.send_chart(chart, symbol, analysis)
                        logger.info(f"✅ {symbol}: ALERT SENT (Confidence {final_signal['confidence_score']}/10)")
                    else:
                        logger.info(f"⏳ {symbol}: Signal below threshold ({final_signal['confidence_score']}/10), skipping alert")
                        
                        # Still generate chart for logging (optional)
                        chart = self.chart_gen.create_combined_chart(analysis)
                        await self.alerter.send_chart(chart, symbol, analysis)  # Remove this line to skip low-confidence alerts
                else:
                    logger.warning(f"⚠️ {symbol} analysis failed")
                
                await asyncio.sleep(3)
                
            except Exception as e:
                logger.error(f"❌ Error processing {symbol}: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info("="*70)
        logger.info(f"✅ CYCLE COMPLETE | Cache sizes: {', '.join([f'{s}:{self.cache.get_cache_size(s)}' for s in INDICES])}")
        logger.info("="*70 + "\n")
    
    async def run(self):
        current_time = datetime.now(IST)
        market_status = "🟢 OPEN" if self.is_market_open() else "🔴 CLOSED"
        
        print("\n" + "="*70)
        print("🚀 ENHANCED UPSTOX OPTIONS BOT v6.0", flush=True)
        print("="*70)
        print(f"📅 {current_time.strftime('%d-%b-%Y %A')}", flush=True)
        print(f"🕐 {current_time.strftime('%H:%M:%S IST')}", flush=True)
        print(f"📊 Market: {market_status}", flush=True)
        print(f"⏱️  Interval: 5 minutes", flush=True)
        print(f"📈 Indices: {', '.join(INDICES)}", flush=True)
        print("="*70)
        print("✅ NEW FEATURES:", flush=True)
        print(f"   • OI Change Tracking (Previous vs Current)", flush=True)
        print(f"   • Multi-Timeframe Analysis (5m/15m/30m)", flush=True)
        print(f"   • PCR Momentum Analysis", flush=True)
        print(f"   • 9 OI Scenarios from PDF", flush=True)
        print(f"   • ATM Range: ±{ATM_RANGE} strikes", flush=True)
        print(f"   • In-Memory Cache (50 snapshots)", flush=True)
        print(f"   • Signal Threshold: {SIGNAL_THRESHOLDS['MIN_CONFIDENCE_SCORE']}/10", flush=True)
        print("="*70 + "\n", flush=True)
        
        await self.client.create_session()
        
        try:
            await self.client.download_instruments()
            
            while True:
                try:
                    await self.process_symbols()
                    
                    next_run = datetime.now(IST) + timedelta(seconds=ANALYSIS_INTERVAL)
                    logger.info(f"⏰ Next cycle: {next_run.strftime('%H:%M:%S')}\n")
                    
                    await asyncio.sleep(ANALYSIS_INTERVAL)
                    
                except Exception as e:
                    logger.error(f"❌ Cycle error: {e}")
                    await asyncio.sleep(60)
        
        except KeyboardInterrupt:
            logger.info("\n🛑 Bot stopped by user")
        
        finally:
            await self.client.close_session()
            logger.info("👋 Session closed")


# ======================== ENTRY POINT ========================
if __name__ == "__main__":
    bot = UpstoxOptionsBot()
    asyncio.run(bot.run())
