import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import json
import logging
import os
import warnings
warnings.filterwarnings('ignore')

try:
    import influxdb_client
except ImportError:
    influxdb_client = None


# =============================================================================
# SETUP LOGGING
# =============================================================================

def setup_logging(log_file: str = 'ostrad_backtest.log'):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('OSTRAD')


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
@dataclass 
class MarginConfig:
    margin_percent: float = 15.0
    min_margin_per_lot: float = 50000

    def calculate_margin(self, spot_price: float, lot_size: int,
                        option_price: float, quantity: int) -> float:
        """Simple static margin calculation"""
        num_lots = abs(quantity) / lot_size
        contract_value = spot_price * lot_size * num_lots
        base_margin = contract_value * (self.margin_percent / 100)
        premium_received = option_price * abs(quantity)
        
        return max(base_margin - premium_received, 
                  self.min_margin_per_lot * num_lots)


@dataclass 
class Position:
    symbol: str
    strike: float
    option_type: str
    expiry: datetime
    entry_time: datetime
    entry_price: float
    quantity: int
    lot_size: int
    sl_price: Optional[float] = None
    sl_limit_price: Optional[float] = None
    sl2_price: Optional[float] = None
    sl_order_placed: bool = False
    sl_hit: bool = False
    is_hedge: bool = False
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl: float = 0.0

    @property
    def is_short(self) -> bool:
        return self.quantity < 0

    @property
    def is_open(self) -> bool:
        return self.exit_time is None

    @property
    def lots(self) -> int:
        return abs(self.quantity) // self.lot_size


# =============================================================================
# DATA LOADER - BASE CLASS
# =============================================================================

class DataLoader(ABC):
    @abstractmethod
    def load_data(self, symbol: str, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        pass


# =============================================================================
# CSV DATA LOADER - FIXED (removed duplicate method)
# =============================================================================

class CSVDataLoader(DataLoader):
    def __init__(self, spot_file: str, options_file: str, logger: logging.Logger = None):
        self.spot_file = spot_file
        self.options_file = options_file
        self.logger = logger or logging.getLogger('OSTRAD')
        self.spot_df = None
        self.options_df = None
        self._load_files()

    def _load_files(self):
        # Load spot data
        self.spot_df = pd.read_csv(self.spot_file, parse_dates=['date_time'])
        self.spot_df.set_index('date_time', inplace=True)
        
        # Strip timezone if present
        if self.spot_df.index.tz is not None:
            self.spot_df.index = self.spot_df.index.tz_localize(None)
        
        # Rename 'index' column to 'symbol'
        if 'index' in self.spot_df.columns:
            self.spot_df = self.spot_df.rename(columns={'index': 'symbol'})
        
        # Select columns
        cols = ['open', 'high', 'low', 'close', 'volume']
        if 'symbol' in self.spot_df.columns:
            cols = ['symbol'] + cols
        
        # Add missing columns with defaults
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col not in self.spot_df.columns:
                if col == 'volume':
                    self.spot_df[col] = 100000
                else:
                    raise KeyError(f"Required column '{col}' missing in spot data")
        
        self.spot_df = self.spot_df[cols].copy()
        self.spot_df['volume'] = self.spot_df['volume'].fillna(100000)
        
        # Load options data
        self.options_df = pd.read_csv(self.options_file, parse_dates=['date_time', 'expiry_date'])
        self.options_df.set_index('date_time', inplace=True)
        self.options_df = self.options_df.rename(columns={'index': 'symbol', 'expiry_date': 'expiry'})
        
        # Strip timezone if present
        if self.options_df.index.tz is not None:
            self.options_df.index = self.options_df.index.tz_localize(None)
        
        if self.options_df['expiry'].dt.tz is not None:
            self.options_df['expiry'] = self.options_df['expiry'].dt.tz_localize(None)
        
        # Select columns
        cols = ['symbol', 'strike', 'option_type', 'expiry', 'open', 'high', 'low', 'close', 'volume']
        if 'oi' in self.options_df.columns:
            cols.append('oi')
        
        available_cols = [c for c in cols if c in self.options_df.columns]
        self.options_df = self.options_df[available_cols].copy()
        
        self.logger.info(f"Loaded {len(self.spot_df)} spot records, {len(self.options_df)} option records")

    def load_data(self, symbol: str, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        """Single load_data method (FIX: removed duplicate)"""
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date).normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)
        
        spot = self.spot_df[(self.spot_df.index >= start_ts) & (self.spot_df.index <= end_ts)].copy()
        options = self.options_df[(self.options_df.index >= start_ts) & (self.options_df.index <= end_ts)].copy()
        
        # Filter by symbol
        if 'symbol' in options.columns:
            options = options[options['symbol'] == symbol]
        
        return {'spot': spot, 'options': options}


# =============================================================================
# INFLUXDB DATA LOADER
# =============================================================================

class InfluxDBDataLoader(DataLoader):
    """
    InfluxDB loader that mirrors CSVDataLoader behavior.
    """

    def __init__(self, url: str, token: str, org: str,
                 bucket: str = None,
                 spot_bucket: str = None,
                 option_bucket: str = None,
                 spot_measurement: str = "fut_spot_merged",
                 option_measurement: str = "options_1min",
                 symbol: str = "NIFTY",
                 preload_start: datetime = None,
                 preload_end: datetime = None,
                 logger: logging.Logger = None):
        if influxdb_client is None:
            raise ImportError("influxdb-client is not installed. Run: pip install influxdb-client")

        self.url = url
        self.token = token
        self.org = org
        self.spot_bucket = spot_bucket or bucket
        self.option_bucket = option_bucket or bucket
        if not self.spot_bucket or not self.option_bucket:
            raise ValueError("Provide either 'bucket' or both 'spot_bucket' and 'option_bucket'")
        self.spot_measurement = spot_measurement
        self.option_measurement = option_measurement
        self.symbol = symbol
        self.logger = logger or logging.getLogger('OSTRAD')

        self.client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
        self.query_api = self.client.query_api()

        self.spot_df = None
        self.options_df = None

        if preload_start and preload_end:
            self._load_data(preload_start, preload_end)

    def _load_data(self, start_date: datetime, end_date: datetime):
        """Load and preprocess all data from InfluxDB"""
        start_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
        stop_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

        # Load Spot Data
        spot_query = f'''
from(bucket: "{self.spot_bucket}")
    |> range(start: {start_str}, stop: {stop_str})
    |> filter(fn: (r) => r._measurement == "{self.spot_measurement}")
    |> filter(fn: (r) => r.data_type == "SPOT")
    |> filter(fn: (r) => r.index == "{self.symbol}")
    |> filter(fn: (r) => r._field == "open" or r._field == "high" or
                         r._field == "low"  or r._field == "close" or
                         r._field == "volume")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
        self.spot_df = self._query_to_df(spot_query)

        if not self.spot_df.empty:
            self.spot_df = self.spot_df.rename(columns={"_time": "date_time"})
            self.spot_df["date_time"] = pd.to_datetime(self.spot_df["date_time"])
            self.spot_df.set_index("date_time", inplace=True)

            if self.spot_df.index.tz is not None:
                self.spot_df.index = self.spot_df.index.tz_localize(None)

            if 'index' in self.spot_df.columns:
                self.spot_df = self.spot_df.rename(columns={'index': 'symbol'})
            else:
                self.spot_df['symbol'] = self.symbol

            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col not in self.spot_df.columns:
                    if col == 'volume':
                        self.spot_df[col] = 100000
                    else:
                        raise KeyError(f"Required column '{col}' missing in spot data")

            cols = ['symbol', 'open', 'high', 'low', 'close', 'volume']
            available_cols = [c for c in cols if c in self.spot_df.columns]
            self.spot_df = self.spot_df[available_cols].copy()
            self.spot_df['volume'] = self.spot_df['volume'].fillna(100000)
        else:
            self.spot_df = pd.DataFrame()

        # Load Options Data
        option_query = f'''
from(bucket: "{self.option_bucket}")
    |> range(start: {start_str}, stop: {stop_str})
    |> filter(fn: (r) => r._measurement == "{self.option_measurement}")
    |> filter(fn: (r) => r.index == "{self.symbol}")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
        self.options_df = self._query_to_df(option_query)

        if not self.options_df.empty:
            self.options_df = self.options_df.rename(columns={"_time": "date_time"})
            self.options_df["date_time"] = pd.to_datetime(self.options_df["date_time"])
            self.options_df.set_index("date_time", inplace=True)

            if self.options_df.index.tz is not None:
                self.options_df.index = self.options_df.index.tz_localize(None)

            if 'index' in self.options_df.columns:
                self.options_df = self.options_df.rename(columns={'index': 'symbol'})
            elif 'trading_symbol' in self.options_df.columns:
                self.options_df = self.options_df.rename(columns={'trading_symbol': 'symbol'})

            if 'expiry' not in self.options_df.columns and 'expiry_date' in self.options_df.columns:
                self.options_df = self.options_df.rename(columns={'expiry_date': 'expiry'})
            elif 'expiry' in self.options_df.columns and 'expiry_date' in self.options_df.columns:
                self.options_df = self.options_df.drop(columns=['expiry_date'])

            if 'open_interest' in self.options_df.columns:
                self.options_df = self.options_df.rename(columns={'open_interest': 'oi'})

            if 'expiry' in self.options_df.columns:
                self.options_df['expiry'] = pd.to_datetime(self.options_df['expiry'])
                if self.options_df['expiry'].dt.tz is not None:
                    self.options_df['expiry'] = self.options_df['expiry'].dt.tz_localize(None)

            if 'strike' in self.options_df.columns:
                self.options_df['strike'] = pd.to_numeric(self.options_df['strike'], errors='coerce')

            cols = ['symbol', 'strike', 'option_type', 'expiry', 'open', 'high', 'low', 'close', 'volume']
            if 'oi' in self.options_df.columns:
                cols.append('oi')
            available_cols = [c for c in cols if c in self.options_df.columns]
            self.options_df = self.options_df[available_cols].copy()
        else:
            self.options_df = pd.DataFrame()

        self.logger.info(
            f"Loaded {len(self.spot_df)} spot records, {len(self.options_df)} option records from InfluxDB"
        )

    def _query_to_df(self, query: str) -> pd.DataFrame:
        """Run a Flux query and always return a single DataFrame"""
        try:
            result = self.query_api.query_data_frame(query)
            if isinstance(result, list):
                return pd.concat(result, ignore_index=True) if result else pd.DataFrame()
            return result if result is not None else pd.DataFrame()
        except Exception as e:
            self.logger.error(f"InfluxDB query error: {e}")
            return pd.DataFrame()

    def load_data(self, symbol: str, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        if self.spot_df is None or self.options_df is None:
            self._load_data(start_date, end_date)

        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date).normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)

        spot = self.spot_df[(self.spot_df.index >= start_ts) & (self.spot_df.index <= end_ts)].copy()
        options = self.options_df[(self.options_df.index >= start_ts) & (self.options_df.index <= end_ts)].copy()

        if 'symbol' in options.columns:
            options = options[options['symbol'] == symbol]

        return {'spot': spot, 'options': options}

    def close(self):
        """Close the InfluxDB client connection"""
        if self.client:
            self.client.close()


# =============================================================================
# VWAP INDICATOR
# =============================================================================

class VWAPIndicator:
    @staticmethod
    def calculate(df: pd.DataFrame, sell_threshold: float = 15.0) -> pd.DataFrame:
        _temp = df.copy()
        _temp['traded_volume'] = _temp['close'] * _temp['volume']
        _temp['indicator'] = _temp['traded_volume'].cumsum() / _temp['volume'].cumsum()
        _temp['diff'] = _temp['close'] - _temp['indicator']
        _temp['order_side'] = _temp['diff'].apply(lambda x: 'buy' if x > 0 else 'sell')
        _temp['perc_diff'] = (_temp['diff'].abs() / _temp['indicator'] * 100).round(2)
        _temp['can_trade'] = _temp.apply(
            lambda x: x['perc_diff'] <= sell_threshold if x['order_side'] == 'sell' else x['perc_diff'] <= sell_threshold,
            axis=1)
        _temp['signal'] = _temp.apply(
            lambda x: x['order_side'] if x['can_trade'] else np.nan, axis=1)
        return _temp


# =============================================================================
# MAIN BACKTESTER ENGINE - WITH ALL FIXES APPLIED
# =============================================================================

class OSTRADBacktester:
    def __init__(self, symbol: str, params: Dict, data_loader: DataLoader, logger: logging.Logger = None):
        self.symbol = symbol
        self.params = params
        self.data_loader = data_loader
        self.logger = logger or logging.getLogger('OSTRAD')
        
        self.margin_config = MarginConfig(
            var_margin_percent=params.get('var_margin_percent', 12.0),
            elm_percent=params.get('elm_percent', 3.0),
            span_multiplier=params.get('span_multiplier', 1.0),
            min_margin_per_lot=params.get('min_margin_per_lot', 50000)
        )
        
        self.positions: List[Position] = []
        self.closed_positions: List[Position] = []
        self.traded_strikes: Dict[str, bool] = {}  # Keep for backward compatibility
        
        self.spot_data: pd.DataFrame = pd.DataFrame()
        self.options_data: pd.DataFrame = pd.DataFrame()
        self._option_index: pd.DataFrame = pd.DataFrame()
        
        self.trade_log: List[Dict] = []
        self.is_first_hedge: bool = True
        self.hedge_strikes: List[str] = []
        
        # Daily tracking
        self.daily_stats: List[Dict] = []

    # -------------------------------------------------------------------------
    # DATA LOADING
    # -------------------------------------------------------------------------

    def load_market_data(self, start_date: datetime, end_date: datetime) -> None:
        data = self.data_loader.load_data(self.symbol, start_date, end_date)
        self.spot_data = data['spot']
        self.options_data = data['options']
        self._build_option_index()
        
        self.logger.info(f"{self.symbol}: {len(self.spot_data)} spot candles, {len(self.options_data)} option records")

    def _build_option_index(self) -> None:
        df = self.options_data.copy()
        if df.empty:
            return
        df['_date'] = df.index.date
        df['_expiry_date'] = df['expiry'].dt.date
        self._option_index = df

    # -------------------------------------------------------------------------
    # PRICE HELPERS
    # -------------------------------------------------------------------------

    def get_spot_price(self, timestamp: datetime) -> float:
        data = self.spot_data[self.spot_data.index <= timestamp]
        return float(data['close'].iloc[-1]) if not data.empty else 0.0

    def get_atm_strike(self, timestamp: datetime) -> float:
        spot = self.get_spot_price(timestamp)
        if spot == 0:
            return 0
        strike_diff = self.params['strike_diff']
        return round(spot / strike_diff) * strike_diff

    def get_option_price(self, strike: float, option_type: str,
                         expiry: datetime, timestamp: datetime,
                         price_type: str = 'close') -> float:
        if self._option_index.empty:
            return 0.0
        expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
        mask = (
            (self._option_index['strike'] == strike) &
            (self._option_index['option_type'] == option_type) &
            (self._option_index['_expiry_date'] == expiry_date) &
            (self._option_index.index <= timestamp)
        )
        filtered = self._option_index[mask]
        return float(filtered[price_type].iloc[-1]) if not filtered.empty else 0.0

    def get_option_candle_high(self, strike: float, option_type: str,
                               expiry: datetime, current_date: date) -> float:
        if self._option_index.empty:
            return 0.0
        expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
        mask = (
            (self._option_index['strike'] == strike) &
            (self._option_index['option_type'] == option_type) &
            (self._option_index['_expiry_date'] == expiry_date) &
            (self._option_index['_date'] == current_date)
        )
        filtered = self._option_index[mask]
        return float(filtered['high'].max()) if not filtered.empty else 0.0

    def get_nearest_expiry(self, timestamp: datetime) -> Optional[datetime]:
        today = pd.Timestamp(timestamp).normalize()
        expiries = self.options_data[self.options_data['expiry'] >= today]['expiry'].unique()
        return min(expiries) if len(expiries) > 0 else None

    def get_adjusted_dte(self, expiry, current_date: date) -> int:
        expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
        raw_days = (expiry_date - current_date).days
        adjusted = raw_days
        holidays = self.params.get('holidays', [])
        for i in range(raw_days):
            check_day = current_date + timedelta(days=i)
            if check_day.weekday() in [5, 6] or check_day in holidays:
                adjusted -= 1
        return adjusted

    # -------------------------------------------------------------------------
    # MARGIN CALCULATION
    # -------------------------------------------------------------------------

    def calculate_margin(self, timestamp: datetime) -> Dict:
        total_margin = 0.0
        spot_price = self.get_spot_price(timestamp)
        lot_size = self.params['lot_size']
        capital = self.params['capital']

        for pos in self.positions:
            if not pos.is_open:
                continue
            opt_price = self.get_option_price(pos.strike, pos.option_type, 
                                            pos.expiry, timestamp)
            margin = self.margin_config.calculate_margin(
                spot_price=spot_price,
                lot_size=lot_size,
                option_price=opt_price,
                quantity=pos.quantity
            )
            total_margin += margin

        margin_percent = (total_margin / capital) * 100 if capital > 0 else 0
        return {'total_margin': total_margin, 'margin_percent': margin_percent}

    # -------------------------------------------------------------------------
    # VWAP SIGNAL
    # -------------------------------------------------------------------------

    def get_straddle_vwap_signal(self, strike: float, expiry: datetime, 
                                  timestamp: datetime, current_date: date) -> Optional[Dict]:
        if self._option_index.empty:
            return None

        expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
        day_start = datetime.combine(current_date, datetime.strptime("09:15:00", "%H:%M:%S").time())

        base_mask = (
            (self._option_index['strike'] == strike) &
            (self._option_index['_expiry_date'] == expiry_date) &
            (self._option_index.index >= day_start) &
            (self._option_index.index <= timestamp)
        )

        ce_data = self._option_index[base_mask & (self._option_index['option_type'] == 'CE')][['close', 'volume']].copy()
        pe_data = self._option_index[base_mask & (self._option_index['option_type'] == 'PE')][['close', 'volume']].copy()

        if ce_data.empty or pe_data.empty or len(ce_data) < 2 or len(pe_data) < 2:
            return None

        merged = ce_data.join(pe_data, how='inner', lsuffix='_ce', rsuffix='_pe')
        if len(merged) < 2:
            return None

        straddle = pd.DataFrame(index=merged.index)
        straddle['close'] = merged['close_ce'] + merged['close_pe']
        straddle['volume'] = merged[['volume_ce', 'volume_pe']].min(axis=1)

        result = VWAPIndicator.calculate(
            straddle.reset_index().rename(columns={straddle.index.name or 'index': 'date'}),
            sell_threshold=self.params['threshold_percentage'])

        if 'date' in result.columns:
            result.set_index('date', inplace=True)

        if len(result) < 2:
            return None

        prev = result.iloc[-2]
        return {
            'signal': prev['signal'],
            'can_trade': prev['can_trade'],
            'indicator': prev['indicator'],
            'close': prev['close'],
            'perc_diff': prev['perc_diff'],
            'ce_close': merged.iloc[-2]['close_ce'],
            'pe_close': merged.iloc[-2]['close_pe']
        }

    # -------------------------------------------------------------------------
    # POSITION HELPERS
    # -------------------------------------------------------------------------

    def get_open_shorts(self) -> List[Position]:
        return [p for p in self.positions if p.is_open and p.is_short and not p.is_hedge]

    def get_open_hedges(self) -> List[Position]:
        return [p for p in self.positions if p.is_open and p.is_hedge]

    def count_open_by_type(self, option_type: str) -> int:
        return sum(1 for p in self.get_open_shorts() if p.option_type == option_type)

    # -------------------------------------------------------------------------
    # FIX 4: Check if strike is already OPEN (not just traded)
    # -------------------------------------------------------------------------
    
    def _is_strike_already_open(self, strike: float, option_type: str, expiry_date) -> bool:
        """
        FIX 4: Only block entry if position is STILL OPEN for that strike.
        This allows re-entry after SL hit.
        """
        return any(
            p.strike == strike and
            p.option_type == option_type and
            p.is_open and 
            not p.is_hedge
            for p in self.positions
        )

    # -------------------------------------------------------------------------
    # SIGNAL CHECK - FIXED with _is_strike_already_open
    # -------------------------------------------------------------------------

    def check_signals(self, timestamp: datetime, current_date: date) -> List[Dict]:
        signals = []
        atm = self.get_atm_strike(timestamp)
        if atm == 0:
            return signals

        expiry = self.get_nearest_expiry(timestamp)
        if expiry is None:
            return signals

        dte = self.get_adjusted_dte(expiry, current_date)
        if dte not in self.params['active_days_to_expiry']:
            return signals

        spot_price = self.get_spot_price(timestamp)
        min_premium = spot_price * (self.params['min_premium_percent'] / 100)
        strike_diff = self.params['strike_diff']
        num_strikes = self.params['num_strikes']
        expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry

        for offset in range(-num_strikes, num_strikes + 1):
            strike = atm + (offset * strike_diff)
            vwap = self.get_straddle_vwap_signal(strike, expiry, timestamp, current_date)

            if vwap is None or vwap['signal'] != 'sell' or not vwap['can_trade']:
                continue

            for opt_type, opt_price in [('CE', vwap['ce_close']), ('PE', vwap['pe_close'])]:
                # FIX 4: Use _is_strike_already_open instead of traded_strikes
                if self._is_strike_already_open(strike, opt_type, expiry_date):
                    continue
                    
                if opt_price > min_premium:
                    signals.append({
                        'strike': strike,
                        'option_type': opt_type,
                        'expiry': expiry,
                        'entry_price': opt_price,
                        'timestamp': timestamp
                    })

        return signals

    # -------------------------------------------------------------------------
    # TAKE POSITION
    # -------------------------------------------------------------------------

    def take_position(self, signal: Dict, timestamp: datetime) -> Optional[Position]:
        margin_info = self.calculate_margin(timestamp)
        if margin_info['margin_percent'] >= self.params['new_position_margin_limit']:
            self.logger.debug(f"Margin limit reached: {margin_info['margin_percent']:.1f}%")
            return None

        ce_count = self.count_open_by_type('CE')
        pe_count = self.count_open_by_type('PE')

        if signal['option_type'] == 'CE' and ce_count >= self.params['max_open_strikes_per_leg']:
            return None
        if signal['option_type'] == 'PE' and pe_count >= self.params['max_open_strikes_per_leg']:
            return None

        entry_price = self.get_option_price(signal['strike'], signal['option_type'],
                                            signal['expiry'], timestamp, price_type='close')
        if entry_price <= 0:
            return None

        lot_size = self.params['lot_size']
        quantity = -self.params['quantity_per_strike']

        sl_perc = self.params['sl_percentage']
        sl_price = entry_price * (1 + sl_perc / 100)
        tick_size = 0.05
        sl_price = round(sl_price / tick_size) * tick_size
        sl_limit_price = sl_price * (1 + self.params['sl_diff_percentage'] / 100)
        sl_limit_price = round(sl_limit_price / tick_size) * tick_size

        pos = Position(
            symbol=self.symbol,
            strike=signal['strike'],
            option_type=signal['option_type'],
            expiry=signal['expiry'],
            entry_time=timestamp,
            entry_price=entry_price,
            quantity=quantity,
            lot_size=lot_size,
            sl_price=round(sl_price, 2),
            sl_limit_price=round(sl_limit_price, 2),
            sl_order_placed=True
        )

        self.positions.append(pos)
        
        # Keep traded_strikes for logging/debugging (but don't use for blocking)
        expiry_date = signal['expiry'].date() if hasattr(signal['expiry'], 'date') else signal['expiry']
        self.traded_strikes[f"{signal['strike']}_{signal['option_type']}_{expiry_date}"] = True

        self._log(timestamp, signal['strike'], signal['option_type'],
                  'ENTRY', f"Qty:{quantity} EP:{entry_price:.2f} SL:{sl_price:.2f}")
        return pos

    # -------------------------------------------------------------------------
    # HEDGE
    # -------------------------------------------------------------------------

    def check_and_take_hedge(self, timestamp: datetime) -> List[Position]:
        hedges = []
        margin_info = self.calculate_margin(timestamp)
        if margin_info['margin_percent'] < self.params['hedge_margin_limit']:
            return hedges

        open_shorts = self.get_open_shorts()
        if not open_shorts:
            return hedges

        spot_price = self.get_spot_price(timestamp)
        strike_diff = self.params['strike_diff']
        lot_size = self.params['lot_size']
        expiry = self.get_nearest_expiry(timestamp)
        if expiry is None:
            return hedges

        hedge_pct = self.params['hedge_strike_diff_percent'] if self.is_first_hedge else self.params['hedge_strike_diff_percent_2']

        existing_hedges = self.get_open_hedges()
        ce_short_qty = sum(abs(p.quantity) for p in open_shorts if p.option_type == 'CE')
        pe_short_qty = sum(abs(p.quantity) for p in open_shorts if p.option_type == 'PE')
        ce_hedge_qty = sum(p.quantity for p in existing_hedges if p.option_type == 'CE')
        pe_hedge_qty = sum(p.quantity for p in existing_hedges if p.option_type == 'PE')

        for opt_type, short_qty, hedge_qty, direction in [
            ('CE', ce_short_qty, ce_hedge_qty, 1),
            ('PE', pe_short_qty, pe_hedge_qty, -1)
        ]:
            if short_qty == 0 or hedge_qty >= short_qty // 2:
                continue

            hedge_strike = round(spot_price * (1 + direction * hedge_pct / 100) / strike_diff) * strike_diff
            hedge_qty_to_take = max(lot_size, round(short_qty / lot_size / 2) * lot_size)

            entry_price = self.get_option_price(hedge_strike, opt_type, expiry, timestamp)
            if entry_price <= 0:
                continue

            hedge_pos = Position(
                symbol=self.symbol, strike=hedge_strike, option_type=opt_type,
                expiry=expiry, entry_time=timestamp, entry_price=entry_price,
                quantity=int(hedge_qty_to_take), lot_size=lot_size, is_hedge=True
            )
            self.positions.append(hedge_pos)
            self.hedge_strikes.append(str(hedge_strike))
            hedges.append(hedge_pos)
            self._log(timestamp, hedge_strike, opt_type,
                      'HEDGE_ENTRY', f"BUY qty:{hedge_qty_to_take} EP:{entry_price:.2f}")

        if hedges:
            self.is_first_hedge = False

        return hedges

    # -------------------------------------------------------------------------
    # SL HIT CHECK
    # -------------------------------------------------------------------------

    def check_sl_hits(self, timestamp: datetime) -> List[Position]:
        hit_positions = []

        for pos in self.positions:
            if not pos.is_open or not pos.is_short or pos.sl_hit or pos.sl_price is None:
                continue

            candle_high = self.get_option_price(pos.strike, pos.option_type, pos.expiry, timestamp, price_type='high')
            if candle_high <= 0:
                continue

            if candle_high >= pos.sl_price:
                pos.sl_hit = True
                pos.exit_time = timestamp
                pos.exit_price = min(pos.sl_limit_price if pos.sl_limit_price else candle_high, candle_high)
                pos.exit_reason = 'SL2_HIT' if pos.sl2_price else 'SL_HIT'
                pos.pnl = (pos.entry_price - pos.exit_price) * abs(pos.quantity)
                self.closed_positions.append(pos)
                hit_positions.append(pos)
                self._log(timestamp, pos.strike, pos.option_type,
                          pos.exit_reason, f"Exit:{pos.exit_price:.2f} PnL:{pos.pnl:.2f}")

        return hit_positions

    # -------------------------------------------------------------------------
    # SL MODIFICATION
    # -------------------------------------------------------------------------

    def modify_sl_to_high(self, timestamp: datetime, current_date: date) -> List[Position]:
        modified = []

        for pos in self.positions:
            if not pos.is_open or not pos.is_short or pos.sl_hit or pos.sl2_price is not None:
                continue

            day_high = self.get_option_candle_high(pos.strike, pos.option_type, pos.expiry, current_date)
            if day_high <= 0:
                continue

            if pos.sl_price and day_high < pos.sl_price:
                tick_size = 0.05
                new_sl = round((day_high + 1) / tick_size) * tick_size
                new_sl = round(new_sl, 2)
                new_limit = round(new_sl * (1 + self.params['sl_diff_percentage'] / 100) / tick_size) * tick_size
                new_limit = round(new_limit, 2)

                pos.sl2_price = new_sl
                pos.sl_price = new_sl
                pos.sl_limit_price = new_limit
                modified.append(pos)
                self._log(timestamp, pos.strike, pos.option_type,
                          'SL_MODIFIED', f"NewSL:{new_sl:.2f} Limit:{new_limit:.2f}")

        return modified

    # -------------------------------------------------------------------------
    # RMS CHECK
    # -------------------------------------------------------------------------

    def run_rms_checks(self, timestamp: datetime, current_date: date) -> List[Position]:
        rms_exits = []
        atm = self.get_atm_strike(timestamp)
        strike_diff = self.params['strike_diff']
        num_strikes = self.params['num_strikes']
        min_strike = atm - (num_strikes * strike_diff)
        max_strike = atm + (num_strikes * strike_diff)

        for pos in self.positions:
            if not pos.is_open or pos.is_hedge:
                continue

            exit_reason = None
            
            if pos.quantity > 0:
                exit_reason = 'RMS_POSITIVE_QTY'
            elif pos.strike < min_strike or pos.strike > max_strike:
                exit_reason = 'RMS_INVALID_STRIKE'
            elif pos.sl_hit and pos.exit_time is None:
                exit_reason = 'RMS_SL_HIT_OPEN'

            if exit_reason:
                exit_price = self.get_option_price(pos.strike, pos.option_type, pos.expiry, timestamp)
                pos.exit_time = timestamp
                pos.exit_price = exit_price
                pos.exit_reason = exit_reason
                pos.pnl = ((pos.entry_price - exit_price) * abs(pos.quantity)
                          if pos.is_short else
                          (exit_price - pos.entry_price) * pos.quantity)
                self.closed_positions.append(pos)
                rms_exits.append(pos)
                self._log(timestamp, pos.strike, pos.option_type,
                          exit_reason, f"Exit:{exit_price:.2f}")

        return rms_exits

    # -------------------------------------------------------------------------
    # EOD SQUARE OFF - FIX 6: Added fallback for missing prices
    # -------------------------------------------------------------------------

    def square_off_all(self, timestamp: datetime) -> List[Position]:
        squared = []

        # Short positions first, then hedges
        for is_short_first in [True, False]:
            for pos in self.positions:
                if not pos.is_open:
                    continue
                if is_short_first and not pos.is_short:
                    continue
                if not is_short_first and pos.is_short:
                    continue

                exit_price = self.get_option_price(pos.strike, pos.option_type, pos.expiry, timestamp)
                
                # FIX 6: Fallback if exit price is 0
                if exit_price <= 0:
                    # Try 1 minute before
                    fallback_time = timestamp - timedelta(minutes=1)
                    exit_price = self.get_option_price(pos.strike, pos.option_type, pos.expiry, fallback_time)
                
                if exit_price <= 0:
                    # Try 5 minutes before
                    fallback_time = timestamp - timedelta(minutes=5)
                    exit_price = self.get_option_price(pos.strike, pos.option_type, pos.expiry, fallback_time)
                
                if exit_price <= 0:
                    # Last resort: use entry price (no P&L)
                    exit_price = pos.entry_price
                    self.logger.warning(f"No exit price found for {pos.strike}{pos.option_type}, using entry price")

                pos.exit_time = timestamp
                pos.exit_price = exit_price
                pos.exit_reason = 'EOD_SQUAREOFF'
                pos.pnl = ((pos.entry_price - exit_price) * abs(pos.quantity)
                          if pos.is_short else
                          (exit_price - pos.entry_price) * pos.quantity)
                self.closed_positions.append(pos)
                squared.append(pos)
                self._log(timestamp, pos.strike, pos.option_type,
                          'EOD_SQUAREOFF', f"Exit:{exit_price:.2f} PnL:{pos.pnl:.2f}")

        return squared

    # -------------------------------------------------------------------------
    # LOGGING
    # -------------------------------------------------------------------------

    def _log(self, timestamp: datetime, strike: float, option_type: str,
             event: str, message: str) -> None:
        self.trade_log.append({
            'timestamp': timestamp,
            'symbol': self.symbol,
            'strike': strike,
            'option_type': option_type,
            'event': event,
            'message': message
        })

    # -------------------------------------------------------------------------
    # RUN BACKTEST FOR SINGLE DAY
    # -------------------------------------------------------------------------

    def run_day(self, current_date: date) -> Dict:
        """Run backtest for a single day"""
        self.positions = []
        self.closed_positions = []
        self.traded_strikes = {}
        self.is_first_hedge = True
        self.hedge_strikes = []
        
        day_start = datetime.combine(current_date, datetime.strptime(self.params['start_time'], "%H:%M:%S").time())
        entry_end = datetime.combine(current_date, datetime.strptime(self.params['entry_end_time'], "%H:%M:%S").time())
        sl_mod_time = datetime.combine(current_date, datetime.strptime(self.params['sl_modification_time'], "%H:%M:%S").time())
        eod_time = datetime.combine(current_date, datetime.strptime(self.params['square_off_time'], "%H:%M:%S").time())

        timestamps = pd.date_range(start=day_start, end=eod_time, freq=f'{self.params["time_frame"]}min')
        
        sl_modified_today = False
        day_pnl = 0.0
        trades_today = 0
        
        # FIX 3: Track last signal candle to avoid duplicate signals
        last_signal_candle = None
        
        # Determine candle boundary based on time_frame
        signal_timeframe = self.params.get('signal_timeframe', self.params['time_frame'])
        
        for timestamp in timestamps:
            # Skip if no spot data yet
            if self.spot_data[self.spot_data.index <= timestamp].empty:
                continue

            # RMS checks
            for pos in self.run_rms_checks(timestamp, current_date):
                day_pnl += pos.pnl

            # Hedge check
            self.check_and_take_hedge(timestamp)

            # FIX 3: Only check signals on candle boundary
            candle_minutes = signal_timeframe if signal_timeframe > 1 else 5  # Default to 5-min for 1-min data
            candle_boundary = timestamp.replace(second=0, microsecond=0)
            candle_boundary = candle_boundary - timedelta(minutes=candle_boundary.minute % candle_minutes)
            
            # Entry signals - only on new candle
            if timestamp <= entry_end and candle_boundary != last_signal_candle:
                for signal in self.check_signals(timestamp, current_date):
                    pos = self.take_position(signal, timestamp)
                    if pos:
                        trades_today += 1
                last_signal_candle = candle_boundary

            # SL Modification (once per day)
            if timestamp >= sl_mod_time and not sl_modified_today:
                mods = self.modify_sl_to_high(timestamp, current_date)
                if mods:
                    sl_modified_today = True

            # SL Hits
            for pos in self.check_sl_hits(timestamp):
                day_pnl += pos.pnl

        # EOD square off
        for pos in self.square_off_all(eod_time):
            day_pnl += pos.pnl

        # Calculate statistics
        day_stats = {
            'date': current_date,
            'symbol': self.symbol,
            'trades_count': trades_today,
            'positions_opened': len([p for p in self.closed_positions if p.exit_reason != 'EOD_SQUAREOFF']),
            'sl_hits': len([p for p in self.closed_positions if 'SL' in str(p.exit_reason)]),
            'eod_squareoffs': len([p for p in self.closed_positions if p.exit_reason == 'EOD_SQUAREOFF']),
            'rms_exits': len([p for p in self.closed_positions if 'RMS' in str(p.exit_reason)]),
            'day_pnl': round(day_pnl, 2),
            'total_trades': len(self.closed_positions),
            'winning_trades': len([p for p in self.closed_positions if p.pnl > 0]),
            'losing_trades': len([p for p in self.closed_positions if p.pnl < 0])
        }
        
        self.daily_stats.append(day_stats)
        return day_stats

    # -------------------------------------------------------------------------
    # RUN FULL BACKTEST - FIX 1 & 2 Applied
    # -------------------------------------------------------------------------

    def run_backtest(self, start_date: datetime, end_date: datetime) -> Dict:
        self.logger.info("=" * 60)
        self.logger.info(f"OSTRAD BACKTEST: {self.symbol}")
        self.logger.info(f"Period: {start_date.date()} to {end_date.date()}")
        self.logger.info("=" * 60)

        self.load_market_data(start_date, end_date)

        current_date = pd.Timestamp(start_date).date()
        end_date_only = pd.Timestamp(end_date).date()

        all_trades = []
        
        while current_date <= end_date_only:
            if current_date.weekday() >= 5:  # Skip weekends
                current_date += timedelta(days=1)
                continue

            day_stats = self.run_day(current_date)
            self.logger.info(f"{current_date}: Trades={day_stats['trades_count']}, "
                           f"Total={day_stats['total_trades']}, P&L=₹{day_stats['day_pnl']:,.2f}")
            
            # FIX 1: Collect trades IMMEDIATELY after run_day (before positions reset)
            all_trades.extend([self._pos_to_dict(p) for p in self.closed_positions])
            
            current_date += timedelta(days=1)

        # FIX 2: Pass all_trades to summary calculation
        results = {
            'symbol': self.symbol,
            'daily_stats': self.daily_stats,
            'trades': all_trades,
            'summary': self._calculate_summary(all_trades)
        }
        
        return results

    def _pos_to_dict(self, pos: Position) -> Dict:
        return {
            'symbol': pos.symbol,
            'strike': pos.strike,
            'option_type': pos.option_type,
            'expiry': pos.expiry,
            'entry_time': pos.entry_time,
            'entry_price': pos.entry_price,
            'quantity': pos.quantity,
            'lots': pos.lots,
            'sl_price': pos.sl_price,
            'sl2_price': pos.sl2_price,
            'is_hedge': pos.is_hedge,
            'exit_time': pos.exit_time,
            'exit_price': pos.exit_price,
            'exit_reason': pos.exit_reason,
            'pnl': pos.pnl
        }

    # FIX 2: Updated signature to accept all_trades
    def _calculate_summary(self, all_trades: List[Dict] = None) -> Dict:
        if not self.daily_stats:
            return {'error': 'No trading days'}
        
        total_pnl = sum(d['day_pnl'] for d in self.daily_stats)
        total_trades = sum(d['total_trades'] for d in self.daily_stats)
        winning_days = sum(1 for d in self.daily_stats if d['day_pnl'] > 0)
        losing_days = sum(1 for d in self.daily_stats if d['day_pnl'] < 0)
        
        # FIX 2: Use all_trades instead of self.closed_positions
        all_pnls = [t['pnl'] for t in all_trades] if all_trades else []
        winning_trades = len([t for t in all_trades if t['pnl'] > 0]) if all_trades else 0
        losing_trades = len([t for t in all_trades if t['pnl'] < 0]) if all_trades else 0
        
        return {
            'total_days': len(self.daily_stats),
            'profitable_days': winning_days,
            'losing_days': losing_days,
            'total_pnl': round(total_pnl, 2),
            'avg_daily_pnl': round(total_pnl / len(self.daily_stats), 2) if self.daily_stats else 0,
            'max_daily_profit': max(d['day_pnl'] for d in self.daily_stats) if self.daily_stats else 0,
            'max_daily_loss': min(d['day_pnl'] for d in self.daily_stats) if self.daily_stats else 0,
            'total_trades': total_trades,
            'avg_trades_per_day': round(total_trades / len(self.daily_stats), 1) if self.daily_stats else 0,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': round(winning_trades / len(all_trades) * 100, 1) if all_trades else 0,
            'avg_trade_pnl': round(sum(all_pnls) / len(all_pnls), 2) if all_pnls else 0,
            'max_trade_profit': round(max(all_pnls), 2) if all_pnls else 0,
            'max_trade_loss': round(min(all_pnls), 2) if all_pnls else 0
        }

    # -------------------------------------------------------------------------
    # SAVE RESULTS
    # -------------------------------------------------------------------------

    def save_results(self, results: Dict, output_dir: str = 'results'):
        os.makedirs(output_dir, exist_ok=True)
        
        # Save daily stats
        daily_df = pd.DataFrame(results['daily_stats'])
        daily_file = os.path.join(output_dir, f"{self.symbol}_daily_stats.csv")
        daily_df.to_csv(daily_file, index=False)
        self.logger.info(f"Daily stats saved to {daily_file}")
        
        # Save all trades
        trades_df = pd.DataFrame(results['trades'])
        trades_file = None
        if not trades_df.empty:
            trades_file = os.path.join(output_dir, f"{self.symbol}_trades.csv")
            trades_df.to_csv(trades_file, index=False)
            self.logger.info(f"Trades saved to {trades_file}")
        
        # Save trade log
        log_df = pd.DataFrame(self.trade_log)
        log_file = None
        if not log_df.empty:
            log_file = os.path.join(output_dir, f"{self.symbol}_log.csv")
            log_df.to_csv(log_file, index=False)
        
        # Save summary as JSON
        summary_file = os.path.join(output_dir, f"{self.symbol}_summary.json")
        with open(summary_file, 'w') as f:
            json.dump(results['summary'], f, indent=2, default=str)
        
        return {
            'daily_stats': daily_file,
            'trades': trades_file,
            'log': log_file,
            'summary': summary_file
        }


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def load_params(params_file: str = 'params.json') -> Dict:
    with open(params_file, 'r') as f:
        return json.load(f)


def create_symbol_params(global_params: Dict, symbol_config: Dict) -> Dict:
    """Merge global and symbol-specific parameters"""
    params = global_params.copy()
    params.update(symbol_config)
    return params