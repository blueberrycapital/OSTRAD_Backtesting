

import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
import warnings
from abc import ABC, abstractmethod
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.query_api import QueryApi

warnings.filterwarnings('ignore')


class DataSourceType(Enum):
    SINGLE_FILE = "single_file"           # Spot + options in one file
    SEPARATE_FILES = "separate_files"     # Spot and options separate
    DATAFRAME = "dataframe"               # DataFrame directly
    INFLUXDB = "influxdb"                 # InfluxDB query


@dataclass
class MarginConfig:
    """Configuration for margin calculation"""
    var_margin_percent: float = 12.0        # VaR margin (SPAN standard)
    elm_percent: float = 3.0                # Extreme Loss Margin
    span_multiplier: float = 1.0              # Additional SPAN buffer
    min_margin_per_lot: float = 50000       # Minimum margin per lot (broker specific)
    
    def calculate_span_margin(self, spot_price: float, lot_size: int, 
                             option_price: float, quantity: int,
                             is_short: bool = True) -> float:
        """
        SPAN-like margin calculation for short options
        
        Formula: (VaR + ELM) × Quantity - Premium Received (for shorts)
        """
        # Base margin components
        base_margin = spot_price * lot_size * (self.var_margin_percent / 100)
        elm_margin = spot_price * lot_size * (self.elm_percent / 100)
        
        total_base = (base_margin + elm_margin) * abs(quantity) / lot_size
        total_base *= self.span_multiplier
        
        # For short options, subtract premium received (hedge benefit)
        if is_short:
            premium_received = option_price * abs(quantity)
            margin = max(total_base - premium_received, 
                        self.min_margin_per_lot * abs(quantity) / lot_size)
        else:
            # For long options, margin is just the premium paid
            margin = option_price * abs(quantity)
            
        return max(margin, 0)


@dataclass
class StrategyParams:
    """OSTRAD Strategy Parameters"""
    # Time parameters
    time_frame: int = 5                     # Candle timeframe in minutes
    start_time: str = "09:20:00"
    end_time: str = "15:15:00"
    square_off_time: str = "15:15:00"
    
    # Entry parameters
    threshold_percentage: float = 10.0      # VWAP deviation threshold
    sl_percentage: float = 20.0             # Stop loss percentage
    sl_diff_percentage: float = 5.0         # SL limit price buffer
    
    # Position limits
    max_open_strikes_per_leg: int = 3
    num_strikes: int = 2                    # Number of strikes around ATM
    capital_per_symbol: float = 10_000_000  # 1 Crore per symbol
    
    # Margin limits
    new_position_margin_limit: float = 70.0  # Max margin % for new positions
    hedge_margin_limit: float = 80.0         # Max margin % before hedging
    
    # Hedge parameters
    hedge_strike_diff_percent: float = 2.5   # OTM % for first hedge
    hedge_strike_diff_percent_2: float = 5.0 # OTM % for subsequent hedges
    max_hedge_multiplier: int = 3           # Max hedge quantity multiplier
    
    # Risk parameters
    min_premium_percent: float = 0.055      # Min premium as % of spot
    
    # Data parameters
    symbols: List[str] = field(default_factory=lambda: ['NIFTY', 'BANKNIFTY'])
    active_days_to_expiry: List[int] = field(default_factory=lambda: [0, 1])
    
    # Margin config
    margin_config: MarginConfig = field(default_factory=MarginConfig)


class DataLoader(ABC):
    """Abstract base class for data loading"""
    
    @abstractmethod
    def load_data(self, symbol: str, start_date: datetime, 
                  end_date: datetime) -> Dict[str, pd.DataFrame]:
        """Load data and return dict with 'spot' and options DataFrames"""
        pass


class FileDataLoader(DataLoader):
    """Load data from CSV/Parquet files"""
    
    def __init__(self, file_path: str, file_format: str = 'csv',
                 spot_file_path: Optional[str] = None,
                 date_column: str = 'date',
                 spot_column_map: Optional[Dict] = None,
                 option_column_map: Optional[Dict] = None):
        self.file_path = file_path
        self.spot_file_path = spot_file_path
        self.file_format = file_format
        self.date_column = date_column
        self.spot_column_map = spot_column_map or {
            'date': 'date', 'open': 'open', 'high': 'high',
            'low': 'low', 'close': 'close', 'volume': 'volume'
        }
        self.option_column_map = option_column_map or {
            'date': 'date', 'symbol': 'symbol', 'strike': 'strike',
            'option_type': 'option_type', 'expiry': 'expiry',
            'open': 'open', 'high': 'high', 'low': 'low',
            'close': 'close', 'volume': 'volume'
        }
    
    def load_data(self, symbol: str, start_date: datetime, 
                  end_date: datetime) -> Dict[str, pd.DataFrame]:
        """Load from single or separate files"""
        
        # Load spot data
        if self.spot_file_path:
            spot_df = self._read_file(self.spot_file_path)
        else:
            # Single file - filter spot
            full_df = self._read_file(self.file_path)
            spot_df = full_df[full_df['symbol'] == symbol].copy()
        
        spot_df = self._standardize_spot(spot_df)
        spot_df = spot_df[(spot_df.index >= start_date) & 
                         (spot_df.index <= end_date)]
        
        # Load options data
        options_df = self._read_file(self.file_path)
        options_df = self._standardize_options(options_df)
        if 'symbol' in options_df.columns:
            options_df = options_df[options_df['symbol'] == symbol]
        options_df = options_df[(options_df.index >= start_date) &
                                (options_df.index <= end_date)]
        
        return {'spot': spot_df, 'options': options_df}
    
    def _read_file(self, path: str) -> pd.DataFrame:
        """Read CSV or Parquet"""
        if self.file_format == 'csv':
            return pd.read_csv(path, parse_dates=[self.date_column])
        elif self.file_format == 'parquet':
            return pd.read_parquet(path)
        else:
            raise ValueError(f"Unsupported format: {self.file_format}")

    def _standardize_spot(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize spot column names"""
        df = df.rename(columns=self.spot_column_map)
        df.set_index(self.date_column, inplace=True)
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        return df[['open', 'high', 'low', 'close', 'volume']]

    def _standardize_options(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize options column names"""
        df = df.rename(columns=self.option_column_map)
        df.set_index(self.date_column, inplace=True)
        if df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        if 'expiry' in df.columns:
            df['expiry'] = pd.to_datetime(df['expiry'])
        return df


class DataFrameLoader(DataLoader):
    """Use DataFrames directly"""
    
    def __init__(self, spot_df: pd.DataFrame, options_df: pd.DataFrame):
        self.spot_df = spot_df.copy()
        self.options_df = options_df.copy()
    
    def load_data(self, symbol: str, start_date: datetime, 
                  end_date: datetime) -> Dict[str, pd.DataFrame]:
        """Filter and return provided DataFrames"""
        spot = self.spot_df[(self.spot_df.index >= start_date) & 
                           (self.spot_df.index <= end_date)].copy()
        options = self.options_df[(self.options_df.index >= start_date) & 
                                  (self.options_df.index <= end_date)].copy()
        
        # Filter by symbol if column exists
        if 'symbol' in options.columns:
            options = options[options['symbol'] == symbol]
        
        return {'spot': spot, 'options': options}


class InfluxDBLoader(DataLoader):
    """Query data from InfluxDB (zerodha-pipeline schema)

    Measurements:
      - Spot  : fut_spot_merged  (tag data_type="SPOT", tag index=<symbol>)
      - Options: options_1min    (tag index=<symbol>, tags option_type/strike/expiry)
    Fields: open, high, low, close, volume, oi  (all float)
    """

    def __init__(self, url: str, token: str, org: str, bucket: str,
                 spot_measurement: str = "fut_spot_merged",
                 option_measurement: str = "options_1min"):
        self.client = influxdb_client.InfluxDBClient(url=url, token=token, org=org)
        self.bucket = bucket
        self.spot_measurement = spot_measurement
        self.option_measurement = option_measurement
        self.query_api = self.client.query_api()

    def load_data(self, symbol: str, start_date: datetime,
                  end_date: datetime) -> Dict[str, pd.DataFrame]:
        """Query InfluxDB for spot and options data"""

        # InfluxDB range is RFC3339 UTC; include the full end day
        start_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
        stop_str  = (end_date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")

        # ── Spot ──────────────────────────────────────────────────
        spot_query = f'''
from(bucket: "{self.bucket}")
    |> range(start: {start_str}, stop: {stop_str})
    |> filter(fn: (r) => r._measurement == "{self.spot_measurement}")
    |> filter(fn: (r) => r.data_type == "SPOT")
    |> filter(fn: (r) => r.index == "{symbol}")
    |> filter(fn: (r) => r._field == "open" or r._field == "high" or
                         r._field == "low"  or r._field == "close" or
                         r._field == "volume")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
        spot_df = self._query_to_df(spot_query)
        if not spot_df.empty:
            spot_df = self._set_time_index(spot_df)
            for col in ["open", "high", "low", "close", "volume"]:
                if col not in spot_df.columns:
                    spot_df[col] = 0.0
            spot_df = spot_df[["open", "high", "low", "close", "volume"]]

        # ── Options ───────────────────────────────────────────────
        option_query = f'''
from(bucket: "{self.bucket}")
    |> range(start: {start_str}, stop: {stop_str})
    |> filter(fn: (r) => r._measurement == "{self.option_measurement}")
    |> filter(fn: (r) => r.index == "{symbol}")
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
        options_df = self._query_to_df(option_query)
        if not options_df.empty:
            options_df = self._set_time_index(options_df)
            options_df = options_df.rename(columns={
                "trading_symbol": "symbol",
                "oi":             "open_interest",
            })
            if "strike" in options_df.columns:
                options_df["strike"] = pd.to_numeric(options_df["strike"], errors="coerce")
            # Pipeline writes both "expiry" and "expiry_date" tags; prefer "expiry"
            if "expiry" not in options_df.columns and "expiry_date" in options_df.columns:
                options_df = options_df.rename(columns={"expiry_date": "expiry"})
            if "expiry" in options_df.columns:
                options_df["expiry"] = pd.to_datetime(options_df["expiry"])

        return {"spot": spot_df, "options": options_df}

    def _query_to_df(self, query: str) -> pd.DataFrame:
        """Run a Flux query and always return a single DataFrame"""
        result = self.query_api.query_data_frame(query)
        if isinstance(result, list):
            return pd.concat(result, ignore_index=True) if result else pd.DataFrame()
        return result if result is not None else pd.DataFrame()

    @staticmethod
    def _set_time_index(df: pd.DataFrame) -> pd.DataFrame:
        """Move _time to the index, stripped of timezone info"""
        df = df.rename(columns={"_time": "date_time"})
        df["date_time"] = pd.to_datetime(df["date_time"]).dt.tz_localize(None)
        df.set_index("date_time", inplace=True)
        return df


class VWAPCalculator:
    """Calculate VWAP indicator (bbc8 equivalent)"""
    
    @staticmethod
    def calculate(df: pd.DataFrame, buy_threshold: float = 10.0, 
                  sell_threshold: float = 10.0) -> pd.DataFrame:
        """
        Calculate VWAP and trading signals
        
        Args:
            df: DataFrame with 'close' and 'volume' columns
            buy_threshold: % deviation threshold for buy zone
            sell_threshold: % deviation threshold for sell zone
        """
        df = df.copy()
        
        # Calculate VWAP
        df['traded_volume'] = df['close'] * df['volume']
        df['cum_traded_volume'] = df['traded_volume'].cumsum()
        df['cum_volume'] = df['volume'].cumsum()
        df['vwap'] = df['cum_traded_volume'] / df['cum_volume']
        
        # Calculate deviation
        df['diff'] = df['close'] - df['vwap']
        df['perc_diff'] = abs(df['diff'] / df['vwap']) * 100
        
        # Determine side and tradability
        df['order_side'] = df.apply(
            lambda x: 'SELL' if x['close'] > x['vwap'] else 'BUY', axis=1
        )
        df['can_trade'] = df.apply(
            lambda x: (x['order_side'] == 'SELL' and x['perc_diff'] <= sell_threshold) or
                     (x['order_side'] == 'BUY' and x['perc_diff'] <= buy_threshold),
            axis=1
        )
        df['signal'] = df.apply(
            lambda x: x['order_side'] if x['can_trade'] else None, axis=1
        )
        
        return df


@dataclass
class Position:
    """Represents a single position"""
    symbol: str
    strike: float
    option_type: str  # 'CE' or 'PE'
    expiry: datetime
    entry_time: datetime
    entry_price: float
    quantity: int     # Positive for long, negative for short
    sl_price: Optional[float] = None
    sl2_price: Optional[float] = None  # Modified SL
    hedge_quantity: int = 0
    exit_time: Optional[datetime] = None
    exit_price: Optional[float] = None
    exit_reason: Optional[str] = None
    pnl: float = 0.0
    
    @property
    def is_short(self) -> bool:
        return self.quantity < 0
    
    @property
    def is_hedged(self) -> bool:
        return self.hedge_quantity > 0


class OSTRADBacktester:
    """
    Robust OSTRAD Strategy Backtester
    
    Features:
    - Multiple data source support (File, DataFrame, InfluxDB)
    - Realistic SPAN-like margin calculation
    - Hedge position management
    - Dynamic stop-loss modification
    - Comprehensive P&L tracking
    """
    
    def __init__(self, params: StrategyParams, data_loader: DataLoader):
        self.params = params
        self.data_loader = data_loader
        self.margin_config = params.margin_config
        
        # State tracking
        self.positions: Dict[str, List[Position]] = {s: [] for s in params.symbols}
        self.closed_positions: List[Position] = []
        self.margin_history: List[Dict] = []
        self.daily_pnl: Dict[datetime, float] = {}
        
        # Market data cache
        self.spot_data: Dict[str, pd.DataFrame] = {}
        self.options_data: Dict[str, pd.DataFrame] = {}
        self.straddle_data: Dict[str, pd.DataFrame] = {}
        
    def load_market_data(self, symbol: str, start_date: datetime, 
                        end_date: datetime) -> None:
        """Load and prepare market data"""
        data = self.data_loader.load_data(symbol, start_date, end_date)
        
        self.spot_data[symbol] = data['spot']
        options_df = data['options']
        
        # Preprocess options data - create pivot by strike and option type
        self.options_data[symbol] = options_df
        
        # Calculate straddle (ATM CE + PE) VWAP for signal generation
        self._prepare_straddle_data(symbol)
    
    def _prepare_straddle_data(self, symbol: str) -> None:
        """Prepare straddle VWAP data for each strike"""
        spot_df = self.spot_data[symbol]
        options_df = self.options_data[symbol]
        
        # Get unique strikes and expiries
        strikes = options_df['strike'].unique()
        expiries = options_df['expiry'].unique()
        
        straddle_data = {}
        
        for expiry in expiries:
            expiry_options = options_df[options_df['expiry'] == expiry]
            
            for strike in strikes:
                ce_data = expiry_options[
                    (expiry_options['strike'] == strike) & 
                    (expiry_options['option_type'] == 'CE')
                ].copy()
                pe_data = expiry_options[
                    (expiry_options['strike'] == strike) & 
                    (expiry_options['option_type'] == 'PE')
                ].copy()
                
                if ce_data.empty or pe_data.empty:
                    continue
                
                # Align timestamps
                merged = pd.merge(
                    ce_data[['close', 'volume']].rename(
                        columns={'close': 'ce_close', 'volume': 'ce_volume'}
                    ),
                    pe_data[['close', 'volume']].rename(
                        columns={'close': 'pe_close', 'volume': 'pe_volume'}
                    ),
                    left_index=True, right_index=True, how='inner'
                )
                
                if merged.empty:
                    continue
                
                # Calculate straddle price and combined volume
                merged['close'] = merged['ce_close'] + merged['pe_close']
                merged['volume'] = merged[['ce_volume', 'pe_volume']].min(axis=1)
                
                # Calculate VWAP indicator
                vwap_input = merged[['close', 'volume']].reset_index()
                vwap_input.rename(columns={vwap_input.columns[0]: 'date'}, inplace=True)
                straddle_vwap = VWAPCalculator.calculate(
                    vwap_input,
                    buy_threshold=self.params.threshold_percentage,
                    sell_threshold=self.params.threshold_percentage
                )
                straddle_vwap.set_index('date', inplace=True)
                
                key = f"{expiry}_{strike}"
                straddle_data[key] = straddle_vwap
        
        self.straddle_data[symbol] = straddle_data
    
    def get_atm_strike(self, symbol: str, timestamp: datetime) -> float:
        """Calculate ATM strike based on spot price"""
        spot_price = self.spot_data[symbol].loc[
            self.spot_data[symbol].index <= timestamp
        ]['close'].iloc[-1]
        
        # Get strike diff from available strikes
        available_strikes = self._get_available_strikes(symbol, timestamp)
        if not available_strikes:
            return round(spot_price / 50) * 50  # Default 50-point step
        
        strike_diff = min([abs(available_strikes[i] - available_strikes[i-1]) 
                          for i in range(1, len(available_strikes))])
        
        atm = round(spot_price / strike_diff) * strike_diff
        return atm
    
    def _get_available_strikes(self, symbol: str, timestamp: datetime) -> List[float]:
        """Get available strikes for nearest expiry"""
        options_df = self.options_data[symbol]
        current_expiries = options_df[options_df.index >= timestamp]['expiry'].unique()
        if len(current_expiries) == 0:
            return []
        
        nearest_expiry = min(current_expiries)
        strikes = options_df[options_df['expiry'] == nearest_expiry]['strike'].unique()
        return sorted(strikes)
    
    def calculate_margin(self, symbol: str, timestamp: datetime) -> Dict[str, float]:
        """
        Calculate total margin requirement using SPAN-like method
        
        Returns margin breakdown by position and total
        """
        total_margin = 0.0
        margin_breakdown = {}
        
        for pos in self.positions[symbol]:
            # Get current option price
            opt_price = self._get_option_price(
                symbol, pos.strike, pos.option_type, 
                pos.expiry, timestamp
            )
            
            # Get spot for margin calc
            spot = self.spot_data[symbol].loc[
                self.spot_data[symbol].index <= timestamp
            ]['close'].iloc[-1]
            
            # Calculate margin for this position
            margin = self.margin_config.calculate_span_margin(
                spot_price=spot,
                lot_size=abs(pos.quantity),  # Assuming quantity = lot_size for 1 lot
                option_price=opt_price,
                quantity=pos.quantity,
                is_short=pos.is_short
            )
            
            margin_breakdown[f"{pos.strike}_{pos.option_type}"] = margin
            total_margin += margin
        
        return {
            'total_margin': total_margin,
            'margin_used_percent': (total_margin / self.params.capital_per_symbol) * 100,
            'breakdown': margin_breakdown
        }
    
    def _get_option_price(self, symbol: str, strike: float, 
                         option_type: str, expiry: datetime, 
                         timestamp: datetime) -> float:
        """Get option price at timestamp"""
        options_df = self.options_data[symbol]
        price_data = options_df[
            (options_df['strike'] == strike) &
            (options_df['option_type'] == option_type) &
            (options_df['expiry'] == expiry) &
            (options_df.index <= timestamp)
        ]
        
        if price_data.empty:
            return 0.0
        
        return price_data['close'].iloc[-1]
    
    def check_signals(self, symbol: str, timestamp: datetime) -> List[Dict]:
        """
        Check for entry signals based on straddle VWAP deviation
        Returns list of potential entry signals
        """
        signals = []
        
        atm = self.get_atm_strike(symbol, timestamp)
        available_strikes = self._get_available_strikes(symbol, timestamp)
        
        # Find ATM index and select surrounding strikes
        atm_idx = available_strikes.index(min(available_strikes, 
                                              key=lambda x: abs(x - atm)))
        
        start_idx = max(0, atm_idx - self.params.num_strikes)
        end_idx = min(len(available_strikes), atm_idx + self.params.num_strikes + 1)
        selected_strikes = available_strikes[start_idx:end_idx]
        
        # Get nearest expiry
        options_df = self.options_data[symbol]
        current_expiries = options_df[options_df.index >= timestamp]['expiry'].unique()
        if len(current_expiries) == 0:
            return signals
        
        nearest_expiry = min(current_expiries)
        
        for strike in selected_strikes:
            key = f"{nearest_expiry}_{strike}"
            if key not in self.straddle_data[symbol]:
                continue
            
            straddle_df = self.straddle_data[symbol][key]
            current_data = straddle_df[straddle_df.index <= timestamp]
            
            if len(current_data) < 2:
                continue
            
            # Check previous candle signal (like original logic)
            prev_candle = current_data.iloc[-2]
            
            if prev_candle['signal'] == 'SELL' and prev_candle['can_trade']:
                # Check minimum premium condition
                spot = self.spot_data[symbol].loc[
                    self.spot_data[symbol].index <= timestamp
                ]['close'].iloc[-1]
                
                for opt_type in ['CE', 'PE']:
                    opt_price = self._get_option_price(
                        symbol, strike, opt_type, nearest_expiry, timestamp
                    )
                    min_premium = spot * (self.params.min_premium_percent / 100)
                    
                    if opt_price > min_premium:
                        signals.append({
                            'strike': strike,
                            'option_type': opt_type,
                            'expiry': nearest_expiry,
                            'signal_price': prev_candle['close'],
                            'vwap': prev_candle['vwap'],
                            'deviation': prev_candle['perc_diff'],
                            'entry_time': timestamp
                        })
        
        return signals
    
    def take_position(self, symbol: str, signal: Dict, timestamp: datetime,
                     quantity: int) -> Optional[Position]:
        """Execute a new position"""
        # Check margin limits
        margin_info = self.calculate_margin(symbol, timestamp)
        if margin_info['margin_used_percent'] >= self.params.new_position_margin_limit:
            return None
        
        # Check max strikes per leg
        current_ce = len([p for p in self.positions[symbol] 
                         if p.option_type == 'CE' and not p.exit_time])
        current_pe = len([p for p in self.positions[symbol] 
                         if p.option_type == 'PE' and not p.exit_time])
        
        opt_type = signal['option_type']
        if opt_type == 'CE' and current_ce >= self.params.max_open_strikes_per_leg:
            return None
        if opt_type == 'PE' and current_pe >= self.params.max_open_strikes_per_leg:
            return None
        
        # Get entry price
        entry_price = self._get_option_price(
            symbol, signal['strike'], opt_type, 
            signal['expiry'], timestamp
        )
        
        # Calculate SL
        sl_price = entry_price * (1 + self.params.sl_percentage / 100)
        
        position = Position(
            symbol=symbol,
            strike=signal['strike'],
            option_type=opt_type,
            expiry=signal['expiry'],
            entry_time=timestamp,
            entry_price=entry_price,
            quantity=-quantity,  # Short position
            sl_price=sl_price
        )
        
        self.positions[symbol].append(position)
        return position
    
    def take_hedge(self, symbol: str, timestamp: datetime) -> List[Position]:
        """
        Take hedge positions when margin exceeds limit
        Returns list of hedge positions created
        """
        hedge_positions = []
        margin_info = self.calculate_margin(symbol, timestamp)
        
        if margin_info['margin_used_percent'] < self.params.hedge_margin_limit:
            return hedge_positions
        
        spot = self.spot_data[symbol].loc[
            self.spot_data[symbol].index <= timestamp
        ]['close'].iloc[-1]
        
        # Determine which side needs hedging
        ce_exposure = sum([abs(p.quantity) for p in self.positions[symbol] 
                          if p.option_type == 'CE' and not p.exit_time])
        pe_exposure = sum([abs(p.quantity) for p in self.positions[symbol] 
                          if p.option_type == 'PE' and not p.exit_time])
        
        # Get available strikes
        available_strikes = self._get_available_strikes(symbol, timestamp)
        if not available_strikes:
            return hedge_positions
        
        strike_diff = min([abs(available_strikes[i] - available_strikes[i-1]) 
                          for i in range(1, len(available_strikes))])
        
        # Determine hedge strikes (OTM by specified %)
        is_first_hedge = not any(p.hedge_quantity > 0 for p in self.positions[symbol])
        hedge_pct = (self.params.hedge_strike_diff_percent if is_first_hedge 
                    else self.params.hedge_strike_diff_percent_2)
        
        # Calculate hedge strikes
        call_hedge_strike = round(spot * (1 + hedge_pct / 100) / strike_diff) * strike_diff
        put_hedge_strike = round(spot * (1 - hedge_pct / 100) / strike_diff) * strike_diff
        
        # Get nearest expiry
        options_df = self.options_data[symbol]
        current_expiries = options_df[options_df.index >= timestamp]['expiry'].unique()
        nearest_expiry = min(current_expiries)
        
        # Take hedge positions
        for opt_type, strike, exposure in [('CE', call_hedge_strike, ce_exposure),
                                          ('PE', put_hedge_strike, pe_exposure)]:
            if exposure == 0:
                continue
            
            # Check max hedge limit
            current_hedge = sum([p.hedge_quantity for p in self.positions[symbol] 
                               if p.option_type == opt_type])
            max_hedge = exposure * self.params.max_hedge_multiplier
            
            if current_hedge >= max_hedge:
                continue
            
            hedge_qty = min(exposure // 2, max_hedge - current_hedge)
            if hedge_qty <= 0:
                continue
            
            entry_price = self._get_option_price(
                symbol, strike, opt_type, nearest_expiry, timestamp
            )
            
            hedge_pos = Position(
                symbol=symbol,
                strike=strike,
                option_type=opt_type,
                expiry=nearest_expiry,
                entry_time=timestamp,
                entry_price=entry_price,
                quantity=hedge_qty,  # Long position
                hedge_quantity=hedge_qty
            )
            
            self.positions[symbol].append(hedge_pos)
            hedge_positions.append(hedge_pos)
        
        return hedge_positions
    
    def modify_sl(self, symbol: str, timestamp: datetime) -> List[Position]:
        """
        Modify SL to day's high after specified time (SL2 logic)
        Returns list of positions with modified SL
        """
        modified = []
        
        # Check if modification time reached
        mod_time = datetime.combine(timestamp.date(), 
                                   datetime.strptime("13:00:00", "%H:%M:%S").time())
        if timestamp < mod_time:
            return modified
        
        for pos in self.positions[symbol]:
            if pos.exit_time or pos.sl2_price or not pos.is_short:
                continue
            
            # Get day's high for this option
            options_df = self.options_data[symbol]
            day_data = options_df[
                (options_df['strike'] == pos.strike) &
                (options_df['option_type'] == pos.option_type) &
                (options_df['expiry'] == pos.expiry) &
                (options_df.index.date == timestamp.date())
            ]
            
            if day_data.empty:
                continue
            
            day_high = day_data['high'].max()
            
            # Only modify if day's high is below current SL
            if day_high < pos.sl_price:
                pos.sl2_price = day_high
                pos.sl_price = day_high  # Update active SL
                modified.append(pos)
        
        return modified
    
    def check_sl_hit(self, symbol: str, timestamp: datetime) -> List[Position]:
        """Check and execute stop losses"""
        hit_positions = []
        
        for pos in self.positions[symbol]:
            if pos.exit_time or not pos.is_short:
                continue
            
            current_price = self._get_option_price(
                symbol, pos.strike, pos.option_type, 
                pos.expiry, timestamp
            )
            
            # Check if SL hit
            if current_price >= pos.sl_price:
                pos.exit_time = timestamp
                pos.exit_price = current_price
                pos.exit_reason = 'SL_HIT'
                pos.pnl = (pos.entry_price - pos.exit_price) * abs(pos.quantity)
                
                # Close associated hedge if exists
                self._close_hedge(symbol, pos, timestamp)
                
                hit_positions.append(pos)
                self.closed_positions.append(pos)
        
        return hit_positions
    
    def _close_hedge(self, symbol: str, position: Position, timestamp: datetime) -> None:
        """Close hedge position when main position hits SL"""
        for pos in self.positions[symbol]:
            if (pos.hedge_quantity > 0 and 
                pos.option_type == position.option_type and
                not pos.exit_time):
                
                exit_price = self._get_option_price(
                    symbol, pos.strike, pos.option_type, 
                    pos.expiry, timestamp
                )
                
                pos.exit_time = timestamp
                pos.exit_price = exit_price
                pos.exit_reason = 'HEDGE_CLOSE'
                pos.pnl = (pos.exit_price - pos.entry_price) * pos.quantity
                self.closed_positions.append(pos)
                break
    
    def square_off_all(self, symbol: str, timestamp: datetime) -> List[Position]:
        """Square off all positions at EOD"""
        squared_off = []
        
        for pos in self.positions[symbol]:
            if pos.exit_time:
                continue
            
            exit_price = self._get_option_price(
                symbol, pos.strike, pos.option_type, 
                pos.expiry, timestamp
            )
            
            pos.exit_time = timestamp
            pos.exit_price = exit_price
            pos.exit_reason = 'EOD_SQUAREOFF'
            
            if pos.is_short:
                pos.pnl = (pos.entry_price - pos.exit_price) * abs(pos.quantity)
            else:
                pos.pnl = (pos.exit_price - pos.entry_price) * pos.quantity
            
            squared_off.append(pos)
            self.closed_positions.append(pos)
        
        return squared_off
    
    def run_backtest(self, start_date: datetime, end_date: datetime) -> Dict:
        """
        Run complete backtest
        
        Returns comprehensive results dictionary
        """
        results = {
            'trades': [],
            'daily_pnl': {},
            'margin_history': [],
            'statistics': {}
        }
        
        for symbol in self.params.symbols:
            print(f"Loading data for {symbol}...")
            self.load_market_data(symbol, start_date, end_date)
            
            print(f"Running backtest for {symbol}...")
            symbol_results = self._run_symbol_backtest(symbol, start_date, end_date)
            results['trades'].extend(symbol_results['trades'])
            results['daily_pnl'].update(symbol_results['daily_pnl'])
            results['margin_history'].extend(symbol_results['margin_history'])
        
        # Calculate statistics
        results['statistics'] = self._calculate_statistics(results['trades'])
        
        return results
    
    def _run_symbol_backtest(self, symbol: str, start_date: datetime, 
                            end_date: datetime) -> Dict:
        """Run backtest for single symbol"""
        trades = []
        daily_pnl = {}
        margin_history = []
        
        # Generate trading timestamps
        current_date = start_date
        while current_date <= end_date:
            # Check if trading day
            if current_date.weekday() >= 5:  # Weekend
                current_date += timedelta(days=1)
                continue
            
            day_start = datetime.combine(current_date.date(),
                                      datetime.strptime(self.params.start_time, "%H:%M:%S").time())
            day_end = datetime.combine(current_date.date(),
                                      datetime.strptime(self.params.end_time, "%H:%M:%S").time())

            # Use actual spot data timestamps for this day
            timestamps = self.spot_data[symbol][
                (self.spot_data[symbol].index >= day_start) &
                (self.spot_data[symbol].index <= day_end)
            ].index

            if len(timestamps) == 0:
                current_date += timedelta(days=1)
                continue

            day_pnl = 0.0

            for timestamp in timestamps:
                
                # 1. Check and take hedge positions if needed
                hedges = self.take_hedge(symbol, timestamp)
                for hedge in hedges:
                    trades.append(self._position_to_dict(hedge, 'HEDGE_ENTRY'))
                
                # 2. Check for entry signals (only during entry window)
                entry_end = datetime.combine(current_date.date(),
                                            datetime.strptime("14:30:00", "%H:%M:%S").time())
                
                if timestamp <= entry_end:
                    signals = self.check_signals(symbol, timestamp)
                    for signal in signals:
                        # Calculate position size
                        margin_info = self.calculate_margin(symbol, timestamp)
                        available_cap = self.params.capital_per_symbol * \
                                       (self.params.new_position_margin_limit / 100) - \
                                       margin_info['total_margin']
                        
                        # Estimate margin per lot (rough estimate)
                        spot = self.spot_data[symbol].loc[
                            self.spot_data[symbol].index <= timestamp
                        ]['close'].iloc[-1]
                        est_margin_per_lot = spot * 0.15  # Rough 15% of spot
                        
                        if est_margin_per_lot > 0:
                            max_lots = int(available_cap / est_margin_per_lot)
                            lots = min(max_lots, 1)  # At least 1 lot if possible
                            
                            if lots > 0:
                                pos = self.take_position(symbol, signal, timestamp, lots)
                                if pos:
                                    trades.append(self._position_to_dict(pos, 'ENTRY'))
                
                # 3. Modify SL after 1 PM
                modified = self.modify_sl(symbol, timestamp)
                for mod in modified:
                    trades.append(self._position_to_dict(mod, 'SL_MODIFIED'))
                
                # 4. Check SL hits
                hits = self.check_sl_hit(symbol, timestamp)
                for hit in hits:
                    trades.append(self._position_to_dict(hit, 'SL_HIT'))
                    day_pnl += hit.pnl
                
                # 5. Record margin usage
                margin_info = self.calculate_margin(symbol, timestamp)
                margin_history.append({
                    'timestamp': timestamp,
                    'symbol': symbol,
                    'total_margin': margin_info['total_margin'],
                    'margin_percent': margin_info['margin_used_percent']
                })
            
            # Square off at EOD
            square_off_time = datetime.combine(current_date.date(),
                                              datetime.strptime(self.params.square_off_time, "%H:%M:%S").time())
            squared = self.square_off_all(symbol, square_off_time)
            for sq in squared:
                trades.append(self._position_to_dict(sq, 'SQUARE_OFF'))
                day_pnl += sq.pnl
            
            daily_pnl[current_date.date()] = day_pnl
            current_date += timedelta(days=1)
        
        return {
            'trades': trades,
            'daily_pnl': daily_pnl,
            'margin_history': margin_history
        }
    
    def _position_to_dict(self, pos: Position, event: str) -> Dict:
        """Convert position to dictionary for results"""
        return {
            'symbol': pos.symbol,
            'strike': pos.strike,
            'option_type': pos.option_type,
            'expiry': pos.expiry,
            'entry_time': pos.entry_time,
            'entry_price': pos.entry_price,
            'quantity': pos.quantity,
            'sl_price': pos.sl_price,
            'sl2_price': pos.sl2_price,
            'hedge_quantity': pos.hedge_quantity,
            'exit_time': pos.exit_time,
            'exit_price': pos.exit_price,
            'exit_reason': pos.exit_reason,
            'pnl': pos.pnl,
            'event': event
        }
    
    def _calculate_statistics(self, trades: List[Dict]) -> Dict:
        """Calculate comprehensive statistics"""
        df = pd.DataFrame(trades)
        
        if df.empty:
            return {}
        
        # Filter completed trades
        completed = df[df['exit_time'].notna()].copy()
        
        if completed.empty:
            return {}
        
        stats = {
            'total_trades': len(completed),
            'total_pnl': completed['pnl'].sum(),
            'winning_trades': len(completed[completed['pnl'] > 0]),
            'losing_trades': len(completed[completed['pnl'] < 0]),
            'avg_pnl_per_trade': completed['pnl'].mean(),
            'max_profit': completed['pnl'].max(),
            'max_loss': completed['pnl'].min(),
            'profit_factor': abs(completed[completed['pnl'] > 0]['pnl'].sum() / 
                                completed[completed['pnl'] < 0]['pnl'].sum()) \
                           if completed[completed['pnl'] < 0]['pnl'].sum() != 0 else float('inf'),
            'sl_hit_rate': len(completed[completed['exit_reason'] == 'SL_HIT']) / len(completed) * 100,
            'eod_squareoff_rate': len(completed[completed['exit_reason'] == 'EOD_SQUAREOFF']) / len(completed) * 100
        }
        
        # Calculate Sharpe-like metric (simplified)
        daily_returns = pd.Series(self.daily_pnl)
        if len(daily_returns) > 1:
            stats['daily_volatility'] = daily_returns.std()
            stats['sharpe_ratio'] = (daily_returns.mean() / daily_returns.std()) \
                                   if daily_returns.std() != 0 else 0
        
        return stats


if __name__ == "__main__":
    import argparse
    import os
    from dotenv import load_dotenv
    load_dotenv()

    parser = argparse.ArgumentParser(description="OSTRAD Backtester")
    parser.add_argument("--start",  required=True, metavar="YYYY-MM-DD", help="Backtest start date")
    parser.add_argument("--end",    required=True, metavar="YYYY-MM-DD", help="Backtest end date")
    parser.add_argument("--source", default="influx", choices=["csv", "influx"], help="Data source (default: influx)")

    # CSV arguments
    parser.add_argument("--spot",    metavar="PATH", help="Spot CSV file path (csv source)")
    parser.add_argument("--options", metavar="PATH", help="Options CSV file path (csv source)")

    # InfluxDB arguments — fall back to .env values
    parser.add_argument("--url",      default=os.getenv("INFLUX_URL"),    metavar="URL",    help="InfluxDB URL")
    parser.add_argument("--token",    default=os.getenv("INFLUX_TOKEN"),  metavar="TOKEN",  help="InfluxDB token")
    parser.add_argument("--org",      default=os.getenv("INFLUX_ORG"),    metavar="ORG",    help="InfluxDB org")
    parser.add_argument("--bucket",   default=os.getenv("INFLUX_BUCKET"), metavar="BUCKET", help="InfluxDB bucket")
    parser.add_argument("--spot-meas",   default="fut_spot_merged", metavar="NAME", help="Spot measurement name (default: fut_spot_merged)")
    parser.add_argument("--opt-meas",    default="options_1min",    metavar="NAME", help="Option measurement name (default: options_1min)")

    args = parser.parse_args()

    # --- Validate dates ---
    try:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
    except ValueError:
        parser.error(f"Invalid --start date '{args.start}'. Use YYYY-MM-DD.")

    try:
        end_date = datetime.strptime(args.end, "%Y-%m-%d")
    except ValueError:
        parser.error(f"Invalid --end date '{args.end}'. Use YYYY-MM-DD.")

    if end_date < start_date:
        parser.error("--end date must be on or after --start date.")

    # --- Build loader ---
    if args.source == "csv":
        if not args.spot or not args.options:
            parser.error("--spot and --options are required when --source=csv")
        loader = FileDataLoader(
            file_path=args.options,
            spot_file_path=args.spot,
            file_format='csv',
            date_column='date_time',
            spot_column_map={
                'open': 'open', 'high': 'high',
                'low': 'low', 'close': 'close', 'volume': 'volume'
            },
            option_column_map={
                'trading_symbol': 'symbol',
                'expiry_date': 'expiry',
                'strike': 'strike',
                'option_type': 'option_type',
                'open': 'open', 'high': 'high',
                'low': 'low', 'close': 'close', 'volume': 'volume'
            }
        )
    else:  # influx
        if not all([args.url, args.token, args.org, args.bucket]):
            parser.error("--url, --token, --org, and --bucket are required when --source=influx")
        loader = InfluxDBLoader(
            url=args.url, token=args.token, org=args.org, bucket=args.bucket,
            spot_measurement=args.spot_meas,
            option_measurement=args.opt_meas
        )

    print("=" * 60)
    print("OSTRAD BACKTESTER")
    print("=" * 60)

    # --- Configure and run ---
    params = StrategyParams(
        symbols=['NIFTY'],
        capital_per_symbol=10_000_000,
        threshold_percentage=10.0,
        sl_percentage=20.0,
        margin_config=MarginConfig(
            var_margin_percent=12.0,
            elm_percent=3.0
        )
    )

    backtester = OSTRADBacktester(params, loader)
    results = backtester.run_backtest(start_date=start_date, end_date=end_date)

    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    stats = results['statistics']
    if not stats:
        print("\nNo trades were generated. Check data loading and strategy parameters.")
        print(f"Total trade events recorded: {len(results['trades'])}")
    else:
        print(f"\nTotal Trades:  {stats['total_trades']}")
        print(f"Total P&L:     ₹{stats['total_pnl']:,.2f}")
        print(f"Win Rate:      {stats['winning_trades'] / max(stats['total_trades'], 1) * 100:.1f}%")
        print(f"Profit Factor: {stats['profit_factor']:.2f}")
        print(f"SL Hit Rate:   {stats['sl_hit_rate']:.1f}%")
        print(f"Max Profit:    ₹{stats['max_profit']:,.2f}")
        print(f"Max Loss:      ₹{stats['max_loss']:,.2f}")
