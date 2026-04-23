# import numpy as np
# import pandas as pd
# from datetime import datetime, timedelta
# from typing import Dict, List, Optional
# from dataclasses import dataclass, field
# from abc import ABC, abstractmethod
# import warnings
# warnings.filterwarnings('ignore')


# # =============================================================================
# # CONFIGURATION CLASSES
# # =============================================================================

# @dataclass
# class MarginConfig:
#     """Configuration for margin calculation"""
#     var_margin_percent: float = 12.0
#     elm_percent: float = 3.0
#     span_multiplier: float = 1.0
#     min_margin_per_lot: float = 50000

#     def calculate_span_margin(self, spot_price: float, lot_size: int,
#                               option_price: float, quantity: int,
#                               is_short: bool = True) -> float:
#         if is_short:
#             base_margin = spot_price * lot_size * (self.var_margin_percent / 100)
#             elm_margin = spot_price * lot_size * (self.elm_percent / 100)
#             total_base = (base_margin + elm_margin) * abs(quantity) / lot_size
#             total_base *= self.span_multiplier
#             premium_received = option_price * abs(quantity)
#             margin = max(total_base - premium_received,
#                          self.min_margin_per_lot * abs(quantity) / lot_size)
#         else:
#             margin = option_price * abs(quantity)
#         return max(margin, 0)


# @dataclass
# class StrategyParams:
#     """OSTRAD Strategy Parameters - Matching Live Code"""
#     # Timing
#     time_frame: int = 5  # minutes
#     start_time: str = "09:25:00"
#     entry_end_time: str = "14:30:00"
#     sl_modification_time: str = "10:00:00"
#     square_off_time: str = "15:20:00"
    
#     # Strategy
#     threshold_percentage: float = 10.0  # VWAP deviation threshold
#     sl_percentage: Dict[str, float] = field(default_factory=lambda: {'NIFTY': 22.0})
#     sl_diff_percentage: float = 2.0  # Limit price buffer above SL
#     max_open_strikes_per_leg: int = 3
#     num_strikes: Dict[str, int] = field(default_factory=lambda: {'NIFTY': 2})
#     min_premium_percent: float = 0.055  # Minimum premium filter
    
#     # Capital & Margin
#     capital_per_symbol: float = 10_000_000
#     new_position_margin_limit: float = 75.0
#     hedge_margin_limit: float = 60.0
#     hedge_strike_diff_percent: float = 2.5  # First hedge
#     hedge_strike_diff_percent_2: float = 5.0  # Subsequent hedges
    
#     # Symbols
#     symbols: List[str] = field(default_factory=lambda: ['NIFTY'])
#     active_days_to_expiry: List[int] = field(default_factory=lambda: [0, 1, 2])
#     lot_sizes: Dict[str, int] = field(default_factory=lambda: {'NIFTY': 75})
#     strike_diffs: Dict[str, float] = field(default_factory=lambda: {'NIFTY': 50.0})
    
#     # Quantity per strike (replaces lots_per_strike calculation)
#     quantity_per_strike: Dict[str, int] = field(default_factory=lambda: {'NIFTY': 800})
    
#     margin_config: MarginConfig = field(default_factory=MarginConfig)


# # =============================================================================
# # DATA LOADER
# # =============================================================================

# class DataLoader(ABC):
#     @abstractmethod
#     def load_data(self, symbol: str, start_date: datetime,
#                   end_date: datetime) -> Dict[str, pd.DataFrame]:
#         pass


# class DataFrameLoader(DataLoader):
#     """Load data from provided DataFrames"""

#     def __init__(self, spot_df: pd.DataFrame, options_df: pd.DataFrame):
#         self.spot_df = spot_df.copy()
#         self.options_df = options_df.copy()

#         # Ensure datetime index
#         if not isinstance(self.spot_df.index, pd.DatetimeIndex):
#             if 'date' in self.spot_df.columns:
#                 self.spot_df.set_index('date', inplace=True)

#         if not isinstance(self.options_df.index, pd.DatetimeIndex):
#             if 'date' in self.options_df.columns:
#                 self.options_df.set_index('date', inplace=True)

#         # Strip timezone info
#         if self.spot_df.index.tz is not None:
#             self.spot_df.index = self.spot_df.index.tz_localize(None)

#         if self.options_df.index.tz is not None:
#             self.options_df.index = self.options_df.index.tz_localize(None)

#         if 'expiry' in self.options_df.columns:
#             self.options_df['expiry'] = pd.to_datetime(
#                 self.options_df['expiry']).dt.tz_localize(None)

#     def load_data(self, symbol: str, start_date: datetime,
#                   end_date: datetime) -> Dict[str, pd.DataFrame]:

#         start_ts = pd.Timestamp(start_date)
#         if start_ts.tz is not None:
#             start_ts = start_ts.tz_localize(None)
            
#         end_ts = pd.Timestamp(end_date)
#         if end_ts.tz is not None:
#             end_ts = end_ts.tz_localize(None)

#         # Extend end_ts to end of day
#         end_ts_day = end_ts.normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)

#         spot = self.spot_df[
#             (self.spot_df.index >= start_ts) &
#             (self.spot_df.index <= end_ts_day)
#         ].copy()

#         options = self.options_df[
#             (self.options_df.index >= start_ts) &
#             (self.options_df.index <= end_ts_day)
#         ].copy()

#         if 'symbol' in options.columns:
#             options = options[options['symbol'] == symbol]

#         return {'spot': spot, 'options': options}


# # =============================================================================
# # VWAP INDICATOR - Matches bbc8 from live code
# # =============================================================================

# class VWAPIndicator:
#     @staticmethod
#     def calculate(df: pd.DataFrame, buy_threshold: float = 10.0,
#                   sell_threshold: float = 10.0) -> pd.DataFrame:
#         """
#         Calculate VWAP indicator - matches bbc8 from live code
#         """
#         _temp = df.copy()
#         _temp['traded_volume'] = _temp['close'] * _temp['volume']
#         _temp['indicator'] = _temp['traded_volume'].cumsum() / _temp['volume'].cumsum()
#         _temp['diff'] = _temp['close'] - _temp['indicator']
#         _temp['order_side'] = _temp.apply(
#             lambda x: 'buy' if x['diff'] > 0 else 'sell', axis=1)
#         _temp['perc_diff'] = round(abs(_temp['diff'] / _temp['indicator']) * 100, 2)
#         _temp['can_trade'] = _temp.apply(
#             lambda x: True if (
#                 (x['order_side'] == 'buy' and x['perc_diff'] <= buy_threshold) or
#                 (x['order_side'] == 'sell' and x['perc_diff'] <= sell_threshold)
#             ) else False, axis=1)
#         _temp['signal'] = _temp.apply(
#             lambda x: x['order_side'] if x['can_trade'] else np.nan, axis=1)
#         return _temp


# # =============================================================================
# # POSITION CLASS
# # =============================================================================

# @dataclass
# class Position:
#     symbol: str
#     strike: float
#     option_type: str  # 'CE' or 'PE'
#     expiry: datetime
#     entry_time: datetime
#     entry_price: float
#     quantity: int  # Negative for short, positive for long
#     lot_size: int
#     sl_price: Optional[float] = None
#     sl_limit_price: Optional[float] = None  # Limit price for SL order
#     sl2_price: Optional[float] = None
#     sl_order_placed: bool = False
#     sl_hit: bool = False
#     is_hedge: bool = False
#     exit_time: Optional[datetime] = None
#     exit_price: Optional[float] = None
#     exit_reason: Optional[str] = None
#     pnl: float = 0.0

#     @property
#     def is_short(self) -> bool:
#         return self.quantity < 0

#     @property
#     def is_open(self) -> bool:
#         return self.exit_time is None

#     @property
#     def lots(self) -> int:
#         return abs(self.quantity) // self.lot_size
    
#     @property
#     def position_key(self) -> str:
#         """Unique key for this position"""
#         return f"{self.strike}_{self.option_type}_{self.expiry.date()}"


# # =============================================================================
# # BACKTESTER CLASS
# # =============================================================================

# class OSTRADBacktester:
#     def __init__(self, params: StrategyParams, data_loader: DataLoader):
#         self.params = params
#         self.data_loader = data_loader
#         self.margin_config = params.margin_config
        
#         # Per-symbol state
#         self.positions: Dict[str, List[Position]] = {s: [] for s in params.symbols}
#         self.closed_positions: List[Position] = []
        
#         # Track strike+option_type separately
#         self.traded_strikes: Dict[str, Dict[str, bool]] = {s: {} for s in params.symbols}
        
#         # Data storage
#         self.spot_data: Dict[str, pd.DataFrame] = {}
#         self.options_data: Dict[str, pd.DataFrame] = {}
        
#         # Results
#         self.daily_pnl: Dict[datetime, float] = {}
#         self.margin_history: List[Dict] = []
#         self.trade_log: List[Dict] = []
        
#         # Hedge tracking
#         self.is_first_hedge: Dict[str, bool] = {s: True for s in params.symbols}
#         self.hedge_strikes: Dict[str, List[str]] = {s: [] for s in params.symbols}

#     def load_market_data(self, symbol: str, start_date: datetime, end_date: datetime) -> None:
#         """Load spot and options data"""
#         data = self.data_loader.load_data(symbol, start_date, end_date)
#         self.spot_data[symbol] = data['spot']
#         self.options_data[symbol] = data['options']
        
#         print(f"  Loaded {len(self.spot_data[symbol])} spot candles")
#         print(f"  Loaded {len(self.options_data[symbol])} option records")
        
#         if not self.options_data[symbol].empty:
#             expiries = self.options_data[symbol]['expiry'].unique()
#             print(f"  Available expiries: {[str(e)[:10] for e in expiries]}")

#     def get_atm_strike(self, symbol: str, timestamp: datetime) -> float:
#         """Get ATM strike based on spot price"""
#         spot_df = self.spot_data[symbol]
#         spot_data = spot_df[spot_df.index <= timestamp]
#         if spot_data.empty:
#             return 0
#         spot_price = spot_data['close'].iloc[-1]
#         strike_diff = self.params.strike_diffs.get(symbol, 50)
#         return round(spot_price / strike_diff) * strike_diff

#     def get_spot_price(self, symbol: str, timestamp: datetime) -> float:
#         """Get spot price at timestamp"""
#         spot_df = self.spot_data[symbol]
#         spot_data = spot_df[spot_df.index <= timestamp]
#         if spot_data.empty:
#             return 0
#         return spot_data['close'].iloc[-1]

#     def get_option_price(self, symbol: str, strike: float, option_type: str,
#                          expiry: datetime, timestamp: datetime,
#                          price_type: str = 'close') -> float:
#         """Get option price at timestamp"""
#         options_df = self.options_data[symbol]
#         expiry_normalized = pd.Timestamp(expiry).normalize()
        
#         price_data = options_df[
#             (options_df['strike'] == strike) &
#             (options_df['option_type'] == option_type) &
#             (options_df['expiry'].dt.normalize() == expiry_normalized) &
#             (options_df.index <= timestamp)]
        
#         if price_data.empty:
#             return 0.0
#         return price_data[price_type].iloc[-1]

#     def get_option_high(self, symbol: str, strike: float, option_type: str,
#                         expiry: datetime, date) -> float:
#         """Get option day high for SL modification"""
#         options_df = self.options_data[symbol]
#         expiry_normalized = pd.Timestamp(expiry).normalize()
        
#         day_data = options_df[
#             (options_df['strike'] == strike) &
#             (options_df['option_type'] == option_type) &
#             (options_df['expiry'].dt.normalize() == expiry_normalized) &
#             (options_df.index.date == date)]
        
#         if day_data.empty:
#             return 0.0
#         return day_data['high'].max()

#     def get_nearest_expiry(self, symbol: str, timestamp: datetime) -> Optional[datetime]:
#         """Get nearest expiry on or after timestamp date"""
#         options_df = self.options_data[symbol]
#         today = pd.Timestamp(timestamp).normalize()
#         expiries = options_df[options_df['expiry'] >= today]['expiry'].unique()
#         if len(expiries) == 0:
#             return None
#         return min(expiries)

#     def get_days_to_expiry(self, expiry: datetime, current_date) -> int:
#         """Calculate days to expiry"""
#         expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
#         return (expiry_date - current_date).days

#     def get_open_positions(self, symbol: str) -> List[Position]:
#         """Get list of open positions"""
#         return [p for p in self.positions[symbol] if p.is_open]

#     def get_open_short_positions(self, symbol: str) -> List[Position]:
#         """Get list of open short positions (non-hedge)"""
#         return [p for p in self.positions[symbol] 
#                 if p.is_open and p.is_short and not p.is_hedge]

#     def get_open_hedge_positions(self, symbol: str) -> List[Position]:
#         """Get list of open hedge positions"""
#         return [p for p in self.positions[symbol] 
#                 if p.is_open and p.is_hedge]

#     def calculate_margin(self, symbol: str, timestamp: datetime) -> Dict:
#         """Calculate current margin usage"""
#         total_margin = 0.0
#         breakdown = {}
#         spot_price = self.get_spot_price(symbol, timestamp)
#         lot_size = self.params.lot_sizes.get(symbol, 75)
        
#         expiry = self.get_nearest_expiry(symbol, timestamp)
#         if expiry is None:
#             return {'total_margin': 0, 'margin_percent': 0, 'breakdown': {}}

#         for pos in self.get_open_positions(symbol):
#             opt_price = self.get_option_price(
#                 symbol, pos.strike, pos.option_type, pos.expiry, timestamp)
#             margin = self.margin_config.calculate_span_margin(
#                 spot_price=spot_price, lot_size=lot_size, option_price=opt_price,
#                 quantity=pos.quantity, is_short=pos.is_short)
#             key = f"{pos.strike}_{pos.option_type}"
#             breakdown[key] = margin
#             total_margin += margin

#         margin_percent = (total_margin / self.params.capital_per_symbol) * 100
#         return {
#             'total_margin': total_margin,
#             'margin_percent': margin_percent,
#             'breakdown': breakdown
#         }

#     def get_straddle_vwap_signal(self, symbol: str, strike: float, 
#                                   expiry: datetime, timestamp: datetime,
#                                   current_date) -> Optional[Dict]:
#         """
#         Calculate straddle VWAP signal for a strike
#         VWAP resets daily at 9:15
#         """
#         options_df = self.options_data[symbol]
#         expiry_normalized = pd.Timestamp(expiry).normalize()
        
#         # Day start at 9:15
#         day_start = pd.Timestamp(datetime.combine(current_date, 
#                                                    datetime.strptime("09:15:00", "%H:%M:%S").time()))
        
#         # Get CE data for the day
#         ce_data = options_df[
#             (options_df['strike'] == strike) &
#             (options_df['option_type'] == 'CE') &
#             (options_df['expiry'].dt.normalize() == expiry_normalized) &
#             (options_df.index >= day_start) &
#             (options_df.index <= timestamp)
#         ].copy()
        
#         # Get PE data for the day
#         pe_data = options_df[
#             (options_df['strike'] == strike) &
#             (options_df['option_type'] == 'PE') &
#             (options_df['expiry'].dt.normalize() == expiry_normalized) &
#             (options_df.index >= day_start) &
#             (options_df.index <= timestamp)
#         ].copy()
        
#         if ce_data.empty or pe_data.empty:
#             return None
        
#         # Merge CE and PE data
#         merged = pd.merge(
#             ce_data[['close', 'volume', 'high', 'low']].rename(
#                 columns={'close': 'ce_close', 'volume': 'ce_volume',
#                          'high': 'ce_high', 'low': 'ce_low'}),
#             pe_data[['close', 'volume', 'high', 'low']].rename(
#                 columns={'close': 'pe_close', 'volume': 'pe_volume',
#                          'high': 'pe_high', 'low': 'pe_low'}),
#             left_index=True, right_index=True, how='inner')
        
#         if len(merged) < 2:
#             return None
        
#         # Create straddle data
#         merged['close'] = merged['ce_close'] + merged['pe_close']
#         merged['volume'] = merged[['ce_volume', 'pe_volume']].min(axis=1)
        
#         # Calculate VWAP (resets daily since we filtered from day_start)
#         straddle_vwap = VWAPIndicator.calculate(
#             merged[['close', 'volume']].reset_index(),
#             buy_threshold=self.params.threshold_percentage,
#             sell_threshold=self.params.threshold_percentage)
        
#         if 'date' in straddle_vwap.columns:
#             straddle_vwap.set_index('date', inplace=True)
        
#         # Get previous candle signal (matches live: iloc[-2])
#         if len(straddle_vwap) < 2:
#             return None
        
#         prev_candle = straddle_vwap.iloc[-2]
        
#         return {
#             'signal': prev_candle['signal'],
#             'can_trade': prev_candle['can_trade'],
#             'indicator': prev_candle['indicator'],
#             'close': prev_candle['close'],
#             'perc_diff': prev_candle['perc_diff'],
#             'ce_close': merged.iloc[-2]['ce_close'],
#             'pe_close': merged.iloc[-2]['pe_close']
#         }

#     def check_signals(self, symbol: str, timestamp: datetime, current_date) -> List[Dict]:
#         """Check for entry signals"""
#         signals = []
#         atm = self.get_atm_strike(symbol, timestamp)
#         if atm == 0:
#             return signals

#         strike_diff = self.params.strike_diffs.get(symbol, 50)
#         expiry = self.get_nearest_expiry(symbol, timestamp)
#         if expiry is None:
#             return signals

#         # Check days to expiry filter
#         days_to_expiry = self.get_days_to_expiry(expiry, current_date)
#         if days_to_expiry not in self.params.active_days_to_expiry:
#             return signals

#         spot_price = self.get_spot_price(symbol, timestamp)
#         min_premium = spot_price * (self.params.min_premium_percent / 100)

#         for strike_offset in range(-self.params.num_strikes, self.params.num_strikes + 1):
#             strike = atm + (strike_offset * strike_diff)
            
#             # Get VWAP signal for this strike
#             vwap_data = self.get_straddle_vwap_signal(
#                 symbol, strike, expiry, timestamp, current_date)
            
#             if vwap_data is None:
#                 continue
            
#             # Check if signal is SELL and can_trade
#             if vwap_data['signal'] != 'sell' or not vwap_data['can_trade']:
#                 continue
            
#             # Check CE - Track strike+option_type separately
#             ce_key = f"{strike}_CE_{expiry.date() if hasattr(expiry, 'date') else expiry}"
#             if not self.traded_strikes[symbol].get(ce_key):
#                 ce_price = vwap_data['ce_close']
#                 if ce_price > min_premium:
#                     signals.append({
#                         'strike': strike,
#                         'option_type': 'CE',
#                         'expiry': expiry,
#                         'signal_type': 'SELL',
#                         'straddle_price': vwap_data['close'],
#                         'vwap': vwap_data['indicator'],
#                         'deviation': vwap_data['perc_diff'],
#                         'entry_price': ce_price,
#                         'timestamp': timestamp
#                     })
            
#             # Check PE
#             pe_key = f"{strike}_PE_{expiry.date() if hasattr(expiry, 'date') else expiry}"
#             if not self.traded_strikes[symbol].get(pe_key):
#                 pe_price = vwap_data['pe_close']
#                 if pe_price > min_premium:
#                     signals.append({
#                         'strike': strike,
#                         'option_type': 'PE',
#                         'expiry': expiry,
#                         'signal_type': 'SELL',
#                         'straddle_price': vwap_data['close'],
#                         'vwap': vwap_data['indicator'],
#                         'deviation': vwap_data['perc_diff'],
#                         'entry_price': pe_price,
#                         'timestamp': timestamp
#                     })

#         return signals

#     def take_position(self, symbol: str, signal: Dict, timestamp: datetime) -> Optional[Position]:
#         """Take a new short position"""
#         # Check margin limit
#         margin_info = self.calculate_margin(symbol, timestamp)
#         if margin_info['margin_percent'] >= self.params.new_position_margin_limit:
#             self._log_trade(timestamp, symbol, signal['strike'], signal['option_type'],
#                             'ENTRY_REJECTED', f"Margin limit: {margin_info['margin_percent']:.1f}%", 0, 0)
#             return None

#         # Check max open strikes per leg
#         open_shorts = self.get_open_short_positions(symbol)
#         ce_count = len([p for p in open_shorts if p.option_type == 'CE'])
#         pe_count = len([p for p in open_shorts if p.option_type == 'PE'])

#         if signal['option_type'] == 'CE' and ce_count >= self.params.max_open_strikes_per_leg:
#             return None
#         if signal['option_type'] == 'PE' and pe_count >= self.params.max_open_strikes_per_leg:
#             return None

#         lot_size = self.params.lot_sizes.get(symbol, 75)
        
#         # Get current price
#         entry_price = self.get_option_price(
#             symbol, signal['strike'], signal['option_type'],
#             signal['expiry'], timestamp)
#         if entry_price <= 0:
#             return None

#         # Use quantity_per_strike from params
#         quantity = -self.params.quantity_per_strike.get(symbol, 800)

#         # Calculate SL price
#         sl_price = entry_price * (1 + self.params.sl_percentage / 100)
#         sl_limit_price = sl_price * (1 + self.params.sl_diff_percentage / 100)

#         position = Position(
#             symbol=symbol,
#             strike=signal['strike'],
#             option_type=signal['option_type'],
#             expiry=signal['expiry'],
#             entry_time=timestamp,
#             entry_price=entry_price,
#             quantity=quantity,
#             lot_size=lot_size,
#             sl_price=round(sl_price, 2),
#             sl_limit_price=round(sl_limit_price, 2),
#             sl_order_placed=True
#         )

#         self.positions[symbol].append(position)
        
#         # Mark strike+option as traded
#         expiry_date = signal['expiry'].date() if hasattr(signal['expiry'], 'date') else signal['expiry']
#         strike_key = f"{signal['strike']}_{signal['option_type']}_{expiry_date}"
#         self.traded_strikes[symbol][strike_key] = True

#         self._log_trade(timestamp, symbol, signal['strike'], signal['option_type'],
#                         'ENTRY', f"Qty: {quantity}, SL: {sl_price:.2f}", entry_price, quantity)
#         return position

#     def check_and_take_hedge(self, symbol: str, timestamp: datetime) -> List[Position]:
#         """Check margin and take hedge positions if needed"""
#         hedges = []
        
#         margin_info = self.calculate_margin(symbol, timestamp)
#         if margin_info['margin_percent'] < self.params.hedge_margin_limit:
#             return hedges

#         open_shorts = self.get_open_short_positions(symbol)
#         if not open_shorts:
#             return hedges

#         spot_price = self.get_spot_price(symbol, timestamp)
#         strike_diff = self.params.strike_diffs.get(symbol, 50)
#         lot_size = self.params.lot_sizes.get(symbol, 75)
#         expiry = self.get_nearest_expiry(symbol, timestamp)
#         if expiry is None:
#             return hedges

#         # Calculate short quantities per leg
#         ce_short_qty = sum(abs(p.quantity) for p in open_shorts if p.option_type == 'CE')
#         pe_short_qty = sum(abs(p.quantity) for p in open_shorts if p.option_type == 'PE')

#         # Get existing hedge quantities
#         existing_hedges = self.get_open_hedge_positions(symbol)
#         ce_hedge_qty = sum(p.quantity for p in existing_hedges if p.option_type == 'CE')
#         pe_hedge_qty = sum(p.quantity for p in existing_hedges if p.option_type == 'PE')

#         # Determine hedge percentage
#         hedge_pct = (self.params.hedge_strike_diff_percent if self.is_first_hedge[symbol]
#                      else self.params.hedge_strike_diff_percent_2)

#         # Calculate hedge strikes
#         ce_hedge_strike = round(spot_price * (1 + hedge_pct / 100) / strike_diff) * strike_diff
#         pe_hedge_strike = round(spot_price * (1 - hedge_pct / 100) / strike_diff) * strike_diff

#         # Hedge CE if needed
#         if ce_short_qty > 0 and ce_hedge_qty < ce_short_qty // 2:
#             hedge_qty = round(ce_short_qty / lot_size / 2) * lot_size
#             hedge_qty = max(lot_size, hedge_qty)
            
#             entry_price = self.get_option_price(symbol, ce_hedge_strike, 'CE', expiry, timestamp)
#             if entry_price > 0:
#                 hedge_pos = Position(
#                     symbol=symbol,
#                     strike=ce_hedge_strike,
#                     option_type='CE',
#                     expiry=expiry,
#                     entry_time=timestamp,
#                     entry_price=entry_price,
#                     quantity=int(hedge_qty),
#                     lot_size=lot_size,
#                     is_hedge=True
#                 )
#                 self.positions[symbol].append(hedge_pos)
#                 self.hedge_strikes[symbol].append(str(ce_hedge_strike))
#                 hedges.append(hedge_pos)
#                 self._log_trade(timestamp, symbol, ce_hedge_strike, 'CE',
#                                 'HEDGE_ENTRY', f"BUY hedge, Qty: {hedge_qty}", entry_price, hedge_qty)

#         # Hedge PE if needed
#         if pe_short_qty > 0 and pe_hedge_qty < pe_short_qty // 2:
#             hedge_qty = round(pe_short_qty / lot_size / 2) * lot_size
#             hedge_qty = max(lot_size, hedge_qty)
            
#             entry_price = self.get_option_price(symbol, pe_hedge_strike, 'PE', expiry, timestamp)
#             if entry_price > 0:
#                 hedge_pos = Position(
#                     symbol=symbol,
#                     strike=pe_hedge_strike,
#                     option_type='PE',
#                     expiry=expiry,
#                     entry_time=timestamp,
#                     entry_price=entry_price,
#                     quantity=int(hedge_qty),
#                     lot_size=lot_size,
#                     is_hedge=True
#                 )
#                 self.positions[symbol].append(hedge_pos)
#                 self.hedge_strikes[symbol].append(str(pe_hedge_strike))
#                 hedges.append(hedge_pos)
#                 self._log_trade(timestamp, symbol, pe_hedge_strike, 'PE',
#                                 'HEDGE_ENTRY', f"BUY hedge, Qty: {hedge_qty}", entry_price, hedge_qty)

#         if hedges:
#             self.is_first_hedge[symbol] = False

#         return hedges

#     def check_sl_hits(self, symbol: str, timestamp: datetime) -> List[Position]:
#         """Check for stop loss hits"""
#         hit_positions = []
        
#         for pos in self.positions[symbol]:
#             if not pos.is_open or not pos.is_short or pos.sl_hit:
#                 continue
#             if pos.sl_price is None:
#                 continue
            
#             current_price = self.get_option_price(
#                 symbol, pos.strike, pos.option_type, pos.expiry, timestamp)
#             if current_price <= 0:
#                 continue
            
#             if current_price >= pos.sl_price:
#                 pos.sl_hit = True
#                 pos.exit_time = timestamp
#                 pos.exit_price = min(current_price, pos.sl_limit_price) if pos.sl_limit_price else current_price
#                 pos.exit_reason = 'SL_HIT' if pos.sl2_price is None else 'SL2_HIT'
#                 pos.pnl = (pos.entry_price - pos.exit_price) * abs(pos.quantity)
#                 hit_positions.append(pos)
#                 self.closed_positions.append(pos)
#                 self._log_trade(timestamp, symbol, pos.strike, pos.option_type,
#                                 pos.exit_reason, f'Exit: {pos.exit_price:.2f}, PnL: {pos.pnl:.2f}',
#                                 pos.exit_price, pos.quantity)
        
#         return hit_positions

#     def modify_sl_to_high(self, symbol: str, timestamp: datetime, current_date) -> List[Position]:
#         """Modify SL to day's high"""
#         modified = []
        
#         for pos in self.positions[symbol]:
#             if not pos.is_open or not pos.is_short or pos.sl_hit:
#                 continue
#             if pos.sl2_price is not None:
#                 continue
            
#             day_high = self.get_option_high(
#                 symbol, pos.strike, pos.option_type, pos.expiry, current_date)
            
#             if day_high <= 0:
#                 continue
            
#             if pos.sl_price and day_high < pos.sl_price:
#                 pos.sl2_price = day_high + 1
#                 pos.sl_price = pos.sl2_price
#                 pos.sl_limit_price = pos.sl2_price * (1 + self.params.sl_diff_percentage / 100)
#                 modified.append(pos)
#                 self._log_trade(timestamp, symbol, pos.strike, pos.option_type,
#                                 'SL_MODIFIED', f'New SL: {pos.sl_price:.2f}', pos.sl_price, 0)
        
#         return modified

#     def run_rms_checks(self, symbol: str, timestamp: datetime) -> List[Position]:
#         """Run RMS checks"""
#         rms_exits = []
#         atm = self.get_atm_strike(symbol, timestamp)
#         strike_diff = self.params.strike_diffs.get(symbol, 50)
        
#         min_valid_strike = atm - (self.params.num_strikes * strike_diff)
#         max_valid_strike = atm + (self.params.num_strikes * strike_diff)

#         for pos in self.positions[symbol]:
#             if not pos.is_open or pos.is_hedge:
#                 continue
            
#             should_exit = False
#             exit_reason = None

#             if pos.quantity > 0:
#                 should_exit = True
#                 exit_reason = 'RMS_POSITIVE_QTY'
#             elif pos.strike < min_valid_strike or pos.strike > max_valid_strike:
#                 should_exit = True
#                 exit_reason = 'RMS_INVALID_STRIKE'
#             elif pos.sl_hit and pos.exit_time is None:
#                 should_exit = True
#                 exit_reason = 'RMS_SL_HIT'

#             if should_exit:
#                 current_price = self.get_option_price(
#                     symbol, pos.strike, pos.option_type, pos.expiry, timestamp)
#                 pos.exit_time = timestamp
#                 pos.exit_price = current_price
#                 pos.exit_reason = exit_reason
#                 if pos.is_short:
#                     pos.pnl = (pos.entry_price - pos.exit_price) * abs(pos.quantity)
#                 else:
#                     pos.pnl = (pos.exit_price - pos.entry_price) * pos.quantity
#                 rms_exits.append(pos)
#                 self.closed_positions.append(pos)
#                 self._log_trade(timestamp, symbol, pos.strike, pos.option_type,
#                                 exit_reason, f'RMS exit: {current_price:.2f}', current_price, pos.quantity)
        
#         return rms_exits

#     def square_off_all(self, symbol: str, timestamp: datetime) -> List[Position]:
#         """Square off all positions at EOD"""
#         squared_off = []
        
#         # First square off shorts
#         shorts = [p for p in self.positions[symbol] if p.is_open and p.is_short]
#         for pos in shorts:
#             exit_price = self.get_option_price(
#                 symbol, pos.strike, pos.option_type, pos.expiry, timestamp)
#             pos.exit_time = timestamp
#             pos.exit_price = exit_price
#             pos.exit_reason = 'EOD_SQUAREOFF'
#             pos.pnl = (pos.entry_price - exit_price) * abs(pos.quantity)
#             squared_off.append(pos)
#             self.closed_positions.append(pos)
#             self._log_trade(timestamp, symbol, pos.strike, pos.option_type,
#                             'EOD_SQUAREOFF', f'Short: {exit_price:.2f}, PnL: {pos.pnl:.2f}', 
#                             exit_price, pos.quantity)

#         # Then square off hedges
#         longs = [p for p in self.positions[symbol] if p.is_open and not p.is_short]
#         for pos in longs:
#             exit_price = self.get_option_price(
#                 symbol, pos.strike, pos.option_type, pos.expiry, timestamp)
#             pos.exit_time = timestamp
#             pos.exit_price = exit_price
#             pos.exit_reason = 'EOD_SQUAREOFF'
#             pos.pnl = (pos.exit_price - pos.entry_price) * pos.quantity
#             squared_off.append(pos)
#             self.closed_positions.append(pos)
#             self._log_trade(timestamp, symbol, pos.strike, pos.option_type,
#                             'EOD_SQUAREOFF', f'Hedge: {exit_price:.2f}, PnL: {pos.pnl:.2f}', 
#                             exit_price, pos.quantity)
        
#         return squared_off

#     def run_backtest(self, start_date: datetime, end_date: datetime) -> Dict:
#         """Run backtest for date range"""
#         print("=" * 60)
#         print("OSTRAD BACKTEST")
#         print("=" * 60)
#         print(f"Date Range: {start_date.date()} to {end_date.date()}")
#         print(f"Symbols: {self.params.symbols}")
#         print(f"Quantity per strike: {self.params.quantity_per_strike}")

#         results = {
#             'trades': [],
#             'daily_pnl': {},
#             'margin_history': [],
#             'statistics': {}
#         }

#         for symbol in self.params.symbols:
#             print(f"\nProcessing {symbol}...")
#             self.load_market_data(symbol, start_date, end_date)
            
#             print(f"Running backtest for {symbol}...")
#             symbol_results = self._run_symbol_backtest(symbol, start_date, end_date)
            
#             results['trades'].extend(symbol_results['trades'])
#             for date, pnl in symbol_results['daily_pnl'].items():
#                 if date not in results['daily_pnl']:
#                     results['daily_pnl'][date] = 0
#                 results['daily_pnl'][date] += pnl
#             results['margin_history'].extend(symbol_results['margin_history'])

#         results['statistics'] = self._calculate_statistics(results)
#         return results

#     def _run_symbol_backtest(self, symbol: str, start_date: datetime,
#                              end_date: datetime) -> Dict:
#         """Run backtest for a single symbol"""
#         trades = []
#         daily_pnl = {}
#         margin_history = []

#         start_ts = pd.Timestamp(start_date)
#         if start_ts.tz is not None:
#             start_ts = start_ts.tz_localize(None)
#         end_ts = pd.Timestamp(end_date)
#         if end_ts.tz is not None:
#             end_ts = end_ts.tz_localize(None)

#         current_date = start_ts.date()
#         end_date_only = end_ts.date()

#         while current_date <= end_date_only:
#             if current_date.weekday() >= 5:
#                 current_date += timedelta(days=1)
#                 continue

#             # Reset daily state
#             self.traded_strikes[symbol] = {}
#             self.positions[symbol] = []
#             self.is_first_hedge[symbol] = True
#             self.hedge_strikes[symbol] = []
#             day_pnl = 0.0

#             day_start = datetime.combine(
#                 current_date,
#                 datetime.strptime(self.params.start_time, "%H:%M:%S").time())
#             entry_end = datetime.combine(
#                 current_date,
#                 datetime.strptime(self.params.entry_end_time, "%H:%M:%S").time())
#             sl_mod_time = datetime.combine(
#                 current_date,
#                 datetime.strptime(self.params.sl_modification_time, "%H:%M:%S").time())
#             eod_time = datetime.combine(
#                 current_date,
#                 datetime.strptime(self.params.square_off_time, "%H:%M:%S").time())

#             timestamps = pd.date_range(
#                 start=day_start, 
#                 end=eod_time,
#                 freq=f'{self.params.time_frame}min')

#             sl_modified_today = False

#             for timestamp in timestamps:
#                 if symbol not in self.spot_data:
#                     continue
#                 available_spot = self.spot_data[symbol][
#                     self.spot_data[symbol].index <= timestamp]
#                 if available_spot.empty:
#                     continue

#                 # RMS Checks
#                 rms_exits = self.run_rms_checks(symbol, timestamp)
#                 for pos in rms_exits:
#                     day_pnl += pos.pnl
#                     trades.append(self._position_to_dict(pos))

#                 # Hedge Check
#                 hedges = self.check_and_take_hedge(symbol, timestamp)
#                 for hedge in hedges:
#                     trades.append(self._position_to_dict(hedge))

#                 # Entry Signals
#                 if timestamp <= entry_end:
#                     signals = self.check_signals(symbol, timestamp, current_date)
#                     for signal in signals:
#                         pos = self.take_position(symbol, signal, timestamp)
#                         if pos:
#                             trades.append(self._position_to_dict(pos))

#                 # SL Modification
#                 if timestamp >= sl_mod_time and not sl_modified_today:
#                     modified = self.modify_sl_to_high(symbol, timestamp, current_date)
#                     if modified:
#                         sl_modified_today = True
#                     for mod in modified:
#                         trades.append(self._position_to_dict(mod))

#                 # SL Hit Check
#                 hits = self.check_sl_hits(symbol, timestamp)
#                 for hit in hits:
#                     day_pnl += hit.pnl
#                     trades.append(self._position_to_dict(hit))

#                 # Margin tracking
#                 margin_info = self.calculate_margin(symbol, timestamp)
#                 margin_history.append({
#                     'timestamp': timestamp,
#                     'symbol': symbol,
#                     'total_margin': margin_info['total_margin'],
#                     'margin_percent': margin_info['margin_percent']
#                 })

#             # EOD Square off
#             squared = self.square_off_all(symbol, eod_time)
#             for sq in squared:
#                 day_pnl += sq.pnl
#                 trades.append(self._position_to_dict(sq))

#             daily_pnl[current_date] = day_pnl
#             print(f"  {current_date}: P&L = ₹{day_pnl:,.2f}")
            
#             current_date += timedelta(days=1)

#         return {
#             'trades': trades,
#             'daily_pnl': daily_pnl,
#             'margin_history': margin_history
#         }

#     def _position_to_dict(self, pos: Position) -> Dict:
#         """Convert position to dictionary"""
#         return {
#             'symbol': pos.symbol,
#             'strike': pos.strike,
#             'option_type': pos.option_type,
#             'expiry': pos.expiry,
#             'entry_time': pos.entry_time,
#             'entry_price': pos.entry_price,
#             'quantity': pos.quantity,
#             'lots': pos.lots,
#             'sl_price': pos.sl_price,
#             'sl2_price': pos.sl2_price,
#             'is_hedge': pos.is_hedge,
#             'exit_time': pos.exit_time,
#             'exit_price': pos.exit_price,
#             'exit_reason': pos.exit_reason,
#             'pnl': pos.pnl
#         }

#     def _log_trade(self, timestamp: datetime, symbol: str, strike: float,
#                    option_type: str, event: str, message: str,
#                    price: float, quantity: int) -> None:
#         """Log trade event"""
#         self.trade_log.append({
#             'timestamp': timestamp,
#             'symbol': symbol,
#             'strike': strike,
#             'option_type': option_type,
#             'event': event,
#             'message': message,
#             'price': price,
#             'quantity': quantity
#         })

#     def _calculate_statistics(self, results: Dict) -> Dict:
#         """Calculate backtest statistics"""
#         trades_df = pd.DataFrame(results['trades'])
#         if trades_df.empty:
#             return {'error': 'No trades executed'}

#         completed = trades_df[trades_df['exit_time'].notna()].copy()
#         if completed.empty:
#             return {'error': 'No completed trades'}

#         total_pnl = completed['pnl'].sum()
#         winning = completed[completed['pnl'] > 0]
#         losing = completed[completed['pnl'] < 0]

#         stats = {
#             'total_trades': len(completed),
#             'total_pnl': total_pnl,
#             'winning_trades': len(winning),
#             'losing_trades': len(losing),
#             'win_rate': len(winning) / len(completed) * 100 if len(completed) > 0 else 0,
#             'avg_pnl_per_trade': completed['pnl'].mean(),
#             'max_profit': completed['pnl'].max(),
#             'max_loss': completed['pnl'].min(),
#             'avg_winner': winning['pnl'].mean() if len(winning) > 0 else 0,
#             'avg_loser': losing['pnl'].mean() if len(losing) > 0 else 0,
#             'profit_factor': abs(winning['pnl'].sum() / losing['pnl'].sum())
#                 if losing['pnl'].sum() != 0 else float('inf')
#         }

#         for reason in ['SL_HIT', 'SL2_HIT', 'EOD_SQUAREOFF', 'RMS_POSITIVE_QTY', 'RMS_INVALID_STRIKE']:
#             count = len(completed[completed['exit_reason'] == reason])
#             stats[f'{reason.lower()}_count'] = count
#             stats[f'{reason.lower()}_rate'] = count / len(completed) * 100 if len(completed) > 0 else 0

#         daily_pnl = pd.Series(results['daily_pnl'])
#         if len(daily_pnl) > 0:
#             stats['trading_days'] = len(daily_pnl)
#             stats['profitable_days'] = len(daily_pnl[daily_pnl > 0])
#             stats['losing_days'] = len(daily_pnl[daily_pnl < 0])
#             stats['best_day'] = daily_pnl.max()
#             stats['worst_day'] = daily_pnl.min()
#             stats['avg_daily_pnl'] = daily_pnl.mean()
#             if daily_pnl.std() != 0:
#                 stats['sharpe_ratio'] = (daily_pnl.mean() / daily_pnl.std()) * np.sqrt(252)
#             else:
#                 stats['sharpe_ratio'] = 0

#         return stats

#     def print_trade_log(self, limit: int = 50) -> None:
#         """Print trade log"""
#         print("\n" + "=" * 80)
#         print("TRADE LOG")
#         print("=" * 80)
#         for entry in self.trade_log[:limit]:
#             ts = entry['timestamp'].strftime('%Y-%m-%d %H:%M') if entry['timestamp'] else 'N/A'
#             print(f"{ts} | {entry['symbol']} {entry['strike']}{entry['option_type']} | "
#                   f"{entry['event']}: {entry['message']}")
#         if len(self.trade_log) > limit:
#             print(f"... and {len(self.trade_log) - limit} more entries")

#     def print_results(self, results: Dict) -> None:
#         """Print backtest results"""
#         print("\n" + "=" * 60)
#         print("BACKTEST RESULTS")
#         print("=" * 60)

#         stats = results['statistics']

#         if 'error' in stats:
#             print(f"Error: {stats['error']}")
#             return

#         print(f"\n{'='*40}")
#         print("SUMMARY")
#         print(f"{'='*40}")
#         print(f"Total Trades:        {stats.get('total_trades', 0)}")
#         print(f"Total P&L:           ₹{stats.get('total_pnl', 0):,.2f}")
#         print(f"Win Rate:            {stats.get('win_rate', 0):.1f}%")
#         print(f"Profit Factor:       {stats.get('profit_factor', 0):.2f}")

#         print(f"\n{'='*40}")
#         print("TRADE BREAKDOWN")
#         print(f"{'='*40}")
#         print(f"Winning Trades:      {stats.get('winning_trades', 0)}")
#         print(f"Losing Trades:       {stats.get('losing_trades', 0)}")
#         print(f"Max Profit:          ₹{stats.get('max_profit', 0):,.2f}")
#         print(f"Max Loss:            ₹{stats.get('max_loss', 0):,.2f}")
#         print(f"Avg Winner:          ₹{stats.get('avg_winner', 0):,.2f}")
#         print(f"Avg Loser:           ₹{stats.get('avg_loser', 0):,.2f}")

#         print(f"\n{'='*40}")
#         print("EXIT REASONS")
#         print(f"{'='*40}")
#         print(f"SL Hit:              {stats.get('sl_hit_count', 0)} ({stats.get('sl_hit_rate', 0):.1f}%)")
#         print(f"SL2 Hit:             {stats.get('sl2_hit_count', 0)} ({stats.get('sl2_hit_rate', 0):.1f}%)")
#         print(f"EOD Squareoff:       {stats.get('eod_squareoff_count', 0)} ({stats.get('eod_squareoff_rate', 0):.1f}%)")

#         print(f"\n{'='*40}")
#         print("DAILY STATISTICS")
#         print(f"{'='*40}")
#         print(f"Trading Days:        {stats.get('trading_days', 0)}")
#         print(f"Profitable Days:     {stats.get('profitable_days', 0)}")
#         print(f"Losing Days:         {stats.get('losing_days', 0)}")
#         print(f"Best Day:            ₹{stats.get('best_day', 0):,.2f}")
#         print(f"Worst Day:           ₹{stats.get('worst_day', 0):,.2f}")
#         print(f"Avg Daily P&L:       ₹{stats.get('avg_daily_pnl', 0):,.2f}")
#         print(f"Sharpe Ratio:        {stats.get('sharpe_ratio', 0):.2f}")

# import numpy as np
# import pandas as pd
# from datetime import datetime, timedelta, date
# from typing import Dict, List, Optional
# from dataclasses import dataclass, field
# from abc import ABC, abstractmethod
# import warnings
# warnings.filterwarnings('ignore')


# # =============================================================================
# # CONFIGURATION
# # =============================================================================

# @dataclass
# class MarginConfig:
#     var_margin_percent: float = 12.0
#     elm_percent: float = 3.0
#     span_multiplier: float = 1.0
#     min_margin_per_lot: float = 50000

#     def calculate_span_margin(self, spot_price: float, lot_size: int,
#                               option_price: float, quantity: int,
#                               is_short: bool = True) -> float:
#         if is_short:
#             base_margin = spot_price * lot_size * (self.var_margin_percent / 100)
#             elm_margin = spot_price * lot_size * (self.elm_percent / 100)
#             total_base = (base_margin + elm_margin) * abs(quantity) / lot_size
#             total_base *= self.span_multiplier
#             premium_received = option_price * abs(quantity)
#             margin = max(total_base - premium_received,
#                          self.min_margin_per_lot * abs(quantity) / lot_size)
#         else:
#             margin = option_price * abs(quantity)
#         return max(margin, 0)


# @dataclass
# class StrategyParams:
#     # Timing
#     time_frame: int = 15                        # minutes - match live
#     start_time: str = "09:25:00"
#     entry_end_time: str = "14:00:00"            # fixed: live uses 14:00
#     sl_modification_time: str = "10:30:00"      # fixed: live uses 10:30
#     square_off_time: str = "15:15:00"           # fixed: live uses 15:15

#     # Strategy - per symbol dicts
#     threshold_percentage: float = 15.0          # fixed: live uses 15
#     sl_percentage: Dict[str, float] = field(default_factory=lambda: {
#         'BANKNIFTY': 15.0,
#         'NIFTY': 22.0
#     })
#     sl_diff_percentage: float = 10.0            # fixed: live uses 10
#     num_strikes: Dict[str, int] = field(default_factory=lambda: {
#         'BANKNIFTY': 5,
#         'NIFTY': 2
#     })
#     max_open_strikes_per_leg: int = 18          # match live
#     min_premium_percent: float = 0.055

#     # Capital & Margin
#     capital_per_symbol: Dict[str, float] = field(default_factory=lambda: {
#         'BANKNIFTY': 75_000_000,
#         'NIFTY': 50_000_000
#     })
#     new_position_margin_limit: float = 100.0    # fixed: live uses 100
#     hedge_margin_limit: float = 100.0           # fixed: live uses 100
#     hedge_strike_diff_percent: float = 2.5
#     hedge_strike_diff_percent_2: float = 5.0

#     # Symbols
#     symbols: List[str] = field(default_factory=lambda: ['NIFTY'])
#     active_days_to_expiry: List[int] = field(default_factory=lambda: [0, 1, 3, 5])  # match live
#     lot_sizes: Dict[str, int] = field(default_factory=lambda: {
#         'BANKNIFTY': 15,
#         'NIFTY': 75
#     })
#     strike_diffs: Dict[str, float] = field(default_factory=lambda: {
#         'BANKNIFTY': 100.0,
#         'NIFTY': 50.0
#     })
#     quantity_per_strike: Dict[str, int] = field(default_factory=lambda: {
#         'BANKNIFTY': 15,
#         'NIFTY': 75
#     })

#     holidays: List[date] = field(default_factory=list)
#     margin_config: MarginConfig = field(default_factory=MarginConfig)


# # =============================================================================
# # DATA LOADER
# # =============================================================================

# class DataLoader(ABC):
#     @abstractmethod
#     def load_data(self, symbol: str, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
#         pass


# class DataFrameLoader(DataLoader):
#     def __init__(self, spot_df: pd.DataFrame, options_df: pd.DataFrame):
#         self.spot_df = self._prepare_df(spot_df)
#         self.options_df = self._prepare_df(options_df)

#         if 'expiry' in self.options_df.columns:
#             self.options_df['expiry'] = pd.to_datetime(self.options_df['expiry'])
#             if self.options_df['expiry'].dt.tz is not None:
#                 self.options_df['expiry'] = self.options_df['expiry'].dt.tz_localize(None)

#     def _prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
#         df = df.copy()
#         if not isinstance(df.index, pd.DatetimeIndex):
#             date_col = next((c for c in ['date', 'date_time', 'datetime'] if c in df.columns), None)
#             if date_col:
#                 df.set_index(date_col, inplace=True)
#         if df.index.tz is not None:
#             df.index = df.index.tz_localize(None)
#         return df

#     def load_data(self, symbol: str, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
#         start_ts = pd.Timestamp(start_date)
#         end_ts = pd.Timestamp(end_date).normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)

#         spot = self.spot_df[(self.spot_df.index >= start_ts) & (self.spot_df.index <= end_ts)].copy()

#         options = self.options_df[(self.options_df.index >= start_ts) & (self.options_df.index <= end_ts)].copy()
#         if 'symbol' in options.columns:
#             options = options[options['symbol'] == symbol]

#         return {'spot': spot, 'options': options}


# # =============================================================================
# # VWAP INDICATOR - matches bbc8 from live code exactly
# # =============================================================================

# class VWAPIndicator:
#     @staticmethod
#     def calculate(df: pd.DataFrame, sell_threshold: float = 15.0) -> pd.DataFrame:
#         _temp = df.copy()
#         _temp['traded_volume'] = _temp['close'] * _temp['volume']
#         _temp['indicator'] = _temp['traded_volume'].cumsum() / _temp['volume'].cumsum()
#         _temp['diff'] = _temp['close'] - _temp['indicator']
#         _temp['order_side'] = _temp['diff'].apply(lambda x: 'buy' if x > 0 else 'sell')
#         _temp['perc_diff'] = (_temp['diff'].abs() / _temp['indicator'] * 100).round(2)
#         _temp['can_trade'] = _temp.apply(
#             lambda x: x['perc_diff'] <= sell_threshold if x['order_side'] == 'sell' else x['perc_diff'] <= sell_threshold,
#             axis=1)
#         _temp['signal'] = _temp.apply(
#             lambda x: x['order_side'] if x['can_trade'] else np.nan, axis=1)
#         return _temp


# # =============================================================================
# # POSITION
# # =============================================================================

# @dataclass
# class Position:
#     symbol: str
#     strike: float
#     option_type: str
#     expiry: datetime
#     entry_time: datetime
#     entry_price: float
#     quantity: int
#     lot_size: int
#     sl_price: Optional[float] = None
#     sl_limit_price: Optional[float] = None
#     sl2_price: Optional[float] = None
#     sl_order_placed: bool = False
#     sl_hit: bool = False
#     is_hedge: bool = False
#     exit_time: Optional[datetime] = None
#     exit_price: Optional[float] = None
#     exit_reason: Optional[str] = None
#     pnl: float = 0.0

#     @property
#     def is_short(self) -> bool:
#         return self.quantity < 0

#     @property
#     def is_open(self) -> bool:
#         return self.exit_time is None

#     @property
#     def lots(self) -> int:
#         return abs(self.quantity) // self.lot_size


# # =============================================================================
# # BACKTESTER
# # =============================================================================

# class OSTRADBacktester:
#     def __init__(self, params: StrategyParams, data_loader: DataLoader):
#         self.params = params
#         self.data_loader = data_loader
#         self.margin_config = params.margin_config

#         self.positions: Dict[str, List[Position]] = {s: [] for s in params.symbols}
#         self.closed_positions: List[Position] = []
#         self.traded_strikes: Dict[str, Dict[str, bool]] = {s: {} for s in params.symbols}

#         self.spot_data: Dict[str, pd.DataFrame] = {}
#         self.options_data: Dict[str, pd.DataFrame] = {}

#         # Pre-build option lookup index for speed
#         self._option_index: Dict[str, pd.DataFrame] = {}

#         self.trade_log: List[Dict] = []
#         self.is_first_hedge: Dict[str, bool] = {s: True for s in params.symbols}
#         self.hedge_strikes: Dict[str, List[str]] = {s: [] for s in params.symbols}

#     # -------------------------------------------------------------------------
#     # DATA LOADING
#     # -------------------------------------------------------------------------

#     def load_market_data(self, symbol: str, start_date: datetime, end_date: datetime) -> None:
#         data = self.data_loader.load_data(symbol, start_date, end_date)
#         self.spot_data[symbol] = data['spot']
#         self.options_data[symbol] = data['options']
#         self._build_option_index(symbol)

#         print(f"  {symbol}: {len(self.spot_data[symbol])} spot candles, "
#               f"{len(self.options_data[symbol])} option records")
#         if not self.options_data[symbol].empty:
#             expiries = self.options_data[symbol]['expiry'].unique()
#             print(f"  Expiries: {[str(e)[:10] for e in sorted(expiries)]}")

#     def _build_option_index(self, symbol: str) -> None:
#         """Pre-index options data for fast lookup"""
#         df = self.options_data[symbol].copy()
#         if df.empty:
#             return
#         df['_date'] = df.index.date
#         df['_expiry_date'] = df['expiry'].dt.date
#         self._option_index[symbol] = df

#     # -------------------------------------------------------------------------
#     # PRICE HELPERS
#     # -------------------------------------------------------------------------

#     def get_spot_price(self, symbol: str, timestamp: datetime) -> float:
#         df = self.spot_data[symbol]
#         data = df[df.index <= timestamp]
#         return float(data['close'].iloc[-1]) if not data.empty else 0.0

#     def get_atm_strike(self, symbol: str, timestamp: datetime) -> float:
#         spot = self.get_spot_price(symbol, timestamp)
#         if spot == 0:
#             return 0
#         strike_diff = self.params.strike_diffs.get(symbol, 50)
#         return round(spot / strike_diff) * strike_diff

#     def get_option_price(self, symbol: str, strike: float, option_type: str,
#                          expiry: datetime, timestamp: datetime,
#                          price_type: str = 'close') -> float:
#         df = self._option_index.get(symbol)
#         if df is None or df.empty:
#             return 0.0
#         expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
#         mask = (
#             (df['strike'] == strike) &
#             (df['option_type'] == option_type) &
#             (df['_expiry_date'] == expiry_date) &
#             (df.index <= timestamp)
#         )
#         filtered = df[mask]
#         return float(filtered[price_type].iloc[-1]) if not filtered.empty else 0.0

#     def get_option_candle_high(self, symbol: str, strike: float, option_type: str,
#                                expiry: datetime, current_date) -> float:
#         """Get the max high of the day for SL modification"""
#         df = self._option_index.get(symbol)
#         if df is None or df.empty:
#             return 0.0
#         expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
#         mask = (
#             (df['strike'] == strike) &
#             (df['option_type'] == option_type) &
#             (df['_expiry_date'] == expiry_date) &
#             (df['_date'] == current_date)
#         )
#         filtered = df[mask]
#         return float(filtered['high'].max()) if not filtered.empty else 0.0

#     def get_nearest_expiry(self, symbol: str, timestamp: datetime) -> Optional[datetime]:
#         df = self.options_data[symbol]
#         today = pd.Timestamp(timestamp).normalize()
#         expiries = df[df['expiry'] >= today]['expiry'].unique()
#         return min(expiries) if len(expiries) > 0 else None

#     # -------------------------------------------------------------------------
#     # DAYS TO EXPIRY - matches live code (excludes weekends + holidays)
#     # -------------------------------------------------------------------------

#     def get_adjusted_dte(self, expiry, current_date) -> int:
#         expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
#         raw_days = (expiry_date - current_date).days
#         adjusted = raw_days
#         for i in range(raw_days):
#             check_day = current_date + timedelta(days=i)
#             if check_day.weekday() in [5, 6] or check_day in self.params.holidays:
#                 adjusted -= 1
#         return adjusted

#     # -------------------------------------------------------------------------
#     # MARGIN
#     # -------------------------------------------------------------------------

#     def calculate_margin(self, symbol: str, timestamp: datetime) -> Dict:
#         total_margin = 0.0
#         spot_price = self.get_spot_price(symbol, timestamp)
#         lot_size = self.params.lot_sizes.get(symbol, 75)
#         capital = self.params.capital_per_symbol.get(symbol, 10_000_000)

#         for pos in self.positions[symbol]:
#             if not pos.is_open:
#                 continue
#             opt_price = self.get_option_price(
#                 symbol, pos.strike, pos.option_type, pos.expiry, timestamp)
#             margin = self.margin_config.calculate_span_margin(
#                 spot_price=spot_price, lot_size=lot_size,
#                 option_price=opt_price, quantity=pos.quantity,
#                 is_short=pos.is_short)
#             total_margin += margin

#         margin_percent = (total_margin / capital) * 100 if capital > 0 else 0
#         return {'total_margin': total_margin, 'margin_percent': margin_percent}

#     # -------------------------------------------------------------------------
#     # VWAP SIGNAL
#     # -------------------------------------------------------------------------

#     def get_straddle_vwap_signal(self, symbol: str, strike: float,
#                                   expiry: datetime, timestamp: datetime,
#                                   current_date) -> Optional[Dict]:
#         df = self._option_index.get(symbol)
#         if df is None or df.empty:
#             return None

#         expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry
#         day_start = pd.Timestamp(datetime.combine(current_date,
#                                   datetime.strptime("09:15:00", "%H:%M:%S").time()))

#         base_mask = (
#             (df['strike'] == strike) &
#             (df['_expiry_date'] == expiry_date) &
#             (df.index >= day_start) &
#             (df.index <= timestamp)
#         )

#         ce_data = df[base_mask & (df['option_type'] == 'CE')][['close', 'volume']].copy()
#         pe_data = df[base_mask & (df['option_type'] == 'PE')][['close', 'volume']].copy()

#         if ce_data.empty or pe_data.empty or len(ce_data) < 2 or len(pe_data) < 2:
#             return None

#         merged = ce_data.join(pe_data, how='inner', lsuffix='_ce', rsuffix='_pe')
#         if len(merged) < 2:
#             return None

#         straddle = pd.DataFrame(index=merged.index)
#         straddle['close'] = merged['close_ce'] + merged['close_pe']
#         straddle['volume'] = merged[['volume_ce', 'volume_pe']].min(axis=1)

#         result = VWAPIndicator.calculate(
#             straddle.reset_index().rename(columns={straddle.index.name or 'index': 'date'}),
#             sell_threshold=self.params.threshold_percentage)

#         if 'date' in result.columns:
#             result.set_index('date', inplace=True)

#         if len(result) < 2:
#             return None

#         # Use iloc[-2] — previous candle, matches live code
#         prev = result.iloc[-2]
#         ce_prev_close = merged.iloc[-2]['close_ce']
#         pe_prev_close = merged.iloc[-2]['close_pe']

#         return {
#             'signal': prev['signal'],
#             'can_trade': prev['can_trade'],
#             'indicator': prev['indicator'],
#             'close': prev['close'],
#             'perc_diff': prev['perc_diff'],
#             'ce_close': ce_prev_close,
#             'pe_close': pe_prev_close
#         }

#     # -------------------------------------------------------------------------
#     # OPEN POSITION HELPERS
#     # -------------------------------------------------------------------------

#     def get_open_shorts(self, symbol: str) -> List[Position]:
#         return [p for p in self.positions[symbol] if p.is_open and p.is_short and not p.is_hedge]

#     def get_open_hedges(self, symbol: str) -> List[Position]:
#         return [p for p in self.positions[symbol] if p.is_open and p.is_hedge]

#     # -------------------------------------------------------------------------
#     # SIGNAL CHECK
#     # -------------------------------------------------------------------------

#     def check_signals(self, symbol: str, timestamp: datetime, current_date) -> List[Dict]:
#         signals = []
#         atm = self.get_atm_strike(symbol, timestamp)
#         if atm == 0:
#             return signals

#         expiry = self.get_nearest_expiry(symbol, timestamp)
#         if expiry is None:
#             return signals

#         dte = self.get_adjusted_dte(expiry, current_date)
#         if dte not in self.params.active_days_to_expiry:
#             return signals

#         spot_price = self.get_spot_price(symbol, timestamp)
#         min_premium = spot_price * (self.params.min_premium_percent / 100)
#         strike_diff = self.params.strike_diffs.get(symbol, 50)
#         num_strikes = self.params.num_strikes.get(symbol, 3)
#         expiry_date = expiry.date() if hasattr(expiry, 'date') else expiry

#         for offset in range(-num_strikes, num_strikes + 1):
#             strike = atm + (offset * strike_diff)
#             vwap = self.get_straddle_vwap_signal(symbol, strike, expiry, timestamp, current_date)

#             if vwap is None or vwap['signal'] != 'sell' or not vwap['can_trade']:
#                 continue

#             for opt_type, opt_price in [('CE', vwap['ce_close']), ('PE', vwap['pe_close'])]:
#                 key = f"{strike}_{opt_type}_{expiry_date}"
#                 if self.traded_strikes[symbol].get(key):
#                     continue
#                 if opt_price > min_premium:
#                     signals.append({
#                         'strike': strike,
#                         'option_type': opt_type,
#                         'expiry': expiry,
#                         'entry_price': opt_price,
#                         'timestamp': timestamp
#                     })

#         return signals

#     # -------------------------------------------------------------------------
#     # TAKE POSITION
#     # -------------------------------------------------------------------------

#     def take_position(self, symbol: str, signal: Dict, timestamp: datetime) -> Optional[Position]:
#         margin_info = self.calculate_margin(symbol, timestamp)
#         if margin_info['margin_percent'] >= self.params.new_position_margin_limit:
#             return None

#         open_shorts = self.get_open_shorts(symbol)
#         ce_count = sum(1 for p in open_shorts if p.option_type == 'CE')
#         pe_count = sum(1 for p in open_shorts if p.option_type == 'PE')

#         if signal['option_type'] == 'CE' and ce_count >= self.params.max_open_strikes_per_leg:
#             return None
#         if signal['option_type'] == 'PE' and pe_count >= self.params.max_open_strikes_per_leg:
#             return None

#         entry_price = self.get_option_price(
#             symbol, signal['strike'], signal['option_type'],
#             signal['expiry'], timestamp, price_type='close')
#         if entry_price <= 0:
#             return None

#         lot_size = self.params.lot_sizes.get(symbol, 75)
#         quantity = -self.params.quantity_per_strike.get(symbol, lot_size)

#         sl_perc = self.params.sl_percentage.get(symbol, 20.0)
#         sl_price = entry_price * (1 + sl_perc / 100)
#         tick_size = 0.05
#         sl_price = round(sl_price / tick_size) * tick_size
#         sl_limit_price = sl_price * (1 + self.params.sl_diff_percentage / 100)
#         sl_limit_price = round(sl_limit_price / tick_size) * tick_size

#         pos = Position(
#             symbol=symbol,
#             strike=signal['strike'],
#             option_type=signal['option_type'],
#             expiry=signal['expiry'],
#             entry_time=timestamp,
#             entry_price=entry_price,
#             quantity=quantity,
#             lot_size=lot_size,
#             sl_price=round(sl_price, 2),
#             sl_limit_price=round(sl_limit_price, 2),
#             sl_order_placed=True
#         )

#         self.positions[symbol].append(pos)
#         expiry_date = signal['expiry'].date() if hasattr(signal['expiry'], 'date') else signal['expiry']
#         self.traded_strikes[symbol][f"{signal['strike']}_{signal['option_type']}_{expiry_date}"] = True

#         self._log(timestamp, symbol, signal['strike'], signal['option_type'],
#                   'ENTRY', f"Qty:{quantity} EP:{entry_price:.2f} SL:{sl_price:.2f}")
#         return pos

#     # -------------------------------------------------------------------------
#     # HEDGE
#     # -------------------------------------------------------------------------

#     def check_and_take_hedge(self, symbol: str, timestamp: datetime) -> List[Position]:
#         hedges = []
#         margin_info = self.calculate_margin(symbol, timestamp)
#         if margin_info['margin_percent'] < self.params.hedge_margin_limit:
#             return hedges

#         open_shorts = self.get_open_shorts(symbol)
#         if not open_shorts:
#             return hedges

#         spot_price = self.get_spot_price(symbol, timestamp)
#         strike_diff = self.params.strike_diffs.get(symbol, 50)
#         lot_size = self.params.lot_sizes.get(symbol, 75)
#         expiry = self.get_nearest_expiry(symbol, timestamp)
#         if expiry is None:
#             return hedges

#         hedge_pct = (self.params.hedge_strike_diff_percent if self.is_first_hedge[symbol]
#                      else self.params.hedge_strike_diff_percent_2)

#         existing_hedges = self.get_open_hedges(symbol)
#         ce_short_qty = sum(abs(p.quantity) for p in open_shorts if p.option_type == 'CE')
#         pe_short_qty = sum(abs(p.quantity) for p in open_shorts if p.option_type == 'PE')
#         ce_hedge_qty = sum(p.quantity for p in existing_hedges if p.option_type == 'CE')
#         pe_hedge_qty = sum(p.quantity for p in existing_hedges if p.option_type == 'PE')

#         for opt_type, short_qty, hedge_qty, direction in [
#             ('CE', ce_short_qty, ce_hedge_qty, 1),
#             ('PE', pe_short_qty, pe_hedge_qty, -1)
#         ]:
#             if short_qty == 0 or hedge_qty >= short_qty // 2:
#                 continue

#             hedge_strike = round(spot_price * (1 + direction * hedge_pct / 100) / strike_diff) * strike_diff
#             hedge_qty_to_take = max(lot_size, round(short_qty / lot_size / 2) * lot_size)

#             entry_price = self.get_option_price(symbol, hedge_strike, opt_type, expiry, timestamp)
#             if entry_price <= 0:
#                 continue

#             hedge_pos = Position(
#                 symbol=symbol, strike=hedge_strike, option_type=opt_type,
#                 expiry=expiry, entry_time=timestamp, entry_price=entry_price,
#                 quantity=int(hedge_qty_to_take), lot_size=lot_size, is_hedge=True
#             )
#             self.positions[symbol].append(hedge_pos)
#             self.hedge_strikes[symbol].append(str(hedge_strike))
#             hedges.append(hedge_pos)
#             self._log(timestamp, symbol, hedge_strike, opt_type,
#                       'HEDGE_ENTRY', f"BUY qty:{hedge_qty_to_take} EP:{entry_price:.2f}")

#         if hedges:
#             self.is_first_hedge[symbol] = False

#         return hedges

#     # -------------------------------------------------------------------------
#     # SL HIT CHECK - uses candle HIGH, not close
#     # -------------------------------------------------------------------------

#     def check_sl_hits(self, symbol: str, timestamp: datetime) -> List[Position]:
#         hit_positions = []

#         for pos in self.positions[symbol]:
#             if not pos.is_open or not pos.is_short or pos.sl_hit or pos.sl_price is None:
#                 continue

#             # Use candle HIGH to trigger SL - matches real execution
#             candle_high = self.get_option_price(
#                 symbol, pos.strike, pos.option_type, pos.expiry, timestamp, price_type='high')
#             if candle_high <= 0:
#                 continue

#             if candle_high >= pos.sl_price:
#                 pos.sl_hit = True
#                 pos.exit_time = timestamp
#                 # Exit at SL limit price (capped), matches live SL order behavior
#                 candle_close = self.get_option_price(
#                     symbol, pos.strike, pos.option_type, pos.expiry, timestamp, price_type='close')
#                 pos.exit_price = min(
#                     pos.sl_limit_price if pos.sl_limit_price else candle_high,
#                     candle_high)
#                 pos.exit_reason = 'SL2_HIT' if pos.sl2_price else 'SL_HIT'
#                 pos.pnl = (pos.entry_price - pos.exit_price) * abs(pos.quantity)
#                 self.closed_positions.append(pos)
#                 hit_positions.append(pos)
#                 self._log(timestamp, symbol, pos.strike, pos.option_type,
#                           pos.exit_reason, f"Exit:{pos.exit_price:.2f} PnL:{pos.pnl:.2f}")

#         return hit_positions

#     # -------------------------------------------------------------------------
#     # SL MODIFICATION - matches live sl_modification_timestamp block
#     # -------------------------------------------------------------------------

#     def modify_sl_to_high(self, symbol: str, timestamp: datetime, current_date) -> List[Position]:
#         modified = []

#         for pos in self.positions[symbol]:
#             if not pos.is_open or not pos.is_short or pos.sl_hit or pos.sl2_price is not None:
#                 continue

#             day_high = self.get_option_candle_high(
#                 symbol, pos.strike, pos.option_type, pos.expiry, current_date)
#             if day_high <= 0:
#                 continue

#             if pos.sl_price and day_high < pos.sl_price:
#                 tick_size = 0.05
#                 new_sl = round((day_high + 1) / tick_size) * tick_size
#                 new_sl = round(new_sl, 2)
#                 new_limit = round(new_sl * (1 + self.params.sl_diff_percentage / 100) / tick_size) * tick_size
#                 new_limit = round(new_limit, 2)

#                 pos.sl2_price = new_sl
#                 pos.sl_price = new_sl
#                 pos.sl_limit_price = new_limit
#                 modified.append(pos)
#                 self._log(timestamp, symbol, pos.strike, pos.option_type,
#                           'SL_MODIFIED', f"NewSL:{new_sl:.2f} Limit:{new_limit:.2f}")

#         return modified

#     # -------------------------------------------------------------------------
#     # RMS CHECK
#     # -------------------------------------------------------------------------

#     def run_rms_checks(self, symbol: str, timestamp: datetime) -> List[Position]:
#         rms_exits = []
#         atm = self.get_atm_strike(symbol, timestamp)
#         strike_diff = self.params.strike_diffs.get(symbol, 50)
#         num_strikes = self.params.num_strikes.get(symbol, 3)
#         min_strike = atm - (num_strikes * strike_diff)
#         max_strike = atm + (num_strikes * strike_diff)

#         for pos in self.positions[symbol]:
#             if not pos.is_open or pos.is_hedge:
#                 continue

#             exit_reason = None
#             if pos.quantity > 0:
#                 exit_reason = 'RMS_POSITIVE_QTY'
#             elif pos.strike < min_strike or pos.strike > max_strike:
#                 exit_reason = 'RMS_INVALID_STRIKE'
#             elif pos.sl_hit and pos.exit_time is None:
#                 exit_reason = 'RMS_SL_HIT'

#             if exit_reason:
#                 exit_price = self.get_option_price(
#                     symbol, pos.strike, pos.option_type, pos.expiry, timestamp)
#                 pos.exit_time = timestamp
#                 pos.exit_price = exit_price
#                 pos.exit_reason = exit_reason
#                 pos.pnl = ((pos.entry_price - pos.exit_price) * abs(pos.quantity)
#                            if pos.is_short else
#                            (pos.exit_price - pos.entry_price) * pos.quantity)
#                 self.closed_positions.append(pos)
#                 rms_exits.append(pos)
#                 self._log(timestamp, symbol, pos.strike, pos.option_type,
#                           exit_reason, f"Exit:{exit_price:.2f}")

#         return rms_exits

#     # -------------------------------------------------------------------------
#     # EOD SQUARE OFF
#     # -------------------------------------------------------------------------

#     def square_off_all(self, symbol: str, timestamp: datetime) -> List[Position]:
#         squared = []

#         # Short positions first, then hedges (matches live code order)
#         for is_short_first in [True, False]:
#             for pos in self.positions[symbol]:
#                 if not pos.is_open:
#                     continue
#                 if is_short_first and not pos.is_short:
#                     continue
#                 if not is_short_first and pos.is_short:
#                     continue

#                 exit_price = self.get_option_price(
#                     symbol, pos.strike, pos.option_type, pos.expiry, timestamp)
#                 pos.exit_time = timestamp
#                 pos.exit_price = exit_price
#                 pos.exit_reason = 'EOD_SQUAREOFF'
#                 pos.pnl = ((pos.entry_price - exit_price) * abs(pos.quantity)
#                            if pos.is_short else
#                            (exit_price - pos.entry_price) * pos.quantity)
#                 self.closed_positions.append(pos)
#                 squared.append(pos)
#                 self._log(timestamp, symbol, pos.strike, pos.option_type,
#                           'EOD_SQUAREOFF', f"Exit:{exit_price:.2f} PnL:{pos.pnl:.2f}")

#         return squared

#     # -------------------------------------------------------------------------
#     # LOG
#     # -------------------------------------------------------------------------

#     def _log(self, timestamp: datetime, symbol: str, strike: float,
#              option_type: str, event: str, message: str) -> None:
#         self.trade_log.append({
#             'timestamp': timestamp,
#             'symbol': symbol,
#             'strike': strike,
#             'option_type': option_type,
#             'event': event,
#             'message': message
#         })

#     # -------------------------------------------------------------------------
#     # RUN BACKTEST
#     # -------------------------------------------------------------------------

#     def run_backtest(self, start_date: datetime, end_date: datetime) -> Dict:
#         print("=" * 60)
#         print("OSTRAD BACKTEST")
#         print(f"Period: {start_date.date()} to {end_date.date()}")
#         print(f"Symbols: {self.params.symbols}")
#         print("=" * 60)

#         all_trades = []
#         daily_pnl = {}

#         for symbol in self.params.symbols:
#             print(f"\nLoading {symbol}...")
#             self.load_market_data(symbol, start_date, end_date)

#             symbol_trades, symbol_daily_pnl = self._run_symbol_backtest(symbol, start_date, end_date)
#             all_trades.extend(symbol_trades)
#             for d, pnl in symbol_daily_pnl.items():
#                 daily_pnl[d] = daily_pnl.get(d, 0) + pnl

#         results = {
#             'trades': all_trades,
#             'daily_pnl': daily_pnl,
#             'statistics': self._calculate_statistics(all_trades, daily_pnl)
#         }
#         return results

#     def _run_symbol_backtest(self, symbol: str, start_date: datetime,
#                              end_date: datetime):
#         trades = []
#         daily_pnl = {}

#         current_date = pd.Timestamp(start_date).date()
#         end_date_only = pd.Timestamp(end_date).date()

#         while current_date <= end_date_only:
#             if current_date.weekday() >= 5:
#                 current_date += timedelta(days=1)
#                 continue

#             # Daily reset
#             self.traded_strikes[symbol] = {}
#             self.positions[symbol] = []
#             self.is_first_hedge[symbol] = True
#             self.hedge_strikes[symbol] = []
#             day_pnl = 0.0

#             day_start = datetime.combine(current_date, datetime.strptime(self.params.start_time, "%H:%M:%S").time())
#             entry_end = datetime.combine(current_date, datetime.strptime(self.params.entry_end_time, "%H:%M:%S").time())
#             sl_mod_time = datetime.combine(current_date, datetime.strptime(self.params.sl_modification_time, "%H:%M:%S").time())
#             eod_time = datetime.combine(current_date, datetime.strptime(self.params.square_off_time, "%H:%M:%S").time())

#             timestamps = pd.date_range(
#                 start=day_start, end=eod_time,
#                 freq=f'{self.params.time_frame}min')

#             sl_modified_today = False

#             for timestamp in timestamps:
#                 spot_check = self.spot_data[symbol]
#                 if spot_check[spot_check.index <= timestamp].empty:
#                     continue

#                 # RMS
#                 for pos in self.run_rms_checks(symbol, timestamp):
#                     day_pnl += pos.pnl
#                     trades.append(self._pos_to_dict(pos))

#                 # Hedge
#                 for pos in self.check_and_take_hedge(symbol, timestamp):
#                     trades.append(self._pos_to_dict(pos))

#                 # Entry signals
#                 if timestamp <= entry_end:
#                     for signal in self.check_signals(symbol, timestamp, current_date):
#                         pos = self.take_position(symbol, signal, timestamp)
#                         if pos:
#                             trades.append(self._pos_to_dict(pos))

#                 # SL Modification (once per day)
#                 if timestamp >= sl_mod_time and not sl_modified_today:
#                     mods = self.modify_sl_to_high(symbol, timestamp, current_date)
#                     if mods:
#                         sl_modified_today = True
#                         for m in mods:
#                             trades.append(self._pos_to_dict(m))

#                 # SL Hits
#                 for pos in self.check_sl_hits(symbol, timestamp):
#                     day_pnl += pos.pnl
#                     trades.append(self._pos_to_dict(pos))

#             # EOD square off
#             for pos in self.square_off_all(symbol, eod_time):
#                 day_pnl += pos.pnl
#                 trades.append(self._pos_to_dict(pos))

#             daily_pnl[current_date] = day_pnl
#             print(f"  {current_date}: P&L = ₹{day_pnl:,.2f}")

#             current_date += timedelta(days=1)

#         return trades, daily_pnl

#     def _pos_to_dict(self, pos: Position) -> Dict:
#         return {
#             'symbol': pos.symbol,
#             'strike': pos.strike,
#             'option_type': pos.option_type,
#             'expiry': pos.expiry,
#             'entry_time': pos.entry_time,
#             'entry_price': pos.entry_price,
#             'quantity': pos.quantity,
#             'lots': pos.lots,
#             'sl_price': pos.sl_price,
#             'sl2_price': pos.sl2_price,
#             'is_hedge': pos.is_hedge,
#             'exit_time': pos.exit_time,
#             'exit_price': pos.exit_price,
#             'exit_reason': pos.exit_reason,
#             'pnl': pos.pnl
#         }

#     # -------------------------------------------------------------------------
#     # STATISTICS - clean, no day-level stats when running single day
#     # -------------------------------------------------------------------------

#     def _calculate_statistics(self, trades: List[Dict], daily_pnl: Dict) -> Dict:
#         if not trades:
#             return {'error': 'No trades executed'}

#         df = pd.DataFrame(trades)
#         completed = df[df['exit_time'].notna()].copy()
#         if completed.empty:
#             return {'error': 'No completed trades'}

#         total_pnl = completed['pnl'].sum()
#         winning = completed[completed['pnl'] > 0]
#         losing = completed[completed['pnl'] < 0]

#         stats = {
#             'total_trades': len(completed),
#             'total_pnl': round(total_pnl, 2),
#             'winning_trades': len(winning),
#             'losing_trades': len(losing),
#             'win_rate': round(len(winning) / len(completed) * 100, 1) if completed.shape[0] > 0 else 0,
#             'avg_pnl_per_trade': round(completed['pnl'].mean(), 2),
#             'max_profit_trade': round(completed['pnl'].max(), 2),
#             'max_loss_trade': round(completed['pnl'].min(), 2),
#             'avg_winner': round(winning['pnl'].mean(), 2) if len(winning) > 0 else 0,
#             'avg_loser': round(losing['pnl'].mean(), 2) if len(losing) > 0 else 0,
#             'profit_factor': round(abs(winning['pnl'].sum() / losing['pnl'].sum()), 2)
#                              if losing['pnl'].sum() != 0 else float('inf'),
#         }

#         # Exit reason breakdown
#         for reason in ['SL_HIT', 'SL2_HIT', 'EOD_SQUAREOFF', 'RMS_POSITIVE_QTY', 'RMS_INVALID_STRIKE']:
#             count = len(completed[completed['exit_reason'] == reason])
#             stats[f'{reason.lower()}_count'] = count

#         # Only add day-level stats if more than 1 day
#         if len(daily_pnl) > 1:
#             pnl_series = pd.Series(daily_pnl)
#             stats['trading_days'] = len(pnl_series)
#             stats['profitable_days'] = int((pnl_series > 0).sum())
#             stats['losing_days'] = int((pnl_series < 0).sum())
#             stats['best_day'] = round(pnl_series.max(), 2)
#             stats['worst_day'] = round(pnl_series.min(), 2)
#             stats['avg_daily_pnl'] = round(pnl_series.mean(), 2)
#             stats['sharpe_ratio'] = round(
#                 (pnl_series.mean() / pnl_series.std()) * np.sqrt(252), 2
#             ) if pnl_series.std() != 0 else 0

#         return stats

#     # -------------------------------------------------------------------------
#     # PRINT RESULTS
#     # -------------------------------------------------------------------------

#     def print_results(self, results: Dict) -> None:
#         print("\n" + "=" * 50)
#         print("BACKTEST RESULTS")
#         print("=" * 50)
#         stats = results['statistics']

#         if 'error' in stats:
#             print(f"Error: {stats['error']}")
#             return

#         print(f"Total Trades:      {stats['total_trades']}")
#         print(f"Total P&L:         ₹{stats['total_pnl']:,.2f}")
#         print(f"Win Rate:          {stats['win_rate']}%")
#         print(f"Profit Factor:     {stats['profit_factor']}")
#         print(f"Winning Trades:    {stats['winning_trades']}")
#         print(f"Losing Trades:     {stats['losing_trades']}")
#         print(f"Max Profit Trade:  ₹{stats['max_profit_trade']:,.2f}")
#         print(f"Max Loss Trade:    ₹{stats['max_loss_trade']:,.2f}")
#         print(f"Avg Winner:        ₹{stats['avg_winner']:,.2f}")
#         print(f"Avg Loser:         ₹{stats['avg_loser']:,.2f}")

#         print(f"\nExit Breakdown:")
#         print(f"  SL Hit:          {stats.get('sl_hit_count', 0)}")
#         print(f"  SL2 Hit:         {stats.get('sl2_hit_count', 0)}")
#         print(f"  EOD Squareoff:   {stats.get('eod_squareoff_count', 0)}")
#         print(f"  RMS Exits:       {stats.get('rms_positive_qty_count', 0) + stats.get('rms_invalid_strike_count', 0)}")



#     def print_trade_log(self, limit: int = 50) -> None:
#         print("\n" + "=" * 70)
#         print("TRADE LOG")
#         print("=" * 70)
#         for entry in self.trade_log[:limit]:
#             ts = entry['timestamp'].strftime('%Y-%m-%d %H:%M') if entry['timestamp'] else 'N/A'
#             print(f"{ts} | {entry['symbol']} {entry['strike']}{entry['option_type']} | "
#                   f"{entry['event']}: {entry['message']}")
#         if len(self.trade_log) > limit:
#             print(f"... and {len(self.trade_log) - limit} more entries")

"""
OSTRAD Backtesting Engine - Modular Version
"""

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


# =============================================================================
# SETUP LOGGING
# =============================================================================

def setup_logging(log_file: str = 'ostrad_backtest.log'):
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger('OSTRAD')


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class MarginConfig:
    var_margin_percent: float = 12.0
    elm_percent: float = 3.0
    span_multiplier: float = 1.0
    min_margin_per_lot: float = 50000

    def calculate_span_margin(self, spot_price: float, lot_size: int,
                              option_price: float, quantity: int,
                              is_short: bool = True) -> float:
        if is_short:
            base_margin = spot_price * lot_size * (self.var_margin_percent / 100)
            elm_margin = spot_price * lot_size * (self.elm_percent / 100)
            total_base = (base_margin + elm_margin) * abs(quantity) / lot_size
            total_base *= self.span_multiplier
            premium_received = option_price * abs(quantity)
            margin = max(total_base - premium_received,
                        self.min_margin_per_lot * abs(quantity) / lot_size)
        else:
            margin = option_price * abs(quantity)
        return max(margin, 0)


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
# DATA LOADER
# =============================================================================

class DataLoader(ABC):
    @abstractmethod
    def load_data(self, symbol: str, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        pass


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
        
        if self.spot_df.index.tz is not None:
            self.spot_df.index = self.spot_df.index.tz_localize(None)
        
        self.spot_df = self.spot_df[['symbol', 'open', 'high', 'low', 'close', 'volume']].copy()
        self.spot_df['volume'] = self.spot_df['volume'].fillna(100000)
        
        # Load options data
        self.options_df = pd.read_csv(self.options_file, parse_dates=['date_time', 'expiry_date'])
        self.options_df.set_index('date_time', inplace=True)
        self.options_df = self.options_df.rename(columns={'expiry_date': 'expiry'})
        
        if self.options_df.index.tz is not None:
            self.options_df.index = self.options_df.index.tz_localize(None)
        
        if self.options_df['expiry'].dt.tz is not None:
            self.options_df['expiry'] = self.options_df['expiry'].dt.tz_localize(None)
        
        self.options_df = self.options_df[['symbol', 'strike', 'option_type', 'expiry',
                                            'open', 'high', 'low', 'close', 'volume', 'oi']].copy()
        
        self.logger.info(f"Loaded {len(self.spot_df)} spot records, {len(self.options_df)} option records")

    def load_data(self, symbol: str, start_date: datetime, end_date: datetime) -> Dict[str, pd.DataFrame]:
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date).normalize() + pd.Timedelta(hours=23, minutes=59, seconds=59)
        
        spot = self.spot_df[(self.spot_df.index >= start_ts) & (self.spot_df.index <= end_ts)].copy()
        options = self.options_df[(self.options_df.index >= start_ts) & (self.options_df.index <= end_ts)].copy()
        
        if 'symbol' in options.columns:
            options = options[options['symbol'] == symbol]
        
        return {'spot': spot, 'options': options}


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
# MAIN BACKTESTER ENGINE
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
        self.traded_strikes: Dict[str, bool] = {}
        
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
            opt_price = self.get_option_price(pos.strike, pos.option_type, pos.expiry, timestamp)
            margin = self.margin_config.calculate_span_margin(
                spot_price=spot_price, lot_size=lot_size,
                option_price=opt_price, quantity=pos.quantity,
                is_short=pos.is_short)
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
    # SIGNAL CHECK
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
                key = f"{strike}_{opt_type}_{expiry_date}"
                if self.traded_strikes.get(key):
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
                candle_close = self.get_option_price(pos.strike, pos.option_type, pos.expiry, timestamp, price_type='close')
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
    # RMS CHECK - Fixed logic
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
            
            # Positive quantity check (should be short only)
            if pos.quantity > 0:
                exit_reason = 'RMS_POSITIVE_QTY'
            
            # Invalid strike check - strike outside active range
            elif pos.strike < min_strike or pos.strike > max_strike:
                exit_reason = 'RMS_INVALID_STRIKE'
            
            # SL hit but position still open
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
    # EOD SQUARE OFF
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

        for timestamp in timestamps:
            # Skip if no spot data yet
            if self.spot_data[self.spot_data.index <= timestamp].empty:
                continue

            # RMS checks
            for pos in self.run_rms_checks(timestamp, current_date):
                day_pnl += pos.pnl

            # Hedge check
            for pos in self.check_and_take_hedge(timestamp):
                pass  # Hedge positions tracked in list

            # Entry signals
            if timestamp <= entry_end:
                for signal in self.check_signals(timestamp, current_date):
                    pos = self.take_position(signal, timestamp)
                    if pos:
                        trades_today += 1

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
    # RUN FULL BACKTEST
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
            
            # Collect all trades from closed positions
            for pos in self.closed_positions:
                all_trades.append(self._pos_to_dict(pos))
            
            current_date += timedelta(days=1)

        # Final statistics
        results = {
            'symbol': self.symbol,
            'daily_stats': self.daily_stats,
            'trades': all_trades,
            'summary': self._calculate_summary()
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

    def _calculate_summary(self) -> Dict:
        if not self.daily_stats:
            return {'error': 'No trading days'}
        
        total_pnl = sum(d['day_pnl'] for d in self.daily_stats)
        total_trades = sum(d['total_trades'] for d in self.daily_stats)
        winning_days = sum(1 for d in self.daily_stats if d['day_pnl'] > 0)
        losing_days = sum(1 for d in self.daily_stats if d['day_pnl'] < 0)
        
        # Calculate from all trades
        all_pnls = [t['pnl'] for t in self.trade_log if 'ENTRY' not in str(t)]
        # Actually get from closed positions data
        trade_pnls = []
        for day in self.daily_stats:
            # We need to recalculate from stored data
            pass
        
        return {
            'total_days': len(self.daily_stats),
            'profitable_days': winning_days,
            'losing_days': losing_days,
            'total_pnl': round(total_pnl, 2),
            'avg_daily_pnl': round(total_pnl / len(self.daily_stats), 2),
            'max_daily_profit': max(d['day_pnl'] for d in self.daily_stats),
            'max_daily_loss': min(d['day_pnl'] for d in self.daily_stats),
            'total_trades': total_trades,
            'avg_trades_per_day': round(total_trades / len(self.daily_stats), 1)
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
        if not trades_df.empty:
            trades_file = os.path.join(output_dir, f"{self.symbol}_trades.csv")
            trades_df.to_csv(trades_file, index=False)
            self.logger.info(f"Trades saved to {trades_file}")
        
        # Save trade log
        log_df = pd.DataFrame(self.trade_log)
        if not log_df.empty:
            log_file = os.path.join(output_dir, f"{self.symbol}_log.csv")
            log_df.to_csv(log_file, index=False)
        
        # Save summary as JSON
        import json
        summary_file = os.path.join(output_dir, f"{self.symbol}_summary.json")
        with open(summary_file, 'w') as f:
            json.dump(results['summary'], f, indent=2, default=str)
        
        return {
            'daily_stats': daily_file,
            'trades': trades_file if not trades_df.empty else None,
            'log': log_file if not log_df.empty else None,
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