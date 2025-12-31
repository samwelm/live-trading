# bot/trade_risk_calculator.py
from api.oanda_api import oanda_api
import constants.defs as defs
from infrastructure.instrument_collection import instrumentCollection as ic
import logging
from typing import Union, Dict, Any
from datetime import datetime, timedelta, timezone
from core.config_models import RiskConfig

# Cache for account NAV to reduce API calls
_nav_cache = {'value': None, 'timestamp': None}
_nav_cache_duration = timedelta(minutes=5)

def calculate_risk_amount(risk_config: Union[float, int, Dict[str, Any], RiskConfig],
                         log_message,
                         max_risk_cap: float = 5000.0,
                         default_fallback: float = 300.0) -> float:
    """
    Calculate actual risk amount from config (supports both percent and fixed)

    Args:
        risk_config: Can be:
            - Numeric (float/int): Treated as fixed dollar amount (backward compatible)
            - Dict with 'type' and 'value':
                - type='fixed': value is dollar amount
                - type='percent': value is percentage (0.01 = 1%)
        log_message: Logging function
        max_risk_cap: Maximum risk amount allowed (safety limit)
        default_fallback: Fallback value if NAV fetch fails

    Returns:
        Risk amount in account currency
    """
    try:
        # Backward compatibility: if just a number, treat as fixed amount
        if isinstance(risk_config, (int, float)):
            risk_amount = float(risk_config)
            log_message(f"Risk: Fixed ${risk_amount:.2f}")
            return min(risk_amount, max_risk_cap)

        if isinstance(risk_config, RiskConfig):
            risk_config = risk_config.to_dict()

        # Handle dict-based config
        if not isinstance(risk_config, dict):
            log_message(f"Invalid risk_config type: {type(risk_config)}, using default ${default_fallback}")
            return default_fallback

        risk_type = risk_config.get('type', 'percent')
        risk_value = risk_config.get('value', 0.01)

        # Fixed dollar amount
        if risk_type == 'fixed':
            risk_amount = float(risk_value)
            log_message(f"Risk: Fixed ${risk_amount:.2f}")
            return min(risk_amount, max_risk_cap)

        # Percentage of NAV
        if risk_type == 'percent':
            nav = _get_cached_nav(log_message)

            if nav is None or nav <= 0:
                log_message(f"⚠️ Could not get valid NAV, using default ${default_fallback}")
                return default_fallback

            risk_amount = nav * float(risk_value)
            risk_amount = min(risk_amount, max_risk_cap)  # Apply cap

            log_message(f"Risk: NAV=${nav:.2f} × {risk_value:.3%} = ${risk_amount:.2f} (capped at ${max_risk_cap})")
            return risk_amount

        # Unknown type
        log_message(f"Unknown risk_type '{risk_type}', using default ${default_fallback}")
        return default_fallback

    except Exception as e:
        log_message(f"⚠️ Error calculating risk amount: {e}, using default ${default_fallback}")
        return default_fallback

def _get_cached_nav(log_message) -> float:
    """
    Get account NAV with 5-minute caching to reduce API calls

    Returns:
        Current NAV or None if unavailable
    """
    global _nav_cache

    now = datetime.now(timezone.utc)

    # Check if cache is valid
    if (_nav_cache['value'] is not None and
        _nav_cache['timestamp'] is not None and
        now - _nav_cache['timestamp'] < _nav_cache_duration):
        log_message(f"Using cached NAV: ${_nav_cache['value']:.2f}")
        return _nav_cache['value']

    # Fetch fresh NAV
    try:
        account = oanda_api.get_account_summary()
        if not account:
            log_message("⚠️ Could not get account summary")
            return None

        nav = float(account.get('NAV', 0))

        if nav <= 0:
            log_message(f"⚠️ Invalid NAV value: {nav}")
            return None

        # Update cache
        _nav_cache['value'] = nav
        _nav_cache['timestamp'] = now

        log_message(f"Fetched fresh NAV: ${nav:.2f}")
        return nav

    except Exception as e:
        log_message(f"⚠️ Error fetching NAV: {e}")
        return None

def get_trade_units(pair: str, signal: int, entry_price: float, sl_price: float,
                   trade_risk: float, log_message) -> float:
    """
    Calculate trade units based on actual entry and stop loss prices WITH margin checking

    Args:
        pair: Currency pair
        signal: BUY or SELL
        entry_price: Actual entry price (ask for buy, bid for sell)
        sl_price: Stop loss price
        trade_risk: Risk amount in account currency
        log_message: Logging function

    Returns:
        Number of units to trade (0 if insufficient margin)
    """

    # Get current prices for conversion factors
    prices = oanda_api.get_prices([pair])
    if prices is None or len(prices) == 0:
        log_message("get_trade_units() Prices is None", pair)
        return 0.0

    price_obj = None
    for p in prices:
        if p.instrument == pair:
            price_obj = p
            break

    if price_obj is None:
        log_message("get_trade_units() price object is None", pair)
        return 0.0

    log_message(f"get_trade_units() price {price_obj}", pair)

    # Get conversion factor based on trade direction
    conv = price_obj.buy_conv if signal == defs.BUY else price_obj.sell_conv

    # Get instrument details
    instrument = ic.instruments_dict[pair]
    pip_location = instrument.pipLocation

    # Calculate the actual risk in price terms
    price_risk = abs(entry_price - sl_price)

    # Calculate risk per unit in account currency
    # For most pairs, this is: price_risk * conv
    risk_per_unit = price_risk * conv

    if risk_per_unit <= 0:
        log_message(f"Invalid risk_per_unit: {risk_per_unit}", pair)
        return 0.0

    # Calculate units needed to risk the specified amount
    units = trade_risk / risk_per_unit

    # MARGIN CHECK: Get available margin and limit position size
    try:
        account = oanda_api.get_account_summary()
        if account:
            margin_available = float(account.get('marginAvailable', 0))
            margin_rate = float(account.get('marginRate', 0.02))  # Default 50:1 leverage = 0.02

            # Calculate margin required for this position
            # Margin = Notional Value * Margin Rate
            # Notional = units * entry_price * conv
            notional_value = units * entry_price * conv
            margin_required = notional_value * margin_rate

            # Reserve 30% of available margin for safety (don't use all margin)
            safe_margin = margin_available * 0.3

            log_message(f"Margin check: Available=${margin_available:.2f}, "
                       f"Required=${margin_required:.2f}, Safe limit=${safe_margin:.2f}", pair)

            # If margin required exceeds safe limit, scale down position
            if margin_required > safe_margin:
                if safe_margin > 0:
                    scale_factor = safe_margin / margin_required
                    units = units * scale_factor
                    log_message(f"⚠️ Position scaled down {scale_factor:.1%} due to margin limits", pair)
                else:
                    log_message(f"❌ Insufficient margin available (${margin_available:.2f})", pair)
                    return 0.0
    except Exception as e:
        log_message(f"⚠️ Could not check margin: {e}", pair)

    # Round to instrument precision
    units = round(units, instrument.tradeUnitsPrecision)

    log_message(f"Entry: {entry_price}, SL: {sl_price}, Price Risk: {price_risk:.5f}, "
               f"Conv: {conv:.5f}, Risk/Unit: {risk_per_unit:.5f}, Units: {units:.1f}", pair)

    return units

def calculate_dynamic_tp_sl(df_row, pair: str, tp_pips: int, sl_pips: int, signal: int):
    """
    Calculate take profit and stop loss using current bid/ask prices
    
    Args:
        df_row: Current candle data with bid/ask prices
        pair: Currency pair
        tp_pips: Take profit in pips
        sl_pips: Stop loss in pips  
        signal: BUY or SELL signal
        
    Returns:
        dict with entry_price, tp_price, sl_price
    """
    instrument = ic.instruments_dict[pair]
    pip_value = instrument.pipLocation
    
    if signal == defs.BUY:
        # Buy at ask price
        entry_price = float(df_row.get('ask_c', df_row.get('mid_c', 0)))
        tp_price = entry_price + (tp_pips * pip_value)
        sl_price = entry_price - (sl_pips * pip_value)
    else:  # SELL
        # Sell at bid price
        entry_price = float(df_row.get('bid_c', df_row.get('mid_c', 0)))
        tp_price = entry_price - (tp_pips * pip_value)
        sl_price = entry_price + (sl_pips * pip_value)
    
    return {
        'entry_price': round(entry_price, instrument.displayPrecision),
        'tp_price': round(tp_price, instrument.displayPrecision),
        'sl_price': round(sl_price, instrument.displayPrecision)
    }

def validate_trade_parameters(pair: str, signal: int, entry_price: float, 
                            tp_price: float, sl_price: float, max_spread: float,
                            current_spread: float, log_message) -> bool:
    """
    Validate trade parameters before placing trade
    
    Returns:
        True if trade parameters are valid, False otherwise
    """
    
    # Check spread constraint
    if current_spread > max_spread:
        log_message(f"Spread too wide: {current_spread:.5f} > {max_spread:.5f}", pair)
        return False
    
    # Check TP/SL direction is correct
    if signal == defs.BUY:
        if tp_price <= entry_price or sl_price >= entry_price:
            log_message(f"Invalid BUY levels: Entry={entry_price}, TP={tp_price}, SL={sl_price}", pair)
            return False
    else:  # SELL
        if tp_price >= entry_price or sl_price <= entry_price:
            log_message(f"Invalid SELL levels: Entry={entry_price}, TP={tp_price}, SL={sl_price}", pair)
            return False
    
    # Check minimum distance (at least 1 pip)
    instrument = ic.instruments_dict[pair]
    min_distance = instrument.pipLocation
    
    if abs(tp_price - entry_price) < min_distance or abs(sl_price - entry_price) < min_distance:
        log_message(f"TP/SL too close to entry: min distance {min_distance}", pair)
        return False
    
    return True
