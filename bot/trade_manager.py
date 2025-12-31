# bot/trade_manager.py - FIXED VERSION - No double spread calculation
from bot.trade_risk_calculator import get_trade_units
from models.trade_decision import TradeDecision
from constants import defs
from api.oanda_api import oanda_api
from infrastructure.instrument_collection import instrumentCollection as ic


def trade_is_open(pair: str, strategy_id: str = None, return_status: bool = False):
    """Check if there's already an open trade for this pair"""
    open_trades = oanda_api.get_open_trades()

    if open_trades is None:
        return (None, False) if return_status else None

    for ot in open_trades:
        if ot.instrument == pair:
            if strategy_id is not None and hasattr(ot, "strategy_id") and ot.strategy_id != strategy_id:
                continue
            return (ot, True) if return_status else ot

    return (None, True) if return_status else None


def place_trade(
    trade_decision: TradeDecision,
    log_message,
    log_error,
    trade_risk: float,
    return_id: bool = False,
    attach_broker_stops: bool = True,
):
    """
    Place a trade with SIMPLE TP/SL calculation using strategy pip distances.
    When attach_broker_stops=False, TP/SL are only used for position sizing and are
    not sent to OANDA (exits managed in-memory/externally).

    Args:
        trade_decision: TradeDecision object with trade details
        log_message: Function for logging messages
        log_error: Function for logging errors
        trade_risk: Risk amount in account currency

    Returns:
        bool: True if trade was placed successfully, False otherwise
    """

    pair = trade_decision.pair
    signal = trade_decision.signal
    strategy_id = trade_decision.strategy_id

    # Check for existing trades on this pair
    existing_trade = trade_is_open(pair)
    if existing_trade is not None:
        log_message(f"Cannot place trade for {pair} - existing trade found: {existing_trade.id}", pair)
        return False

    # Get current market prices for entry and unit calculation
    try:
        prices = oanda_api.get_prices([pair])
        if not prices or len(prices) == 0:
            log_error(f"Could not get current prices for {pair}")
            return False

        price_obj = prices[0]
        current_bid = price_obj.bid
        current_ask = price_obj.ask
        current_spread = current_ask - current_bid

        log_message(f"Current market: Bid={current_bid:.5f} Ask={current_ask:.5f} Spread={current_spread:.5f}", pair)

    except Exception as e:
        log_error(f"Error getting current prices for {pair}: {e}")
        return False

    # Determine entry price (OANDA will execute at these prices automatically)
    if signal == defs.BUY:
        entry_price = current_ask  # BUY orders execute at ask
    else:
        entry_price = current_bid  # SELL orders execute at bid

    # Get pip value for calculations
    instrument = ic.instruments_dict[pair]
    pip_value = instrument.pipLocation

    # SIMPLE APPROACH: Extract pip distances using the ENTRY price from strategy
    try:
        # Use the entry price that was calculated in the strategy
        if hasattr(trade_decision, "entry") and trade_decision.entry > 0:
            original_entry = float(trade_decision.entry)
            log_message(f"Using strategy entry price: {original_entry:.5f}", pair)
        else:
            # Fallback: reconstruct based on signal direction and mid price
            if signal == defs.BUY:
                original_entry = trade_decision.mid_c + (current_spread / 2)  # Approximate ask
            else:
                original_entry = trade_decision.mid_c - (current_spread / 2)  # Approximate bid
            log_message(f"No entry field, using reconstructed entry: {original_entry:.5f}", pair)

        # Extract pip distances using the original entry price
        if signal == defs.BUY:
            tp_pips = int(round((float(trade_decision.tp) - original_entry) / pip_value))
            sl_pips = int(round((original_entry - float(trade_decision.sl)) / pip_value))
        else:  # SELL
            tp_pips = int(round((original_entry - float(trade_decision.tp)) / pip_value))
            sl_pips = int(round((float(trade_decision.sl) - original_entry) / pip_value))

        log_message(f"Pip distances from entry={original_entry:.5f}: TP={tp_pips} pips, SL={sl_pips} pips", pair)

        # Validate pip distances
        if tp_pips <= 0 or sl_pips <= 0:
            log_error(f"Invalid pip distances: TP={tp_pips}, SL={sl_pips}")
            return False

    except Exception as e:
        log_error(f"Could not extract pip distances: {e}")
        return False

    # Calculate TP/SL levels relative to entry price
    # OANDA will handle the execution at the right bid/ask prices
    if signal == defs.BUY:
        tp_price = entry_price + (tp_pips * pip_value)
        sl_price = entry_price - (sl_pips * pip_value)
    else:  # SELL
        tp_price = entry_price - (tp_pips * pip_value)
        sl_price = entry_price + (sl_pips * pip_value)

    log_message(
        f"Trade setup: Entry={entry_price:.5f}, TP={tp_price:.5f} ({tp_pips} pips), SL={sl_price:.5f} ({sl_pips} pips)",
        pair,
    )

    # Calculate trade units using entry price and SL
    try:
        trade_units = get_trade_units(
            pair=pair, signal=signal, entry_price=entry_price, sl_price=sl_price, trade_risk=trade_risk, log_message=log_message
        )

        if trade_units <= 0:
            log_error(f"Invalid trade units calculated: {trade_units}")
            return False

    except Exception as e:
        log_error(f"Error calculating trade units: {e}")
        return False

    # Create client extensions to track strategy
    trade_client_extensions = {"tag": str(strategy_id), "comment": f"Strategy {strategy_id}"}

    # Place the trade - OANDA will handle bid/ask execution automatically
    try:
        # Only attach stops when explicitly allowed; brick-count strategies keep them in memory
        stop_loss_param = sl_price if attach_broker_stops else None
        take_profit_param = tp_price if attach_broker_stops else None

        trade_id = oanda_api.place_trade(
            pair=pair,
            units=trade_units,
            direction=signal,
            stop_loss=stop_loss_param,
            take_profit=take_profit_param,
            trade_client_extensions=trade_client_extensions,
        )

        if trade_id is None:
            log_error(f"Failed to place trade for {pair}")
            return False
        else:
            log_message(f"✅ Trade placed successfully - ID: {trade_id}", pair)
            log_message(f"   Units: {trade_units:.0f}, Entry: ~{entry_price:.5f}", pair)
            if attach_broker_stops:
                log_message(f"   TP: {tp_price:.5f} ({tp_pips} pips), SL: {sl_price:.5f} ({sl_pips} pips)", pair)
            else:
                log_message(
                    f"   TP/SL kept in-memory for brick exits "
                    f"(theoretical TP={tp_price:.5f}, SL={sl_price:.5f})",
                    pair,
                )
            return trade_id if return_id else True

    except Exception as e:
        log_error(f"Exception placing trade: {e}")
        return False


def close_trade_by_pair(pair: str, log_message, log_error) -> bool:
    """Close any open trade for the specified pair"""
    try:
        existing_trade = trade_is_open(pair)
        if existing_trade is None:
            log_message(f"No open trade found for {pair}", pair)
            return True

        success = oanda_api.close_trade(existing_trade.id)
        if success:
            log_message(f"Closed trade {existing_trade.id} for {pair}", pair)
            return True
        else:
            log_error(f"Failed to close trade {existing_trade.id} for {pair}")
            return False

    except Exception as e:
        log_error(f"Exception closing trade for {pair}: {e}")
        return False


def get_open_trades_summary(log_message):
    """Get summary of all open trades for logging/monitoring"""
    try:
        open_trades = oanda_api.get_open_trades()
        if not open_trades:
            log_message("No open trades", "trades_summary")
            return

        for trade in open_trades:
            log_message(
                f"Open: {trade.instrument} - Units: {trade.currentUnits}, "
                f"P&L: {trade.unrealizedPL}, Strategy: {getattr(trade, 'strategy_id', 'unknown')}",
                "trades_summary",
            )

    except Exception as e:
        log_message(f"Error getting trades summary: {e}", "trades_summary")
