# bot/candle_manager.py - Updated with better logging and console output

from models.candle_timing import CandleTiming
from datetime import datetime, timezone

class CandleManager:
    """
    Simplified candle manager that tracks one granularity per strategy
    No more separation between signal and trade granularities
    """

    def __init__(self, oanda_api, strategy_repository, log_message):
        self.log_message = log_message
        self.strategy_repository = strategy_repository
        self.oanda_api = oanda_api        
        self.pairs_list = self.get_unique_pairs()

        # Initialize timings dictionary
        self.timings = {}
        print(f"🔧 Initializing candle manager for {len(self.pairs_list)} pair/timeframe combinations...")
        
        for pair_info in self.pairs_list:
            key = self.make_dict_key(pair_info)
            try:
                last_candle = self.oanda_api.last_complete_candle(pair_info['pair'], pair_info['granularity'])
                if last_candle is None:
                    error_msg = f"Warning: Could not get last candle for {pair_info['pair']}_{pair_info['granularity']}"
                    self.log_message(error_msg, "candle_manager")
                    print(f"  ⚠️  {error_msg}")
                    continue

                self.timings[key] = CandleTiming(last_candle)
                success_msg = f"CandleManager() init last_candle: {self.timings[key]}"
                self.log_message(success_msg, f"{pair_info['pair']}_{pair_info['granularity']}")
                print(f"  ✅ {pair_info['pair']} ({pair_info['granularity']}) - Last candle: {last_candle.strftime('%H:%M:%S')}")

            except Exception as e:
                error_msg = f"Error initializing {pair_info['pair']}_{pair_info['granularity']}: {e}"
                self.log_message(error_msg, "candle_manager")
                print(f"  ❌ {error_msg}")

    def make_dict_key(self, pair_info):
        """Convert pair_info dictionary to a hashable tuple key"""
        return (pair_info['pair'], pair_info['granularity'])
        
    def get_unique_pairs(self):
        """
        Get unique pair/granularity combinations from strategies
        Simplified - no more signal/trade separation
        """
        strategies = self.strategy_repository.get_strategies()
        unique_pairs = []
        
        for strategy in strategies:
            pair_info = {
                'pair': strategy["pair"],
                'granularity': strategy["granularity"]
            }
            
            # Add if not already in the list
            if not any(p['pair'] == pair_info['pair'] and 
                      p['granularity'] == pair_info['granularity'] for p in unique_pairs):
                unique_pairs.append(pair_info)
                    
        self.log_message(f"Found {len(unique_pairs)} unique pair/granularity combinations", "candle_manager")
        return unique_pairs

    def update_timings(self):
        """
        Check for new candles and return list of triggered pair/granularity combinations
        
        Returns:
            List of pair_info dicts for combinations that have new candles
        """
        triggered = []
        current_time = datetime.now(timezone.utc)

        for pair_info in self.pairs_list:
            pair = pair_info['pair']
            granularity = pair_info['granularity']
            key = self.make_dict_key(pair_info)
            
            # Skip if key doesn't exist (initialization failed)
            if key not in self.timings:
                continue
                
            try:
                current = self.oanda_api.last_complete_candle(pair, granularity)
                if current is None:
                    error_msg = f"Unable to get current candle for {pair}_{granularity}"
                    self.log_message(error_msg, "candle_manager")
                    # Don't print this every time as it would be too noisy
                    continue
                    
                self.timings[key].is_ready = False
                
                # Check if we have a new candle
                if current > self.timings[key].last_time:
                    self.timings[key].is_ready = True
                    old_time = self.timings[key].last_time
                    self.timings[key].last_time = current

                    # Structured logging: only DEBUG level for routine candle ticks
                    # Use INFO only for significant events or summaries
                    log_msg = f"New candle: {current.strftime('%H:%M:%S')}"
                    # Temporary: print every detected candle for visibility
                    #print(f"🕐 Candle detected {pair}_{granularity} @ {current.strftime('%H:%M:%S')}")

                    # S5 candles: DEBUG level only (too frequent for INFO)
                    if granularity == 'S5':
                        # Count S5 candles, log summary every 60 candles (5 minutes)
                        if not hasattr(self, 's5_candle_count'):
                            self.s5_candle_count = {}

                        candle_key = f"{pair}_S5"
                        self.s5_candle_count[candle_key] = self.s5_candle_count.get(candle_key, 0) + 1

                        # Log summary every 60 S5 candles (5 minutes)
                        if self.s5_candle_count[candle_key] % 60 == 0:
                            self.log_message(
                                f"S5 activity: {self.s5_candle_count[candle_key]} candles processed, last: {current.strftime('%H:%M:%S')}",
                                f"{pair}_S5"
                            )

                    # Other granularities (M1, M5, etc.): Log at INFO level
                    else:
                        self.log_message(log_msg, f"{pair}_{granularity}")
                        # No console output - let RenkoEngine handle that when bricks form

                    triggered.append(pair_info)  # Return the full dict
                    
            except Exception as e:
                error_msg = f"Error updating timing for {pair}_{granularity}: {e}"
                self.log_message(error_msg, "candle_manager")
                print(f"❌ {error_msg}")
                continue
        
        # Only log summary if there are triggered pairs (avoid empty log spam)
        # Don't print to console - let RenkoEngine report when bricks actually form
        if triggered:
            # Group by granularity for cleaner summary
            by_granularity = {}
            for pair_info in triggered:
                gran = pair_info['granularity']
                by_granularity.setdefault(gran, []).append(pair_info['pair'])

            # Log at DEBUG level (too frequent for INFO)
            for gran, pairs in by_granularity.items():
                if gran != 'S5':  # S5 already has periodic summaries
                    self.log_message(
                        f"{gran} candles completed: {', '.join(pairs)}",
                        "candle_manager"
                    )

        return triggered

    def get_timing_info(self, pair: str, granularity: str):
        """
        Get timing information for a specific pair/granularity combination
        
        Returns:
            CandleTiming object or None if not found
        """
        key = (pair, granularity)
        return self.timings.get(key)

    def force_update(self, pair: str, granularity: str):
        """
        Force an update for a specific pair/granularity (useful for testing)
        
        Returns:
            bool: True if update was successful
        """
        key = (pair, granularity)
        if key not in self.timings:
            error_msg = f"Cannot force update - key {key} not found"
            self.log_message(error_msg, "candle_manager")
            print(f"❌ {error_msg}")
            return False
            
        try:
            current = self.oanda_api.last_complete_candle(pair, granularity)
            if current is None:
                return False
                
            self.timings[key].last_time = current
            self.timings[key].is_ready = True
            
            success_msg = f"Forced update for {pair}_{granularity}: {self.timings[key]}"
            self.log_message(success_msg, "candle_manager")
            print(f"✅ {success_msg}")
            return True
            
        except Exception as e:
            error_msg = f"Error in force update for {pair}_{granularity}: {e}"
            self.log_message(error_msg, "candle_manager")
            print(f"❌ {error_msg}")
            return False

    def get_status_summary(self):
        """
        Get a summary of all monitored pairs and their last update times
        Useful for debugging
        """
        print("\n📋 Candle Manager Status Summary:")
        print("-" * 50)
        
        for pair_info in self.pairs_list:
            key = self.make_dict_key(pair_info)
            if key in self.timings:
                timing = self.timings[key]
                last_time_str = timing.last_time.strftime('%Y-%m-%d %H:%M:%S') if timing.last_time else "None"
                print(f"{pair_info['pair']} ({pair_info['granularity']}) - Last: {last_time_str} - Ready: {timing.is_ready}")
            else:
                print(f"{pair_info['pair']} ({pair_info['granularity']}) - NOT INITIALIZED")
        
        print("-" * 50)
