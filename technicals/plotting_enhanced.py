import datetime as dt
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class EnhancedCandlePlot:
    """Enhanced plotting class that supports multiple panels for oscillators/indicators"""

    def __init__(self, df, candles=True, panels=1, panel_heights=None, panel_titles=None):
        """
        Initialize the enhanced candlestick plot with support for multiple panels.

        Args:
            df: DataFrame with OHLC data
            candles: Whether to show candlesticks in the main panel
            panels: Number of panels (subplots) to create
            panel_heights: List of relative heights for each panel (e.g., [0.7, 0.3])
            panel_titles: List of titles for each panel
        """
        self.df_plot = df.copy()
        self.candles = candles
        self.panels = panels
        self.panel_heights = panel_heights or self._get_default_heights(panels)
        self.panel_titles = panel_titles or ['' for _ in range(panels)]
        self.create_figure()

    def _get_default_heights(self, panels):
        """Get default panel heights based on number of panels."""
        if panels == 1:
            return [1.0]
        elif panels == 2:
            return [0.7, 0.3]
        elif panels == 3:
            return [0.5, 0.25, 0.25]
        else:
            # Distribute evenly for more panels
            return [1.0/panels for _ in range(panels)]

    def add_timestr(self):
        """Add formatted time string to remove weekend gaps."""
        self.df_plot['sTime'] = [dt.datetime.strftime(x, "s%Y-%m-%d %H:%M")
                        for x in self.df_plot.time]

    def create_figure(self):
        """Create the figure with specified number of panels."""
        self.add_timestr()

        if self.panels == 1:
            # Single panel - use secondary_y for compatibility
            self.fig = make_subplots(specs=[[{"secondary_y": True}]])
        else:
            # Multiple panels - create vertical subplots
            self.fig = make_subplots(
                rows=self.panels,
                cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=self.panel_heights,
                subplot_titles=self.panel_titles
            )

        # Add candlesticks to the first panel if requested
        if self.candles:
            self.add_candlesticks(panel=1)

    def add_candlesticks(self, panel=1):
        """Add candlestick chart to specified panel."""
        candlestick = go.Candlestick(
            x=self.df_plot.sTime,
            open=self.df_plot.mid_o,
            high=self.df_plot.mid_h,
            low=self.df_plot.mid_l,
            close=self.df_plot.mid_c,
            line=dict(width=1),
            opacity=1,
            increasing_fillcolor='#24A06B',
            decreasing_fillcolor="#CC2E3C",
            increasing_line_color='#2EC886',
            decreasing_line_color='#FF3A4C',
            name='Price'
        )

        if self.panels == 1:
            self.fig.add_trace(candlestick)
        else:
            self.fig.add_trace(candlestick, row=panel, col=1)

    def add_line(self, column, panel=1, name=None, color=None, width=2,
                 dash=None, secondary_y=False, show_legend=True):
        """
        Add a line trace to specified panel.

        Args:
            column: Column name in df_plot to plot
            panel: Panel number (1-indexed)
            name: Display name for the trace
            color: Line color
            width: Line width
            dash: Line style ('solid', 'dash', 'dot', 'dashdot')
            secondary_y: Use secondary y-axis (only for panel 1 with single panel)
            show_legend: Whether to show in legend
        """
        line_config = dict(width=width)
        if color:
            line_config['color'] = color
        if dash:
            line_config['dash'] = dash

        trace = go.Scatter(
            x=self.df_plot.sTime,
            y=self.df_plot[column] if isinstance(column, str) else column,
            mode='lines',
            name=name or column if isinstance(column, str) else 'Line',
            line=line_config,
            showlegend=show_legend
        )

        if self.panels == 1:
            self.fig.add_trace(trace, secondary_y=secondary_y)
        else:
            self.fig.add_trace(trace, row=panel, col=1)

    def add_horizontal_line(self, y_value, panel=1, name=None, color='gray',
                           width=1, dash='dash', show_legend=False):
        """Add a horizontal line to specified panel."""
        self.add_line(
            column=[y_value] * len(self.df_plot),
            panel=panel,
            name=name,
            color=color,
            width=width,
            dash=dash,
            show_legend=show_legend
        )

    def add_scatter(self, x_col, y_col, panel=1, mode='markers',
                   marker_dict=None, name=None, showlegend=True):
        """
        Add scatter plot to specified panel.

        Args:
            x_col: Column name for x values or actual values
            y_col: Column name for y values or actual values
            panel: Panel number
            mode: 'markers', 'lines', 'lines+markers'
            marker_dict: Dictionary with marker configuration
            name: Display name
            showlegend: Show in legend
        """
        x_data = self.df_plot[x_col] if isinstance(x_col, str) else x_col
        y_data = self.df_plot[y_col] if isinstance(y_col, str) else y_col

        trace = go.Scatter(
            x=x_data,
            y=y_data,
            mode=mode,
            marker=marker_dict or dict(size=8),
            name=name,
            showlegend=showlegend
        )

        if self.panels == 1:
            self.fig.add_trace(trace)
        else:
            self.fig.add_trace(trace, row=panel, col=1)

    def add_signals(self, signal_col, panel=1, buy_color='#00FF00',
                   sell_color='#FF0000', arrow_size=15):
        """
        Add buy/sell signal arrows to specified panel.

        Args:
            signal_col: Column with signals (1 for buy, -1 for sell)
            panel: Panel to add signals to
            buy_color: Color for buy signals
            sell_color: Color for sell signals
            arrow_size: Size of arrow markers
        """
        buy_mask = self.df_plot[signal_col] == 1
        sell_mask = self.df_plot[signal_col] == -1

        if buy_mask.any():
            # Calculate position below the low
            arrow_offset = (self.df_plot['mid_h'] - self.df_plot['mid_l']).mean() * 0.3
            self.add_scatter(
                x_col=self.df_plot.loc[buy_mask, 'sTime'],
                y_col=self.df_plot.loc[buy_mask, 'mid_l'] - arrow_offset,
                panel=panel,
                mode='markers',
                marker_dict=dict(
                    symbol='triangle-up',
                    size=arrow_size,
                    color=buy_color,
                    line=dict(color=buy_color, width=1)
                ),
                name='Buy Signal'
            )

        if sell_mask.any():
            # Calculate position above the high
            arrow_offset = (self.df_plot['mid_h'] - self.df_plot['mid_l']).mean() * 0.3
            self.add_scatter(
                x_col=self.df_plot.loc[sell_mask, 'sTime'],
                y_col=self.df_plot.loc[sell_mask, 'mid_h'] + arrow_offset,
                panel=panel,
                mode='markers',
                marker_dict=dict(
                    symbol='triangle-down',
                    size=arrow_size,
                    color=sell_color,
                    line=dict(color=sell_color, width=1)
                ),
                name='Sell Signal'
            )

    def add_oscillator(self, column, panel=2, name=None, color='#00BFFF',
                      thresholds=None, zero_line=True):
        """
        Add an oscillator (like RSI, Stochastic, Z-Score) to specified panel.

        Args:
            column: Column name with oscillator values
            panel: Panel number (typically 2 or 3)
            name: Display name
            color: Line color
            thresholds: Dict with 'upper' and 'lower' threshold values
            zero_line: Whether to add a zero line
        """
        # Add main oscillator line
        self.add_line(column, panel=panel, name=name, color=color, width=2)

        # Add threshold lines if specified
        if thresholds:
            if 'upper' in thresholds:
                self.add_horizontal_line(
                    thresholds['upper'],
                    panel=panel,
                    name='Upper Threshold',
                    color='#FF6B6B'
                )
            if 'lower' in thresholds:
                self.add_horizontal_line(
                    thresholds['lower'],
                    panel=panel,
                    name='Lower Threshold',
                    color='#4ECDC4'
                )

        # Add zero line if requested
        if zero_line:
            self.add_horizontal_line(
                0,
                panel=panel,
                name='Zero',
                color='#888888',
                dash='dot'
            )

    def update_layout(self, width=1200, height=700, title=None, nticks=10):
        """Update the figure layout with styling."""
        # Update all y-axes
        for i in range(1, self.panels + 1):
            self.fig.update_yaxes(
                gridcolor="#1f292f",
                row=i, col=1
            )

        # Update all x-axes
        self.fig.update_xaxes(
            gridcolor="#1f292f",
            rangeslider=dict(visible=False),
            nticks=nticks
        )

        # General layout updates
        layout_updates = dict(
            width=width,
            height=height,
            margin=dict(l=50, r=50, t=50 if title else 30, b=50),
            paper_bgcolor="#2c303c",
            plot_bgcolor="#2c303c",
            font=dict(size=10, color="#e1e1e1"),
            showlegend=True,
            legend=dict(
                yanchor="top",
                y=0.99,
                xanchor="left",
                x=0.01,
                bgcolor="rgba(0,0,0,0.5)"
            )
        )

        if title:
            layout_updates['title'] = title

        self.fig.update_layout(**layout_updates)

    def show(self):
        """Display the plot."""
        self.fig.show()

    def show_plot(self, width=1200, height=700, nticks=10, title=None):
        """Convenience method for compatibility - update layout and show."""
        self.update_layout(width=width, height=height, title=title, nticks=nticks)
        self.show()


# Keep original class for backward compatibility
CandlePlot = EnhancedCandlePlot