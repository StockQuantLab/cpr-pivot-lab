"""Transaction cost models for backtesting.

Supports realistic brokerage, STT, exchange charges, and slippage
to prevent optimizing on gross metrics that vanish after costs.

Usage:
    from engine.cost_model import CostModel

    model = CostModel.zerodha()        # Realistic Zerodha intraday costs
    model = CostModel.zero()           # No costs (legacy comparison)
    model = CostModel.zerodha(slippage_bps=2.0)  # Add 2 bps slippage per side

    cost = model.round_trip_cost(entry_price=520.0, exit_price=525.0, qty=100)
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CostModel:
    """Transaction cost model for intraday equity trades on NSE.

    All percentage fields are expressed as fractions (e.g. 0.025% = 0.00025).
    """

    # Brokerage: flat fee per executed order (one buy + one sell = 2 orders)
    brokerage_per_order: float = 20.0

    # STT: Securities Transaction Tax on sell-side turnover (intraday)
    stt_sell_pct: float = 0.00025  # 0.025%

    # Exchange transaction charges (NSE) on total turnover (both sides)
    exchange_txn_pct: float = 0.0000345  # 0.00345%

    # SEBI turnover fee: Rs.10 per crore ≈ 0.0001%
    sebi_pct: float = 0.000001  # 0.0001%

    # GST: 18% on (brokerage + exchange charges)
    gst_pct: float = 0.18

    # Stamp duty: on buy-side turnover only
    stamp_duty_pct: float = 0.00003  # 0.003%

    # Slippage: basis points per side (applied to entry and exit)
    slippage_bps: float = 0.0

    @classmethod
    def zerodha(cls, *, slippage_bps: float = 0.0) -> CostModel:
        """Zerodha Equity Intraday cost model (as of Mar 2026)."""
        return cls(slippage_bps=slippage_bps)

    @classmethod
    def zero(cls) -> CostModel:
        """Zero-cost model for backward-compatible gross backtests."""
        return cls(
            brokerage_per_order=0.0,
            stt_sell_pct=0.0,
            exchange_txn_pct=0.0,
            sebi_pct=0.0,
            gst_pct=0.0,
            stamp_duty_pct=0.0,
            slippage_bps=0.0,
        )

    def round_trip_cost(
        self,
        entry_price: float,
        exit_price: float,
        qty: int,
        direction: str = "LONG",
    ) -> float:
        """Total round-trip cost in rupees for a completed trade.

        For LONG: buy at entry, sell at exit.
        For SHORT: sell at entry, buy at exit.

        Returns:
            Total cost (always positive) in rupees.
        """
        if qty <= 0 or entry_price <= 0:
            return 0.0

        if direction == "LONG":
            buy_value = entry_price * qty
            sell_value = exit_price * qty
        else:
            sell_value = entry_price * qty
            buy_value = exit_price * qty

        turnover = buy_value + sell_value

        # Brokerage: flat fee per order × 2 (buy + sell)
        brokerage = self.brokerage_per_order * 2

        # STT: on sell-side turnover only (intraday)
        stt = sell_value * self.stt_sell_pct

        # Exchange transaction charges on total turnover
        exchange = turnover * self.exchange_txn_pct

        # SEBI fee on total turnover
        sebi = turnover * self.sebi_pct

        # GST: 18% on (brokerage + exchange charges)
        gst = (brokerage + exchange) * self.gst_pct

        # Stamp duty: on buy-side turnover only
        stamp = buy_value * self.stamp_duty_pct

        # Slippage: bps per side applied to turnover
        slippage = turnover * (self.slippage_bps / 10_000)

        return round(brokerage + stt + exchange + sebi + gst + stamp + slippage, 2)

    def slippage_adjusted_prices(
        self,
        entry_price: float,
        exit_price: float,
        direction: str = "LONG",
    ) -> tuple[float, float]:
        """Return (adjusted_entry, adjusted_exit) after slippage.

        Slippage worsens the fill: entry is higher for LONG, lower for SHORT.
        """
        if self.slippage_bps == 0:
            return entry_price, exit_price

        slip_frac = self.slippage_bps / 10_000
        if direction == "LONG":
            adj_entry = entry_price * (1 + slip_frac)
            adj_exit = exit_price * (1 - slip_frac)
        else:
            adj_entry = entry_price * (1 - slip_frac)
            adj_exit = exit_price * (1 + slip_frac)
        return round(adj_entry, 2), round(adj_exit, 2)

    @property
    def is_zero(self) -> bool:
        """True if this model applies no costs at all."""
        return (
            self.brokerage_per_order == 0
            and self.stt_sell_pct == 0
            and self.exchange_txn_pct == 0
            and self.sebi_pct == 0
            and self.stamp_duty_pct == 0
            and self.slippage_bps == 0
        )


# ---------------------------------------------------------------------------
# Factory from CLI string
# ---------------------------------------------------------------------------

COST_MODELS: dict[str, Callable[..., CostModel]] = {
    "zerodha": CostModel.zerodha,
    "zero": CostModel.zero,
}


def cost_model_from_name(name: str, *, slippage_bps: float = 0.0) -> CostModel:
    """Create a CostModel from a CLI --commission-model name."""
    factory = COST_MODELS.get(name.lower())
    if factory is None:
        raise ValueError(f"Unknown commission model: {name!r}. Choose from: {list(COST_MODELS)}")
    if name.lower() == "zerodha":
        return factory(slippage_bps=slippage_bps)
    return factory()
