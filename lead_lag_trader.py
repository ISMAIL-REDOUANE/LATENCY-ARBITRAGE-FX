"""
lead_lag_trader.py
==================

Lead-Lag momentum strategy: Binance (Leader) -> FXCM (Follower).

The Binance price feed is consumed over a single WebSocket connection (raw
`websockets` library = lowest overhead, no REST polling), a directional
momentum / EMA-crossover trigger is computed from *percentage change* (so the
absolute price mismatch between Binance and FXCM does not matter), and an FXCM
market order is placed in the same direction with Stop Loss / Take Profit.

Architecture (async, non-blocking):
    BinanceWebSocket  : async generator of raw prices (asyncio).
    MomentumSignal    : pure-python trigger logic (stateless, no I/O).
    FXCMExecutor      : wraps the (synchronous) fxcmpy calls in a thread
                        executor so the event loop is never blocked.

IMPORTANT  —  FXCM API note
---------------------------
`fxcmpy` targets FXCM's *legacy* REST API. FXCM decommissioned it in 2022-2023,
so the bundled `fxcmpy` library is effectively dead. FXCM now requires the new
"FXCM Trading API" (client_id / client_secret -> OAuth token). The `FXCMExecutor`
below is written against the fxcmpy surface and isolated behind a single
`_place_market_order()` method — if you have the new API, replace only that
method's body with your new-API call and keep the rest of the framework intact.
See: https://fxcmtrading.com/en/new-trading-api/

Dependencies:
    pip install websockets fxcmpy

Run (paper trade first!):
    python lead_lag_trader.py --dry-run            # no real orders
    python lead_lag_trader.py                      # live (requires FXCM_TOKEN)

All parameters are configurable via CLI flags or environment variables.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import AsyncIterator, Optional

import websockets

log = logging.getLogger("lead_lag")


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass
class TradingConfig:
    """All tunables for the strategy. Defaults are sane for BTC/USD."""

    # Binance feed
    binance_symbol: str = "btcusdt"          # lowercase, e.g. "btcusdt"
    binance_stream: str = "trade"            # "trade" (per-tick) or "kline_1s"
    binance_base_url: str = "wss://stream.binance.com:9443/ws"

    # Signal logic
    window_seconds: float = 2.0              # momentum lookback window (s)
    threshold_pct: float = 0.05              # |% change| required to trigger
    use_ema: bool = True                     # enable fast/slow EMA crossover
    ema_fast: int = 3                        # fast EMA period
    ema_slow: int = 12                       # slow EMA period
    cooldown_seconds: float = 30.0           # min seconds between signals

    # FXCM execution
    fxcm_token: str = field(
        default_factory=lambda: os.environ.get("FXCM_TOKEN", ""))
    fxcm_symbol: str = "BTC/USD"             # FXCM instrument
    trade_amount: float = 0.001              # units (BTC)
    stop_loss_pips: int = 50                 # SL in pips
    take_profit_pips: int = 100              # TP in pips
    max_open_positions: int = 1              # hard cap on concurrent positions

    # Safety
    dry_run: bool = True                     # log instead of trading
    reconnect_delay: float = 2.0             # initial WS reconnect backoff (s)
    max_reconnect_delay: float = 60.0        # cap on backoff (s)

    # ------------------------------------------------------------------ #
    def __post_init__(self) -> None:
        self.binance_symbol = self.binance_symbol.lower()

    @staticmethod
    def build() -> "TradingConfig":
        p = argparse.ArgumentParser(description="Binance->FXCM lead/lag trader")
        p.add_argument("--symbol", default="btcusdt")
        p.add_argument("--stream", default="trade", choices=["trade", "kline_1s"])
        p.add_argument("--window", type=float, default=2.0)
        p.add_argument("--threshold", type=float, default=0.05)
        p.add_argument("--cooldown", type=float, default=30.0)
        p.add_argument("--ema", action="store_true", default=True)
        p.add_argument("--no-ema", dest="ema", action="store_false")
        p.add_argument("--fxcm-symbol", default="BTC/USD")
        p.add_argument("--amount", type=float, default=0.001)
        p.add_argument("--sl", type=int, default=50)
        p.add_argument("--tp", type=int, default=100)
        p.add_argument("--max-positions", type=int, default=1)
        p.add_argument("--dry-run", action="store_true", default=True)
        p.add_argument("--live", dest="dry_run", action="store_false")
        a = p.parse_args()

        return TradingConfig(
            binance_symbol=a.symbol,
            binance_stream=a.stream,
            window_seconds=a.window,
            threshold_pct=a.threshold,
            use_ema=a.ema,
            cooldown_seconds=a.cooldown,
            fxcm_symbol=a.fxcm_symbol,
            trade_amount=a.amount,
            stop_loss_pips=a.sl,
            take_profit_pips=a.tp,
            max_open_positions=a.max_positions,
            dry_run=a.dry_run,
        )


# --------------------------------------------------------------------------- #
# 1) Binance WebSocket feed (Leader)  —  the FASTEST single-stream method
# --------------------------------------------------------------------------- #
class BinanceWebSocket:
    """
    Consumes a single Binance WebSocket stream and yields parsed float prices.

    * Raw `websockets` client: no wrapper overhead, compression disabled
      (`compression=None`) because permessage-deflate adds latency.
    * Auto-reconnect with exponential backoff; pings handled by the library.
    * Yields one price per message:
        - trade stream   -> message["p"] (last trade price)
        - kline_1s stream -> message["k"]["c"] (close of the 1s candle)
    """

    def __init__(self, cfg: TradingConfig) -> None:
        self.cfg = cfg
        self.url = (
            f"{cfg.binance_base_url}/{cfg.binance_symbol}@{cfg.binance_stream}"
        )
        self._backoff = cfg.reconnect_delay

    def _parse_price(self, raw: str) -> float:
        data = json.loads(raw)
        if self.cfg.binance_stream == "trade":
            return float(data["p"])
        return float(data["k"]["c"])  # kline close

    async def prices(self) -> AsyncIterator[float]:
        """Endless async generator of live prices with auto-reconnect."""
        while True:
            try:
                async with websockets.connect(
                    self.url,
                    compression=None,   # lower latency
                    ping_interval=20,   # keep-alive (Binance pings ~every 20s)
                    ping_timeout=20,
                    max_queue=1000,     # drop slow consumers, never block feed
                ) as ws:
                    log.info("Binance connected: %s", self.url)
                    self._backoff = self.cfg.reconnect_delay
                    async for raw in ws:
                        yield self._parse_price(raw)
            except websockets.ConnectionClosed as exc:
                log.warning("Binance WS closed (%s) — reconnecting", exc.code)
            except asyncio.TimeoutError:
                log.warning("Binance WS ping timeout — reconnecting")
            except Exception:
                log.exception("Binance WS error — reconnecting")

            delay = self._backoff
            self._backoff = min(self._backoff * 2, self.cfg.max_reconnect_delay)
            log.info("Reconnect in %.1fs", delay)
            await asyncio.sleep(delay)


# --------------------------------------------------------------------------- #
# 2) Signal generation  —  percentage-based, independent of absolute prices
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Signal:
    """A directional order intent produced by the strategy."""

    direction: str            # "buy" | "sell"
    reason: str               # "momentum" | "ema_cross"
    ref_price: float
    change_pct: float


class MomentumSignal:
    """
    Detects directional movement from the Binance tick stream.

    Two independent triggers (both percentage-based, so the Binance/FXCM
    absolute-price mismatch is irrelevant):

    1. Momentum  —  if the price moved by >= `threshold_pct` within
       `window_seconds`, trade in the direction of the move.
    2. EMA cross —  if the fast EMA crosses above/below the slow EMA,
       trade in the crossover direction.

    A per-symbol cooldown prevents firing multiple orders during a single
    extended move. The class is pure CPU/state (no I/O) and is safe to call
    from any event-loop task.
    """

    def __init__(self, cfg: TradingConfig) -> None:
        self.cfg = cfg
        self._buf: deque[tuple[float, float]] = deque()  # (monotonic_ts, price)
        self._last_trade_ts = -1e9
        self._ema_fast: Optional[float] = None
        self._ema_slow: Optional[float] = None
        self._prev_fast: Optional[float] = None
        self._prev_slow: Optional[float] = None

    # -- internals ------------------------------------------------------ #
    def _prune(self, now: float) -> None:
        """Drop prices older than we need (window + margin for gaps)."""
        keep_for = max(self.cfg.window_seconds, 5.0) + 1.0
        while self._buf and self._buf[0][0] < now - keep_for:
            self._buf.popleft()

    def _price_at(self, cutoff: float) -> float:
        """Closest price at-or-after `cutoff` (the reference price)."""
        for ts, price in self._buf:
            if ts >= cutoff:
                return price
        return self._buf[0][1]

    def _ema(self, value: float, prev: Optional[float], period: int) -> float:
        k = 2.0 / (period + 1.0)
        return value if prev is None else value * k + prev * (1.0 - k)

    # -- public API ------------------------------------------------------ #
    def update(self, price: float) -> Optional[Signal]:
        now = time.monotonic()
        self._buf.append((now, price))
        self._prune(now)

        # --- EMA crossover trigger ------------------------------------- #
        if self.cfg.use_ema:
            self._prev_fast, self._prev_slow = self._ema_fast, self._ema_slow
            self._ema_fast = self._ema(price, self._ema_fast, self.cfg.ema_fast)
            self._ema_slow = self._ema(price, self._ema_slow, self.cfg.ema_slow)
            if (
                self._prev_fast is not None
                and self._prev_slow is not None
                and self._in_cooldown(now) is False
            ):
                crossed_up = (
                    self._ema_fast > self._ema_slow
                    and self._prev_fast <= self._prev_slow
                )
                crossed_down = (
                    self._ema_fast < self._ema_slow
                    and self._prev_fast >= self._prev_slow
                )
                if crossed_up:
                    return self._emit(now, "buy", "ema_cross", price, price)
                if crossed_down:
                    return self._emit(now, "sell", "ema_cross", price, price)

        # --- Momentum (percentage change over window) trigger ------------ #
        cutoff = now - self.cfg.window_seconds
        if len(self._buf) >= 2 and self._buf[0][0] <= cutoff:
            ref = self._price_at(cutoff)
            change_pct = (price / ref - 1.0) * 100.0
            if abs(change_pct) >= self.cfg.threshold_pct and not self._in_cooldown(now):
                direction = "buy" if change_pct > 0 else "sell"
                return self._emit(
                    now, direction, "momentum", ref, change_pct
                )

        return None

    # -- helpers --------------------------------------------------------- #
    def _in_cooldown(self, now: float) -> bool:
        return now - self._last_trade_ts < self.cfg.cooldown_seconds

    def _emit(self, now: float, direction: str, reason: str,
              ref: float, pct: float) -> Signal:
        self._last_trade_ts = now          # arm the cooldown immediately
        return Signal(direction, reason, ref, pct)


# --------------------------------------------------------------------------- #
# 3) FXCM execution (Follower)  —  synchronous calls offloaded to a thread
# --------------------------------------------------------------------------- #
class FXCMExecutor:
    """
    Executes FXCM market orders for the received Binance signals.

    `fxcmpy` is a synchronous library, so every call is dispatched through
    `asyncio.get_running_loop().run_in_executor(...)` to keep the Binance
    listener non-blocking. SL/TP are attached in pips at order time.

    In dry-run mode no orders are placed and nothing connects to FXCM.
    """

    def __init__(self, cfg: TradingConfig) -> None:
        self.cfg = cfg
        self._api = None

    def connect(self) -> None:
        """Establish the FXCM session (blocking; call once at startup)."""
        if self.cfg.dry_run:
            log.warning("DRY-RUN mode: FXCM session NOT opened")
            return
        try:
            import fxcmpy  # deferred import so dry-run needs no FXCM package
        except ImportError:
            raise RuntimeError(
                "fxcmpy is not installed (`pip install fxcmpy`). Note: FXCM "
                "deprecated fxcmpy's API — see the docstring at the top of this "
                "file for the new-FXCM-API migration note."
            ) from None
        if not self.cfg.fxcm_token:
            raise RuntimeError("FXCM_TOKEN is empty — refusing to trade live")
        log.info("Connecting to FXCM ...")
        self._api = fxcmpy.fxcmpy(access_token=self.cfg.fxcm_token,
                                  log_level="error")
        log.info("FXCM connected. Account: %s",
                 self._api.get_accounts()["accountId"].tolist())

    def close(self) -> None:
        if self._api is not None:
            with contextlib.suppress(Exception):
                self._api.close()

    # -- async interface -------------------------------------------------- #
    async def execute(self, signal: Signal) -> None:
        if self.cfg.dry_run:
            log.info(
                "[DRY-RUN] would place %s %s  (reason=%s, ref=%.2f, chg=%.3f%%)",
                signal.direction.upper(), self.cfg.fxcm_symbol,
                signal.reason, signal.ref_price, signal.change_pct,
            )
            return

        if self._api is None:
            log.error("FXCM not connected — dropping signal %s", signal)
            return

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._place_market_order, signal)

    # -- blocking worker (runs inside a thread executor) ------------------- #
    def _place_market_order(self, signal: Signal) -> None:
        try:
            open_positions = len(self._api.get_open_positions())
        except Exception:
            log.exception("Could not read FXCM open positions")
            open_positions = 0

        if open_positions >= self.cfg.max_open_positions:
            log.warning(
                "Max positions reached (%d) — skipping signal %s",
                self.cfg.max_open_positions, signal.direction,
            )
            return

        is_buy = signal.direction == "buy"
        log.info(
            "FXCM market order: %s %s %.4f (SL=%d, TP=%d pips)",
            signal.direction.upper(), self.cfg.fxcm_symbol,
            self.cfg.trade_amount, self.cfg.stop_loss_pips,
            self.cfg.take_profit_pips,
        )
        # fxcmpy's open_trade: stop/limit are in pips when is_in_pips=True.
        self._api.open_trade(
            symbol=self.cfg.fxcm_symbol,
            is_buy=is_buy,
            amount=self.cfg.trade_amount,
            is_in_pips=True,
            time_in_force="GTC",
            order_type="AtMarket",
            is_in_units=True,
            stop=self.cfg.stop_loss_pips,
            limit=self.cfg.take_profit_pips,
        )


# --------------------------------------------------------------------------- #
# 4) Orchestrator  —  wires the feed -> signal -> execution pipeline
# --------------------------------------------------------------------------- #
class LeadLagTrader:
    """Ties the Binance feed, signal engine and FXCM executor together."""

    def __init__(self, feed: BinanceWebSocket, signal: MomentumSignal,
                 executor: FXCMExecutor, cfg: TradingConfig) -> None:
        self.feed = feed
        self.signal = signal
        self.executor = executor
        self.cfg = cfg

    async def run(self) -> None:
        tick_count = 0
        async for price in self.feed.prices():
            tick_count += 1
            trigger = self.signal.update(price)
            if trigger is not None:
                log.info(
                    "SIGNAL %s  (reason=%s | ref=%.2f | chg=%+.3f%%)",
                    trigger.direction.upper(), trigger.reason,
                    trigger.ref_price, trigger.change_pct,
                )
                await self.executor.execute(trigger)
            if tick_count % 1000 == 0:
                log.debug("processed %d ticks", tick_count)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #
async def _main(cfg: TradingConfig) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )

    feed = BinanceWebSocket(cfg)
    signal = MomentumSignal(cfg)
    executor = FXCMExecutor(cfg)
    trader = LeadLagTrader(feed, signal, executor, cfg)

    try:
        executor.connect()
        await trader.run()
    except KeyboardInterrupt:
        log.info("Shutting down ...")
    finally:
        executor.close()


def main() -> None:
    cfg = TradingConfig.build()
    try:
        asyncio.run(_main(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
