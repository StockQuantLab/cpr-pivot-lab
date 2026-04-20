# Trailing Stop Explained

How the CPR Pivot Lab engine protects and maximises profit after entry.  
Written for operators, analysts, and developers.

---

## The Three Phases

Every trade starts in **PROTECT** and can advance through two more phases.

```
PROTECT  →  BREAKEVEN  →  TRAIL  →  exit
```

| Phase | Stop Loss sits at | What triggers the advance |
|---|---|---|
| **PROTECT** | Original SL (e.g. BC − ATR buffer for LONG) | Candle **CLOSE** ≥ entry + 1R |
| **BREAKEVEN** | Entry price — worst-case exit is ~₹0 | Candle **HIGH** or **CLOSE** ≥ entry + 2R (LONG); SHORT uses the mirror rule with LOW |
| **TRAIL** | Highest completed-bar high seen since entry − 1× ATR | Only moves in your favour; stops you out when price reverses |

**Key numbers (typical CPR_LEVELS LONG trade):**

```
entry        = ₹100
sl           = ₹95     →  SL distance (1R) = ₹5
1R level     = ₹105    →  breakeven trigger
2R level     = ₹110    →  trail trigger
target (R1)  = ₹120    →  exit if trail never fires
ATR          = ₹3
```

---

## Example 1 — LONG, price closes through 2R and keeps running

Price closes above the 2R threshold so TRAIL activates on that bar.  
The stop is tightened as soon as the bar closes, but the bar itself is still evaluated
against the pre-update stop — see *What the engine can't know* below.

> Note: the worked tables below were originally written against the close-only anchor.
> The current LONG implementation uses the completed bar's high as the post-close
> trailing anchor once 2R is proven, so real trailing exits can be tighter than the
> close-only numbers shown in those tables.

| Bar | Close | High | Phase | SL after bar | Note |
|-----|-------|------|-------|--------------|------|
| 1 | 102 | 104 | PROTECT | 95 | |
| 2 | 106 | 108 | → **BREAKEVEN** | 100 | Close ≥ 105 (1R) |
| 3 | 111 | 113 | → **TRAIL** | 100 | Close ≥ 110 (2R); stop is tightened after the bar closes |
| 4 | 113 | 115 | TRAIL | 110 | highest\_close=113 → SL=113−3=110 |
| 5 | 116 | 118 | TRAIL | 113 | highest\_close=116 → SL=116−3=113 |
| 6 | 114 | 115 | TRAIL | 113 | Price dips; SL does **not** move down |
| 7 | 110 | 112 | → **HIT** | — | Low hits SL=113 → **exit ₹113** |

**Result:** entry ₹100 → exit ₹113 = **+₹13 (+2.6R)**.  
Without trail the trade would have exited BREAKEVEN\_SL at ₹100 (−₹83 commission).

---

## Example 2 — LONG, intraday spike to 2R then immediate reversal

Price spikes above 2R **intrabar** (high ≥ ₹110) but the **close** stays below it.  
Before April 2026 the engine missed this entirely.

### Old behaviour (before fix)

| Bar | Close | High | Phase | SL | What happened |
|-----|-------|------|-------|-----|--------------|
| 1 | 102 | 104 | PROTECT | 95 | |
| 2 | 107 | 108 | → BREAKEVEN | 100 | Close ≥ 105 |
| 3 | 108 | **114** | BREAKEVEN | 100 | HIGH ≥ 110 **ignored** — close checked only |
| 4 | 97 | 99 | → **HIT** | — | Reversal; exits at entry = **BREAKEVEN\_SL ≈ −₹83** |

The intraday high of ₹114 was never seen by the engine.

### New behaviour (after fix)

| Bar | Close | High | Phase | SL after bar | Note |
|-----|-------|------|-------|--------------|------|
| 1 | 102 | 104 | PROTECT | 95 | |
| 2 | 107 | 108 | → **BREAKEVEN** | 100 | |
| 3 | 108 | **114** | → **TRAIL** | 100 | HIGH ≥ 110 triggers TRAIL; stop tightens after the bar closes |
| 4 | 109 | 111 | TRAIL | 106 | highest\_close=109 → SL=109−3=106 |
| 5 | 111 | 113 | TRAIL | 108 | highest\_close=111 → SL=111−3=108 |
| 6 | 106 | 108 | TRAIL | 108 | Dip; no change |
| 7 | 102 | 104 | → **HIT** | — | Low hits 108 → **exit ₹108 = +₹8 (+1.6R)** |

**Result:** +₹8 instead of −₹83.  
If the reversal is sharp enough to hit SL=100 on bar 3 itself, the trade exits at entry (₹0 loss) — this is still better than −₹83 under the old code.

---

## Example 3 — SHORT, intraday spike to 2R then reversal

Mirror of Example 2.  For SHORT, "2R" means price **falls** to entry − 2R.

```
entry        = ₹100
sl           = ₹105    →  1R = ₹5
1R level     = ₹95     →  breakeven trigger
2R level     = ₹90     →  trail trigger
ATR          = ₹3
target (S1)  = ₹80
```

| Bar | Close | Low | Phase | SL after bar | Note |
|-----|-------|-----|-------|--------------|------|
| 1 | 98 | 97 | PROTECT | 105 | |
| 2 | 95 | 93 | → **BREAKEVEN** | 100 | Close ≤ 95 (1R for SHORT) |
| 3 | 92 | **87** | → **TRAIL** | 100 | LOW ≤ 90 (2R); stop tightens after the bar closes |
| 4 | 91 | 89 | TRAIL | 94 | lowest\_close=91 → SL=91+3=94 |
| 5 | 93 | 91 | TRAIL | 94 | No move favourable |
| 6 | 97 | 96 | TRAIL | 94 | Price moving against; SL stays |
| 7 | 101 | 98 | → **HIT** | — | High hits 94? No — high=98 > SL=94, **exit ₹94 = +₹6 (+1.2R)** |

**Without the fix:** bar 3 close = 92 < 90 → actually in this case close **does** cross 2R.  
Let me show a case where close stays just above 90:

| Bar | Close | Low | Phase | SL | Note |
|-----|-------|-----|-------|-----|------|
| 3 | **91** | **87** | → TRAIL | 100 | Close=91 > 90 (old code misses it); LOW=87 < 90 fixes it |

Old code: SL stays BREAKEVEN at 100. Reversal to 101 → BREAKEVEN\_SL.  
New code: TRAIL activates (low ≤ 90), stop tightens after the close → subsequent bars exit profitably.

**Why SHORT gains less from this fix than LONG:**  
SHORT already uses the mirror of the LONG touch rule: a bar whose LOW reaches 2R can arm TRAIL
even if the close does not.  The difference is what happens after that:
the short trail anchor (`lowest_since_entry`) still tracks closes only, and post-2R SHORT moves
often reverse faster than LONG continuation trades. That means the trail frequently converts a
TARGET into a TRAILING\_SL at a smaller gain, or even produces no extra gain at all if price snaps
back quickly. In other words, the logic is symmetrical, but the market behavior is not.
See `docs/ISSUES.md` for the measured backtest deltas and `docs/strategy-guide.md` for the operator
summary.

---

## What the Engine Can and Cannot Know on a 5-Minute Candle

A 5-minute candle gives you four numbers: **OPEN, HIGH, LOW, CLOSE**.

```
     HIGH  ──── 114
           │
 OPEN ─────┼──── 108
           │
     LOW   ──── 96
           │
 CLOSE ────┼──── 108   (same as open in this example, but price moved a lot intrabar)
```

**The engine cannot tell:**

- Whether HIGH was touched before LOW or after
- Whether price hit 2R and then reversed, or reversed first and then briefly spiked
- The exact sub-minute sequence of prices within the bar

### Design decisions the engine makes because of this

| Situation | Engine decision | Reason |
|---|---|---|
| TRAIL activates via **candle HIGH** (close < 2R) | SL stays at entry for **this bar's** hit check, then tightens after the bar closes | Can't know if the spike happened before or after any reversal |
| TRAIL activates via **candle CLOSE** (close ≥ 2R) | SL tightens after the bar closes | Same logic — tightening SL mid-bar is an optimistic assumption |
| Trail SL anchor | Tracks the **completed bar high** for LONG | The bar high is known only after the candle completes, so it is safe to use as a post-close trailing anchor |
| BREAKEVEN advancement | Requires candle **CLOSE** ≥ 1R | A close above 1R means price sustained that level; a mere intrabar touch does not |
| LONG trail trigger | Can use candle **HIGH** to arm TRAIL | Lets a 5-minute candle that briefly touches 2R start protecting profit after it closes |
| SHORT trail trigger | Can use candle **LOW** to arm TRAIL | Same mirror rule as LONG, but the post-2R continuation pattern is often weaker |

### Practical consequence

A small number of trades will still exit BREAKEVEN\_SL after the fix:

- Bar has HIGH ≥ 2R → TRAIL activates, stop tightens after the bar closes.
- A later bar may then hit the tighter stop and exit profitably.

This is the correct conservative outcome.  The engine protected capital at the cost of not
capturing the brief spike.  The candle data alone cannot prove the spike preceded the reversal.

The fix captures the majority case: intraday 2R spike followed by a **gradual** reversal
over one or more subsequent bars.  In those trades the trail SL rises above entry immediately
after the candle closes, so the reversal hits a tighter stop on a later bar and produces a
positive exit instead of a breakeven.

### Why we do not expect the same profit lift on SHORT

The trigger rule is symmetric, but the payoff profile is not.  SHORT trades often snap back
faster after the 2R flush, so the trail has less room to ratchet before the reversal starts.
That is why SHORT can show more `TRAILING_SL` exits without a matching jump in total P&L.
We still keep the same bar-touch rule available, but we tune the SHORT trail distance
separately instead of assuming the LONG result will transfer unchanged.

---

## Summary

| Scenario | Before April 2026 fix | After fix |
|---|---|---|
| Close ≥ 2R → TRAIL | SL tightened immediately (optimistic) | SL tightened after bar close |
| High ≥ 2R, close < 2R → TRAIL | TRAIL **never** activated | TRAIL activates, SL tightened after close |
| Same bar: close crosses 1R AND high crosses 2R | Only BREAKEVEN activated | Both BREAKEVEN and TRAIL activate |
| Post-2R gradual reversal over multiple bars | BREAKEVEN\_SL (~−₹83) | Profitable TRAILING\_SL exit |
| Post-2R immediate reversal same/next bar | BREAKEVEN\_SL | BREAKEVEN\_SL (unchanged — no OHLC info to do better) |
