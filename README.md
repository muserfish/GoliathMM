# GoliathMM — Inventory-Aware Market Making for Quantower

A C# market-making strategy prototype built around Quantower's Strategy API. The project explores how two-sided quotes can adapt to **inventory exposure** and **top-of-book order-book imbalance**, with configurable order management and optional hedging.

## What the implementation does

- **Two-sided quoting:** calculates bid and ask quotes around a reference price, normally the best-bid/best-ask midpoint.
- **Inventory-aware skew:** shifts quotes based on the difference between current inventory and a configurable target.
- **Order-book imbalance skew:** incorporates the relative size of best-bid and best-ask volumes.
- **Tick-size normalization:** rounds bid and ask prices to valid price increments and prevents crossed internal quotes.
- **Order management:** places or replaces working quotes, avoids unnecessary replacements at unchanged prices, and retries failed quote operations.
- **Optional exposure hedging:** sends a hedge order when inventory deviation exceeds a threshold, subject to a cooldown.
- **Quantower integration:** exposes strategy parameters and uses market-data callbacks through a platform-specific gateway.

## Architecture

The main implementation is in [`src/MarketMakingStrategy.cs`](src/MarketMakingStrategy.cs).

| Component | Responsibility |
| --- | --- |
| `MarketMakingConfig` | Quote, skew, inventory and hedging parameters |
| `DomSnapshot` | Best bid/ask prices and volumes |
| `MarketMakingEngine` | Quote calculation, lifecycle and optional hedge decisions |
| `ITradingGateway` | Abstraction for placement, replacement, cancellation and positions |
| `MarketMakingStrategy` | Quantower Strategy API integration |

## Quote calculation

The engine starts from a reference price and a configurable spread. It combines inventory skew and top-of-book imbalance skew, caps the resulting price shift, and rounds quotes to the instrument's tick size. The objective is to adjust quoting behaviour as inventory and market conditions change, rather than quote symmetrically at all times.

## Configuration

The Quantower strategy exposes settings for order quantity, base/minimum spread, inventory target and range, inventory/DOM skew weights, maximum skew, quote interval, requote attempts and optional hedging parameters.

## Status and limitations

This repository is a **technical prototype / research implementation**, not a verified production trading system. The public repository does not include independent performance results, a complete reproducible backtest, or evidence of live profitability. Real execution also depends on the broker connection, order semantics, fees, latency, liquidity and operational safeguards.

**Do not deploy with live capital without independent code review, integration testing, simulation and risk controls.**

## Technology

C# · Quantower Strategy API · asynchronous order management · order-book data · inventory-based quoting

---

This project is shared as an example of trading-system design and quantitative execution research. It is not investment advice or a claim of trading returns.
