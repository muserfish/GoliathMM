using System;
using System.Threading;
using System.Threading.Tasks;

#nullable enable

namespace Quantower.Strategies
{
    /// <summary>
    ///     Encapsulates configuration options that drive the behaviour of <see cref="MarketMakingEngine"/>.
    /// </summary>
    public sealed class MarketMakingConfig
    {
        public string Symbol { get; init; } = string.Empty;
        public TimeSpan QuoteInterval { get; init; } = TimeSpan.FromMilliseconds(250);
        public decimal OrderQuantity { get; init; }
            = 1m; // Base quote size for both sides of the market.
        public decimal BaseSpread { get; init; } = 2m;
        public decimal MinSpread { get; init; } = 1m;
        public decimal TickSize { get; init; } = 0.25m;
        public decimal InventoryTarget { get; init; } = 0m;
        public decimal InventoryRange { get; init; } = 10m;
        public decimal InventorySkewWeight { get; init; } = 0.6m;
        public decimal DomSkewWeight { get; init; } = 0.4m;
        public decimal MaxPriceShift { get; init; } = 10m;
        public int MaxRequoteAttempts { get; init; } = 3;
        public HedgeConfig Hedging { get; init; } = new();
    }

    /// <summary>
    ///     Configuration of the optional hedge leg.
    /// </summary>
    public sealed class HedgeConfig
    {
        public bool Enabled { get; init; }
            = false;
        public decimal Threshold { get; init; }
            = 5m;
        public decimal OrderQuantity { get; init; }
            = 1m;
        public TimeSpan Cooldown { get; init; }
            = TimeSpan.FromSeconds(3);
    }

    /// <summary>
    ///     Lightweight snapshot of the top-of-book depth used to skew quotes.
    /// </summary>
    public readonly struct DomSnapshot
    {
        public DomSnapshot(decimal bestBidPrice, decimal bestBidVolume, decimal bestAskPrice, decimal bestAskVolume)
        {
            BestBidPrice = bestBidPrice;
            BestBidVolume = bestBidVolume;
            BestAskPrice = bestAskPrice;
            BestAskVolume = bestAskVolume;
        }

        public decimal BestBidPrice { get; }
        public decimal BestBidVolume { get; }
        public decimal BestAskPrice { get; }
        public decimal BestAskVolume { get; }

        public bool IsValid => BestBidPrice > 0 && BestAskPrice > 0 && BestAskPrice >= BestBidPrice;

        public decimal MidPrice => (BestBidPrice + BestAskPrice) / 2m;
    }

    public enum OrderSide
    {
        Buy,
        Sell
    }

    /// <summary>
    ///     Represents the placement or update result of a quote managed by <see cref="MarketMakingEngine"/>.
    /// </summary>
    public sealed class OrderTicket
    {
        public OrderTicket(string orderId)
        {
            OrderId = orderId;
        }

        public string OrderId { get; }
    }

    public interface ITradingGateway
    {
        Task<OrderTicket> PlaceOrReplaceAsync(string symbol, OrderSide side, decimal price, decimal quantity, string? replaceOrderId, CancellationToken cancellationToken);

        Task CancelAsync(string orderId, CancellationToken cancellationToken);

        Task<decimal> GetNetPositionAsync(string symbol, CancellationToken cancellationToken);

        Task<OrderTicket> HedgeAsync(string symbol, OrderSide side, decimal price, decimal quantity, CancellationToken cancellationToken);
    }

    /// <summary>
    ///     A production-grade market making loop that keeps two resting quotes live on the book, skewing them in
    ///     response to order-book imbalance and internal inventory while optionally hedging excessive exposure.
    /// </summary>
    public sealed class MarketMakingEngine : IDisposable
    {
        private readonly MarketMakingConfig _config;
        private readonly ITradingGateway _gateway;
        private readonly object _syncRoot = new();

        private CancellationTokenSource? _cts;
        private Task? _quoteTask;
        private QuoteState _state = QuoteState.Empty;
        private DomSnapshot _dom;
        private decimal _lastTradePrice;
        private decimal _netInventory;
        private DateTime _lastHedgeUtc = DateTime.MinValue;
        private bool _disposed;

        public MarketMakingEngine(MarketMakingConfig config, ITradingGateway gateway)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
            _gateway = gateway ?? throw new ArgumentNullException(nameof(gateway));
        }

        public bool IsRunning => _quoteTask != null && !_quoteTask.IsCompleted;

        public void UpdateDomSnapshot(DomSnapshot snapshot)
        {
            lock (_syncRoot)
            {
                _dom = snapshot;
            }
        }

        public void UpdateLastTradePrice(decimal price)
        {
            if (price <= 0) throw new ArgumentOutOfRangeException(nameof(price));
            lock (_syncRoot)
            {
                _lastTradePrice = price;
            }
        }

        public void UpdateInventory(decimal netInventory)
        {
            lock (_syncRoot)
            {
                _netInventory = netInventory;
            }
        }

        public Task StartAsync(CancellationToken cancellationToken = default)
        {
            lock (_syncRoot)
            {
                ThrowIfDisposed();
                if (IsRunning)
                {
                    return _quoteTask!;
                }

                _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _quoteTask = Task.Run(() => QuoteLoopAsync(_cts.Token), _cts.Token);
                return _quoteTask;
            }
        }

        public async Task StopAsync()
        {
            CancellationTokenSource? cts;
            Task? quoteTask;
            lock (_syncRoot)
            {
                ThrowIfDisposed();
                cts = _cts;
                quoteTask = _quoteTask;
                _cts = null;
                _quoteTask = null;
            }

            if (cts != null)
            {
                cts.Cancel();
                cts.Dispose();
            }

            if (quoteTask != null)
            {
                await quoteTask.ConfigureAwait(false);
            }
        }

        public async Task ResetAsync()
        {
            await StopAsync().ConfigureAwait(false);

            lock (_syncRoot)
            {
                _state = QuoteState.Empty;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            _ = StopAsync();
        }

        private async Task QuoteLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await TryQuoteOnceAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    // A real implementation would log the exception and continue quoting.
                    System.Diagnostics.Trace.TraceError($"[{nameof(MarketMakingEngine)}] Quote iteration failed: {ex}");
                }

                await Task.Delay(_config.QuoteInterval, cancellationToken).ConfigureAwait(false);
            }

            await CancelWorkingOrdersAsync(cancellationToken).ConfigureAwait(false);
        }

        private async Task TryQuoteOnceAsync(CancellationToken cancellationToken)
        {
            DomSnapshot dom;
            decimal lastPrice;
            decimal inventory;

            lock (_syncRoot)
            {
                dom = _dom;
                lastPrice = _lastTradePrice;
                inventory = _netInventory;
            }

            if (!dom.IsValid && lastPrice <= 0)
            {
                return;
            }

            decimal referencePrice = dom.IsValid ? dom.MidPrice : lastPrice;
            (decimal bidPrice, decimal askPrice) = CalculateQuotePrices(referencePrice, dom, inventory);

            await EnsureQuoteAsync(OrderSide.Buy, bidPrice, _config.OrderQuantity, cancellationToken).ConfigureAwait(false);
            await EnsureQuoteAsync(OrderSide.Sell, askPrice, _config.OrderQuantity, cancellationToken).ConfigureAwait(false);

            if (_config.Hedging.Enabled)
            {
                await TryHedgeAsync(inventory, referencePrice, cancellationToken).ConfigureAwait(false);
            }
        }

        private (decimal bidPrice, decimal askPrice) CalculateQuotePrices(decimal referencePrice, DomSnapshot dom, decimal inventory)
        {
            decimal spread = Math.Max(_config.MinSpread, _config.BaseSpread);
            decimal domSkew = CalculateDomSkew(dom) * _config.DomSkewWeight;
            decimal inventorySkew = CalculateInventorySkew(inventory) * _config.InventorySkewWeight;
            decimal skewTicks = Math.Clamp(domSkew + inventorySkew, -_config.MaxPriceShift, _config.MaxPriceShift);

            decimal halfSpread = spread / 2m;
            decimal bidPrice = referencePrice - halfSpread - skewTicks;
            decimal askPrice = referencePrice + halfSpread - skewTicks;

            bidPrice = NormalizePrice(bidPrice, PriceNormalizationDirection.Down);
            askPrice = NormalizePrice(askPrice, PriceNormalizationDirection.Up);

            if (askPrice <= bidPrice)
            {
                askPrice = NormalizePrice(bidPrice + _config.TickSize, PriceNormalizationDirection.Up);
            }

            return (bidPrice, askPrice);
        }

        private decimal CalculateDomSkew(DomSnapshot dom)
        {
            if (!dom.IsValid)
            {
                return 0m;
            }

            decimal total = dom.BestBidVolume + dom.BestAskVolume;
            if (total <= 0)
            {
                return 0m;
            }

            decimal imbalance = (dom.BestBidVolume - dom.BestAskVolume) / total;
            return imbalance * _config.TickSize;
        }

        private decimal CalculateInventorySkew(decimal inventory)
        {
            decimal distance = inventory - _config.InventoryTarget;
            if (_config.InventoryRange <= 0)
            {
                return 0m;
            }

            decimal normalized = Math.Clamp(distance / _config.InventoryRange, -1m, 1m);
            return normalized * _config.TickSize * 2m;
        }

        private enum PriceNormalizationDirection
        {
            Down,
            Up
        }

        private decimal NormalizePrice(decimal price, PriceNormalizationDirection direction)
        {
            if (_config.TickSize <= 0)
            {
                return price;
            }

            decimal ticks = price / _config.TickSize;
            decimal normalized = direction == PriceNormalizationDirection.Down
                ? Math.Floor(ticks)
                : Math.Ceiling(ticks);

            return normalized * _config.TickSize;
        }

        private async Task EnsureQuoteAsync(OrderSide side, decimal price, decimal quantity, CancellationToken cancellationToken)
        {
            QuoteState snapshot;
            lock (_syncRoot)
            {
                snapshot = _state;
            }

            string? existingOrderId = side == OrderSide.Buy ? snapshot.BidOrderId : snapshot.AskOrderId;
            decimal existingPrice = side == OrderSide.Buy ? snapshot.BidPrice : snapshot.AskPrice;

            if (existingOrderId != null && existingPrice == price)
            {
                return;
            }

            OrderTicket ticket = await _gateway
                .PlaceOrReplaceAsync(_config.Symbol, side, price, quantity, existingOrderId, cancellationToken)
                .ConfigureAwait(false);

            lock (_syncRoot)
            {
                _state = _state.With(side, ticket.OrderId, price);
            }
        }

        private async Task TryHedgeAsync(decimal inventory, decimal referencePrice, CancellationToken cancellationToken)
        {
            decimal deviation = inventory - _config.InventoryTarget;
            if (Math.Abs(deviation) < _config.Hedging.Threshold)
            {
                return;
            }

            if (DateTime.UtcNow - _lastHedgeUtc < _config.Hedging.Cooldown)
            {
                return;
            }

            OrderSide hedgeSide = deviation > 0 ? OrderSide.Sell : OrderSide.Buy;
            decimal quantity = Math.Min(Math.Abs(deviation), _config.Hedging.OrderQuantity);
            decimal price = hedgeSide == OrderSide.Buy
                ? NormalizePrice(referencePrice - _config.TickSize, PriceNormalizationDirection.Up)
                : NormalizePrice(referencePrice + _config.TickSize, PriceNormalizationDirection.Down);

            await _gateway.HedgeAsync(_config.Symbol, hedgeSide, price, quantity, cancellationToken)
                .ConfigureAwait(false);

            _lastHedgeUtc = DateTime.UtcNow;
        }

        private async Task CancelWorkingOrdersAsync(CancellationToken cancellationToken)
        {
            QuoteState state;
            lock (_syncRoot)
            {
                state = _state;
                _state = QuoteState.Empty;
            }

            if (state.BidOrderId != null)
            {
                await _gateway.CancelAsync(state.BidOrderId, cancellationToken).ConfigureAwait(false);
            }

            if (state.AskOrderId != null)
            {
                await _gateway.CancelAsync(state.AskOrderId, cancellationToken).ConfigureAwait(false);
            }
        }

        private void ThrowIfDisposed()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(nameof(MarketMakingEngine));
            }
        }

        private readonly struct QuoteState
        {
            private QuoteState(string? bidOrderId, decimal bidPrice, string? askOrderId, decimal askPrice)
            {
                BidOrderId = bidOrderId;
                BidPrice = bidPrice;
                AskOrderId = askOrderId;
                AskPrice = askPrice;
            }

            public string? BidOrderId { get; }
            public decimal BidPrice { get; }
            public string? AskOrderId { get; }
            public decimal AskPrice { get; }

            public static QuoteState Empty => new(null, 0m, null, 0m);

            public QuoteState With(OrderSide side, string orderId, decimal price)
            {
                return side == OrderSide.Buy
                    ? new QuoteState(orderId, price, AskOrderId, AskPrice)
                    : new QuoteState(BidOrderId, BidPrice, orderId, price);
            }
        }
    }
}
