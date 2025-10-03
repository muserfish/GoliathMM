using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TradingPlatform.BusinessLayer;

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
        public decimal OrderQuantity { get; init; } = 1m;
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
        public bool Enabled { get; init; } = false;
        public decimal Threshold { get; init; } = 5m;
        public decimal OrderQuantity { get; init; } = 1m;
        public TimeSpan Cooldown { get; init; } = TimeSpan.FromSeconds(3);
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
                    System.Diagnostics.Trace.TraceError($"[{nameof(MarketMakingEngine)}] Quote iteration failed: {ex}");
                }

                await Task.Delay(_config.QuoteInterval, cancellationToken).ConfigureAwait(false);
            }

            await CancelWorkingOrdersAsync().ConfigureAwait(false);
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

        private static readonly TimeSpan QuoteRetryDelay = TimeSpan.FromMilliseconds(50);

        private sealed class PendingCancelException : Exception
        {
            public PendingCancelException(string orderId)
                : base($"Cancel pending for order {orderId}.")
            {
                OrderId = orderId;
            }

            public string OrderId { get; }
        }

        private async Task EnsureQuoteAsync(OrderSide side, decimal price, decimal quantity, CancellationToken cancellationToken)
        {
            int attempts = Math.Max(1, _config.MaxRequoteAttempts);
            Exception? lastError = null;

            for (int attempt = 1; attempt <= attempts; attempt++)
            {
                cancellationToken.ThrowIfCancellationRequested();

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

                try
                {
                    OrderTicket ticket = await _gateway
                        .PlaceOrReplaceAsync(_config.Symbol, side, price, quantity, existingOrderId, cancellationToken)
                        .ConfigureAwait(false);

                    lock (_syncRoot)
                    {
                        _state = _state.With(side, ticket.OrderId, price);
                    }

                    return;
                }
                catch (PendingCancelException)
                {
                    await Task.Delay(QuoteRetryDelay, cancellationToken).ConfigureAwait(false);
                    attempt--;
                    continue;
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    lastError = ex;
                    Trace.TraceWarning(
                        $"[{nameof(MarketMakingEngine)}] Failed to {(existingOrderId == null ? "place" : "replace")} {side} quote (attempt {attempt}/{attempts}): {ex.Message}");

                    if (existingOrderId != null)
                    {
                        lock (_syncRoot)
                        {
                            _state = _state.Without(side);
                        }
                    }

                    if (attempt < attempts)
                    {
                        await Task.Delay(QuoteRetryDelay, cancellationToken).ConfigureAwait(false);
                    }
                }
            }

            if (lastError != null)
            {
                Trace.TraceError(
                    $"[{nameof(MarketMakingEngine)}] Exhausted quote retries for {side}: {lastError.Message}");
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

        private async Task CancelWorkingOrdersAsync()
        {
            QuoteState state;
            lock (_syncRoot)
            {
                state = _state;
                _state = QuoteState.Empty;
            }

            if (state.BidOrderId != null)
            {
                await _gateway.CancelAsync(state.BidOrderId, CancellationToken.None).ConfigureAwait(false);
            }

            if (state.AskOrderId != null)
            {
                await _gateway.CancelAsync(state.AskOrderId, CancellationToken.None).ConfigureAwait(false);
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

            public QuoteState Without(OrderSide side)
            {
                return side == OrderSide.Buy
                    ? new QuoteState(null, 0m, AskOrderId, AskPrice)
                    : new QuoteState(BidOrderId, BidPrice, null, 0m);
            }
        }
    }

    /// <summary>
    ///     Thin Quantower wrapper that hosts <see cref="MarketMakingEngine"/> inside the Strategy runtime so it can
    ///     appear in Strategy Manager and receive platform callbacks for market data and position updates.
    /// </summary>
    public sealed class MarketMakingStrategy : Strategy
    {
        private MarketMakingEngine? _engine;
        private CancellationTokenSource? _cts;
        private QuantowerTradingGateway? _gateway;

        [InputParameter("Account", 0)]
        public Account? TradingAccount { get; set; }

        [InputParameter("Symbol", 1)]
        public Symbol? TradingSymbol { get; set; }

        [InputParameter("Order quantity", 10, 0.000001, 100000, 0.000001, 6)]
        public double OrderQuantity { get; set; } = 1;

        [InputParameter("Base spread (ticks)", 11, 1, 1000, 1, 0)]
        public int BaseSpreadTicks { get; set; } = 8;

        [InputParameter("Minimum spread (ticks)", 12, 1, 1000, 1, 0)]
        public int MinSpreadTicks { get; set; } = 4;

        [InputParameter("Inventory target", 13, -100000, 100000, 1, 0)]
        public double InventoryTarget { get; set; }

        [InputParameter("Inventory range", 14, 0.000001, 100000, 0.000001, 6)]
        public double InventoryRange { get; set; } = 10;

        [InputParameter("Inventory skew weight", 15, 0.0, 1.0, 0.05, 2)]
        public double InventoryWeight { get; set; } = 0.6;

        [InputParameter("DOM skew weight", 16, 0.0, 1.0, 0.05, 2)]
        public double DomWeight { get; set; } = 0.4;

        [InputParameter("Max skew (ticks)", 17, 0, 1000, 1, 0)]
        public int MaxSkewTicks { get; set; } = 40;

        [InputParameter("Quote interval (ms)", 18, 50, 10000, 50, 0)]
        public int QuoteIntervalMs { get; set; } = 250;

        [InputParameter("Max requote attempts", 19, 1, 20, 1, 0)]
        public int MaxRequoteAttempts { get; set; } = 3;

        [InputParameter("Enable hedge", 30)]
        public bool HedgeEnabled { get; set; }

        [InputParameter("Hedge threshold", 31, 0.000001, 100000, 0.000001, 6)]
        public double HedgeThreshold { get; set; } = 5;

        [InputParameter("Hedge quantity", 32, 0.000001, 100000, 0.000001, 6)]
        public double HedgeQuantity { get; set; } = 1;

        [InputParameter("Hedge cooldown (ms)", 33, 100, 600000, 100, 0)]
        public int HedgeCooldownMs { get; set; } = 3000;

        [InputParameter("Hedge symbol", 34)]
        public Symbol? HedgeSymbol { get; set; }

        [InputParameter("Hedge account", 35)]
        public Account? HedgeAccount { get; set; }

        public MarketMakingStrategy()
        {
            Name = "Goliath Market Making";
            Description = "Inventory-aware market making loop with DOM skewing and optional hedge execution.";
        }

        public override string[] MonitoringConnectionsIds
            => TradingSymbol?.ConnectionId is { } id ? new[] { id } : Array.Empty<string>();

        protected override void OnRun()
        {
            base.OnRun();
            ValidateInputs();

            _gateway = new QuantowerTradingGateway(this);

            var tickSize = TradingSymbol!.TickSize > 0 ? (decimal)TradingSymbol.TickSize : 0.25m;
            var config = new MarketMakingConfig
            {
                Symbol = TradingSymbol!.Name,
                TickSize = tickSize,
                OrderQuantity = (decimal)OrderQuantity,
                BaseSpread = (decimal)BaseSpreadTicks * tickSize,
                MinSpread = (decimal)MinSpreadTicks * tickSize,
                InventoryTarget = (decimal)InventoryTarget,
                InventoryRange = Math.Max(1m, (decimal)InventoryRange),
                InventorySkewWeight = (decimal)InventoryWeight,
                DomSkewWeight = (decimal)DomWeight,
                MaxPriceShift = (decimal)MaxSkewTicks * tickSize,
                QuoteInterval = TimeSpan.FromMilliseconds(QuoteIntervalMs),
                MaxRequoteAttempts = MaxRequoteAttempts,
                Hedging = new HedgeConfig
                {
                    Enabled = HedgeEnabled,
                    Threshold = (decimal)HedgeThreshold,
                    OrderQuantity = (decimal)HedgeQuantity,
                    Cooldown = TimeSpan.FromMilliseconds(HedgeCooldownMs)
                }
            };

            _engine = new MarketMakingEngine(config, _gateway);
            _cts = new CancellationTokenSource();
            _ = _engine.StartAsync(_cts.Token);

            TradingSymbol!.NewQuote += OnQuote;

            try
            {
                decimal initialInventory = _gateway
                    .GetNetPositionAsync(config.Symbol, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();
                _engine.UpdateInventory(initialInventory);
            }
            catch (Exception ex)
            {
                Log($"Failed to seed inventory: {ex.Message}", StrategyLoggingLevel.Error);
            }
        }

        protected override void OnStop()
        {
            base.OnStop();
            if (TradingSymbol != null)
            {
                TradingSymbol.NewQuote -= OnQuote;
            }
            var cts = Interlocked.Exchange(ref _cts, null);
            cts?.Cancel();
            cts?.Dispose();

            if (_engine != null)
            {
                _engine.StopAsync().GetAwaiter().GetResult();
                _engine.Dispose();
                _engine = null;
            }

            _gateway?.Dispose();
            _gateway = null;
        }

        private DateTime _lastInventoryRefreshUtc = DateTime.MinValue;

        private void OnQuote(Symbol symbol, Quote quote)
        {
            if (_engine == null || quote == null)
            {
                return;
            }

            if (!SymbolsEqual(symbol, TradingSymbol!))
            {
                return;
            }

            decimal bidPrice = quote.Bid > 0 ? (decimal)quote.Bid : 0m;
            decimal askPrice = quote.Ask > 0 ? (decimal)quote.Ask : 0m;
            decimal bidVolume = quote.BidSize > 0 ? (decimal)quote.BidSize : 0m;
            decimal askVolume = quote.AskSize > 0 ? (decimal)quote.AskSize : 0m;

            _engine.UpdateDomSnapshot(new DomSnapshot(bidPrice, bidVolume, askPrice, askVolume));

            var lastPrice = TryExtractLastPrice(quote);
            if (lastPrice.HasValue)
            {
                _engine.UpdateLastTradePrice(lastPrice.Value);
            }

            if ((DateTime.UtcNow - _lastInventoryRefreshUtc).TotalMilliseconds >= Math.Max(100, QuoteIntervalMs))
            {
                RefreshInventorySnapshot();
                _lastInventoryRefreshUtc = DateTime.UtcNow;
            }
        }

        private void RefreshInventorySnapshot()
        {
            if (_engine == null || _gateway == null)
            {
                return;
            }

            try
            {
                decimal netInventory = _gateway
                    .GetNetPositionAsync(TradingSymbol!.Name, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();
                _engine.UpdateInventory(netInventory);
            }
            catch (Exception ex)
            {
                Log($"Failed to refresh inventory: {ex.Message}", StrategyLoggingLevel.Trading);
            }
        }

        private void ValidateInputs()
        {
            if (TradingAccount == null)
            {
                throw new InvalidOperationException("Trading account must be selected.");
            }

            if (TradingSymbol == null)
            {
                throw new InvalidOperationException("Symbol must be selected.");
            }

            if (OrderQuantity <= 0)
            {
                throw new InvalidOperationException("Order quantity must be positive.");
            }
        }

        private static decimal? TryExtractLastPrice(Quote quote)
        {
            if (quote == null)
            {
                return null;
            }

            try
            {
                foreach (var propertyName in new[] { "Last", "LastPrice", "LastTradedPrice", "TradePrice" })
                {
                    var property = quote.GetType().GetProperty(propertyName);
                    if (property == null)
                    {
                        continue;
                    }

                    var value = property.GetValue(quote);
                    if (value == null)
                    {
                        continue;
                    }

                    var numeric = Convert.ToDecimal(value);
                    if (numeric > 0)
                    {
                        return numeric;
                    }
                }

                var tradeProperty = quote.GetType().GetProperty("Trade");
                if (tradeProperty != null)
                {
                    var trade = tradeProperty.GetValue(quote);
                    if (trade != null)
                    {
                        foreach (var propertyName in new[] { "Price", "LastPrice" })
                        {
                            var priceProperty = trade.GetType().GetProperty(propertyName);
                            if (priceProperty == null)
                            {
                                continue;
                            }

                            var value = priceProperty.GetValue(trade);
                            if (value == null)
                            {
                                continue;
                            }

                            var numeric = Convert.ToDecimal(value);
                            if (numeric > 0)
                            {
                                return numeric;
                            }
                        }
                    }
                }
            }
            catch
            {
                // Ignore reflection failures and fall back to DOM-driven quoting.
            }

            return null;
        }

        private static bool SymbolsEqual(Symbol left, Symbol right)
        {
            return string.Equals(left?.Id, right?.Id, StringComparison.OrdinalIgnoreCase)
                   || string.Equals(left?.Name, right?.Name, StringComparison.OrdinalIgnoreCase);
        }

        private sealed class QuantowerTradingGateway : ITradingGateway, IDisposable
        {
            private readonly MarketMakingStrategy _owner;
            private static readonly string[] OrderMetadataPropertyCandidates =
            {
                "CustomTag",
                "Comment",
                "Tag",
                "ClientOrderId",
                "ClientId",
                "ClientID",
                "Text",
                "UserTag"
            };

            private readonly HashSet<string> _openClientOrderIds = new(StringComparer.Ordinal);
            private readonly Dictionary<string, OrderSide> _clientOrderSides = new(StringComparer.Ordinal);
            private readonly Dictionary<string, decimal> _clientOrderPrices = new(StringComparer.Ordinal);
            private readonly Dictionary<string, Account> _clientOrderAccounts = new(StringComparer.Ordinal);
            private readonly Dictionary<string, Symbol> _clientOrderSymbols = new(StringComparer.Ordinal);
            private readonly HashSet<string> _pendingCancelClientOrderIds = new(StringComparer.Ordinal);

            public QuantowerTradingGateway(MarketMakingStrategy owner)
            {
                _owner = owner;
            }

            public Task<OrderTicket> PlaceOrReplaceAsync(string symbol, OrderSide side, decimal price, decimal quantity, string? replaceOrderId, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (replaceOrderId != null)
                {
                    bool wasTracked = _openClientOrderIds.Contains(replaceOrderId);
                    CancelInternal(replaceOrderId);
                    if (wasTracked && _openClientOrderIds.Contains(replaceOrderId))
                    {
                        throw new PendingCancelException(replaceOrderId);
                    }
                }

                string clientOrderId = CreateClientOrderId();
                var request = new PlaceOrderRequestParameters
                {
                    Account = _owner.TradingAccount!,
                    Symbol = _owner.TradingSymbol!,
                    Side = side == OrderSide.Buy ? Side.Buy : Side.Sell,
                    Quantity = (double)quantity,
                    Price = (double)price,
                    OrderTypeId = OrderType.Limit,
                    TimeInForce = TimeInForce.GTC,
                };

                TryAssignOrderMetadata(request, clientOrderId);

                Core.Instance.PlaceOrder(request);

                _openClientOrderIds.Add(clientOrderId);
                _clientOrderSides[clientOrderId] = side;
                _clientOrderPrices[clientOrderId] = price;
                _clientOrderAccounts[clientOrderId] = request.Account;
                _clientOrderSymbols[clientOrderId] = request.Symbol;

                return Task.FromResult(new OrderTicket(clientOrderId));
            }

            public Task CancelAsync(string orderId, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();
                CancelInternal(orderId);
                return Task.CompletedTask;
            }

            public Task<decimal> GetNetPositionAsync(string symbol, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var positions = Core.Instance.Positions?.ToArray();
                if (positions != null)
                {
                    foreach (var pos in positions)
                    {
                        if (pos != null && AccountsEqual(pos.Account, _owner.TradingAccount!) && SymbolsEqual(pos.Symbol, _owner.TradingSymbol!))
                        {
                            return Task.FromResult((decimal)pos.Quantity);
                        }
                    }
                }

                return Task.FromResult(0m);
            }

            public Task<OrderTicket> HedgeAsync(string symbol, OrderSide side, decimal price, decimal quantity, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var account = _owner.HedgeAccount ?? _owner.TradingAccount;
                var hedgeSymbol = _owner.HedgeSymbol ?? _owner.TradingSymbol;
                if (account == null || hedgeSymbol == null)
                {
                    throw new InvalidOperationException("Hedge account and symbol must be configured to place hedge orders.");
                }

                string clientOrderId = CreateClientOrderId();
                var request = new PlaceOrderRequestParameters
                {
                    Account = account,
                    Symbol = hedgeSymbol,
                    Side = side == OrderSide.Buy ? Side.Buy : Side.Sell,
                    Quantity = (double)quantity,
                    Price = (double)price,
                    OrderTypeId = OrderType.Limit,
                    TimeInForce = TimeInForce.GTC,
                };

                TryAssignOrderMetadata(request, clientOrderId);

                Core.Instance.PlaceOrder(request);
                _openClientOrderIds.Add(clientOrderId);
                _clientOrderSides[clientOrderId] = side;
                _clientOrderPrices[clientOrderId] = price;
                if (account != null)
                {
                    _clientOrderAccounts[clientOrderId] = account;
                }
                if (hedgeSymbol != null)
                {
                    _clientOrderSymbols[clientOrderId] = hedgeSymbol;
                }
                return Task.FromResult(new OrderTicket(clientOrderId));
            }

            public void Dispose()
            {
                foreach (var clientId in _openClientOrderIds.ToArray())
                {
                    CancelInternal(clientId);
                }
            }

            private string CreateClientOrderId()
            {
                return "GMM-" + Guid.NewGuid().ToString("N");
            }

            private bool CancelInternal(string clientOrderId)
            {
                if (string.IsNullOrEmpty(clientOrderId))
                {
                    return false;
                }

                bool hasSide = _clientOrderSides.TryGetValue(clientOrderId, out var side);
                bool hasPrice = _clientOrderPrices.TryGetValue(clientOrderId, out var price);
                bool hasAccount = _clientOrderAccounts.TryGetValue(clientOrderId, out var account);
                bool hasSymbol = _clientOrderSymbols.TryGetValue(clientOrderId, out var symbol);

                var orders = Core.Instance.Orders?.ToArray();
                if (orders == null || orders.Length == 0)
                {
                    LogPendingCancel(clientOrderId);
                    return false;
                }

                IOrder? matched = null;

                foreach (var candidate in orders)
                {
                    if (candidate == null)
                    {
                        continue;
                    }

                    if (hasAccount)
                    {
                        if (account != null && !AccountsEqual(candidate.Account, account))
                        {
                            continue;
                        }
                    }
                    else if (_owner.TradingAccount != null && !AccountsEqual(candidate.Account, _owner.TradingAccount))
                    {
                        continue;
                    }

                    if (hasSymbol)
                    {
                        if (symbol != null && !SymbolsEqual(candidate.Symbol, symbol))
                        {
                            continue;
                        }
                    }
                    else if (_owner.TradingSymbol != null && !SymbolsEqual(candidate.Symbol, _owner.TradingSymbol))
                    {
                        continue;
                    }

                    var metadata = TryGetOrderMetadata(candidate);
                    if (!string.IsNullOrEmpty(metadata)
                        && string.Equals(metadata, clientOrderId, StringComparison.Ordinal))
                    {
                        matched = candidate;
                        break;
                    }

                    if (matched == null && hasSide)
                    {
                        if (candidate.Side == (side == OrderSide.Buy ? Side.Buy : Side.Sell))
                        {
                            if (hasPrice && price != 0 && TryConvertPrice(candidate, out var candidatePrice))
                            {
                                var tickSize = (decimal)(_owner.TradingSymbol?.TickSize ?? 0);
                                var tolerance = tickSize > 0 ? tickSize : 0.01m;
                                if (Math.Abs(candidatePrice - price) <= tolerance)
                                {
                                    matched = candidate;
                                }
                            }
                            else if (!hasPrice || price == 0)
                            {
                                matched = candidate;
                            }
                        }
                    }
                }

                if (matched != null)
                {
                    try
                    {
                        Core.Instance.CancelOrder(matched);
                        _openClientOrderIds.Remove(clientOrderId);
                        _clientOrderSides.Remove(clientOrderId);
                        _clientOrderPrices.Remove(clientOrderId);
                        _clientOrderAccounts.Remove(clientOrderId);
                        _clientOrderSymbols.Remove(clientOrderId);
                        _pendingCancelClientOrderIds.Remove(clientOrderId);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        _owner.Log($"Failed to cancel order {clientOrderId}: {ex.Message}", StrategyLoggingLevel.Trading);
                    }
                }

                LogPendingCancel(clientOrderId);
                return false;
            }

            private void LogPendingCancel(string clientOrderId)
            {
                if (!_openClientOrderIds.Contains(clientOrderId))
                {
                    return;
                }

                if (_pendingCancelClientOrderIds.Add(clientOrderId))
                {
                    _owner.Log(
                        $"Cancel pending for order {clientOrderId}; awaiting platform acknowledgement.",
                        StrategyLoggingLevel.Trading);
                }
            }

            private static bool AccountsEqual(Account left, Account right)
            {
                return string.Equals(left?.Id, right?.Id, StringComparison.OrdinalIgnoreCase)
                       || string.Equals(left?.Name, right?.Name, StringComparison.OrdinalIgnoreCase);
            }

            private static void TryAssignOrderMetadata(object request, string clientOrderId)
            {
                if (request == null || string.IsNullOrEmpty(clientOrderId))
                {
                    return;
                }

                try
                {
                    var type = request.GetType();
                    foreach (var propertyName in OrderMetadataPropertyCandidates)
                    {
                        var property = type.GetProperty(propertyName);
                        if (property == null || !property.CanWrite)
                        {
                            continue;
                        }

                        if (property.PropertyType == typeof(string))
                        {
                            property.SetValue(request, clientOrderId);
                            return;
                        }

                        try
                        {
                            property.SetValue(request, Convert.ChangeType(clientOrderId, property.PropertyType));
                            return;
                        }
                        catch
                        {
                            // Continue probing other property names.
                        }
                    }
                }
                catch
                {
                    // Ignore reflection failures; metadata tagging is best-effort.
                }
            }

            private static string? TryGetOrderMetadata(IOrder order)
            {
                if (order == null)
                {
                    return null;
                }

                try
                {
                    var type = order.GetType();
                    foreach (var propertyName in OrderMetadataPropertyCandidates)
                    {
                        var property = type.GetProperty(propertyName);
                        if (property == null || !property.CanRead)
                        {
                            continue;
                        }

                        var value = property.GetValue(order);
                        if (value == null)
                        {
                            continue;
                        }

                        var text = Convert.ToString(value);
                        if (!string.IsNullOrEmpty(text))
                        {
                            return text;
                        }
                    }
                }
                catch
                {
                    // Ignore reflection failures and fall back to other matching heuristics.
                }

                return null;
            }

            private static bool TryConvertPrice(IOrder order, out decimal price)
            {
                try
                {
                    price = Convert.ToDecimal(order.Price);
                    return price > 0;
                }
                catch
                {
                    price = 0m;
                    return false;
                }
            }
        }
    }
}
