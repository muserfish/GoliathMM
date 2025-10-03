using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TradingPlatform.BusinessLayer;

#nullable enable

namespace Quantower.Strategies
{
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
        }

        protected override void OnStop()
        {
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

        protected override void OnQuote(Quote quote)
        {
            base.OnQuote(quote);

            if (_engine == null || quote == null)
            {
                return;
            }

            decimal bidPrice = quote.Bid > 0 ? (decimal)quote.Bid : 0m;
            decimal askPrice = quote.Ask > 0 ? (decimal)quote.Ask : 0m;
            decimal bidVolume = quote.BidSize > 0 ? (decimal)quote.BidSize : 0m;
            decimal askVolume = quote.AskSize > 0 ? (decimal)quote.AskSize : 0m;

            _engine.UpdateDomSnapshot(new DomSnapshot(bidPrice, bidVolume, askPrice, askVolume));

            if (quote.Last > 0)
            {
                _engine.UpdateLastTradePrice((decimal)quote.Last);
            }
        }

        protected override void OnPositionChanged(Position position, PositionEventArgs args)
        {
            base.OnPositionChanged(position, args);

            if (_engine == null || position?.Symbol == null || TradingSymbol == null)
            {
                return;
            }

            if (!SymbolsEqual(position.Symbol, TradingSymbol))
            {
                return;
            }

            _engine.UpdateInventory((decimal)position.Quantity);
        }

        protected override void OnDestroy()
        {
            base.OnDestroy();
            _engine?.Dispose();
            _gateway?.Dispose();
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

        private static bool SymbolsEqual(Symbol left, Symbol right)
        {
            return string.Equals(left?.Id, right?.Id, StringComparison.OrdinalIgnoreCase)
                   || string.Equals(left?.Name, right?.Name, StringComparison.OrdinalIgnoreCase);
        }

        private sealed class QuantowerTradingGateway : ITradingGateway, IDisposable
        {
            private readonly MarketMakingStrategy _owner;
            private readonly HashSet<string> _openClientOrderIds = new(StringComparer.Ordinal);

            public QuantowerTradingGateway(MarketMakingStrategy owner)
            {
                _owner = owner;
            }

            public Task<OrderTicket> PlaceOrReplaceAsync(string symbol, OrderSide side, decimal price, decimal quantity, string? replaceOrderId, CancellationToken cancellationToken)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (replaceOrderId != null)
                {
                    CancelInternal(replaceOrderId);
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
                    CustomTag = clientOrderId
                };

                Core.Instance.PlaceOrder(request);

                _openClientOrderIds.Add(clientOrderId);

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
                    CustomTag = clientOrderId
                };

                Core.Instance.PlaceOrder(request);
                _openClientOrderIds.Add(clientOrderId);
                return Task.FromResult(new OrderTicket(clientOrderId));
            }

            public void Dispose()
            {
                foreach (var clientId in _openClientOrderIds.ToArray())
                {
                    CancelInternal(clientId);
                }
                _openClientOrderIds.Clear();
            }

            private string CreateClientOrderId()
            {
                return "GMM-" + Guid.NewGuid().ToString("N");
            }

            private void CancelInternal(string clientOrderId)
            {
                if (!_openClientOrderIds.Remove(clientOrderId))
                {
                    return;
                }

                IOrder? order = null;

                order ??= Core.Instance.Orders?
                    .FirstOrDefault(o => o != null && string.Equals(o.CustomTag, clientOrderId, StringComparison.Ordinal));

                if (order != null)
                {
                    Core.Instance.CancelOrder(order);
                }
            }

            private static bool AccountsEqual(Account left, Account right)
            {
                return string.Equals(left?.Id, right?.Id, StringComparison.OrdinalIgnoreCase)
                       || string.Equals(left?.Name, right?.Name, StringComparison.OrdinalIgnoreCase);
            }
        }
    }
}
