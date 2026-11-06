package info.bitrich.xchangestream.hyperliquid.p2p;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.knowm.xchange.dto.Order.OrderType;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;
import org.knowm.xchange.dto.trade.LimitOrder;
import org.knowm.xchange.instrument.Instrument;

/** Single-writer full-depth L3 to L2 reconstruction. Never expose mutable internal state. */
public final class P2pOrderBook {
  private final String coin;
  private final Map<BigInteger, Entry> ordersById = new HashMap<>();
  private final NavigableMap<BigDecimal, BigDecimal> bids = new TreeMap<>(Collections.reverseOrder());
  private final NavigableMap<BigDecimal, BigDecimal> asks = new TreeMap<>();

  public P2pOrderBook(String coin) {
    if (coin == null || coin.isEmpty()) throw new IllegalArgumentException("Missing coin");
    this.coin = coin;
  }

  public void seed(List<P2pWireCodec.Change> orders) {
    if (!ordersById.isEmpty()) throw new IllegalStateException("Seed requires an empty book");
    for (P2pWireCodec.Change order : orders) {
      if (!order.present || ordersById.containsKey(order.oid)) {
        throw new IllegalArgumentException("Duplicate or removed snapshot order");
      }
      apply(order);
    }
    validate();
  }

  /** Validate only a complete seed/frame, never its temporary intermediate order states. */
  void validate() {
    if (!bids.isEmpty() && !asks.isEmpty() && bids.firstKey().compareTo(asks.firstKey()) >= 0) {
      throw new IllegalArgumentException("Crossed or locked book for " + coin);
    }
  }

  public void apply(P2pWireCodec.Change change) {
    if (!coin.equals(change.coin) || change.size == null || change.size.signum() < 0
        || change.price == null || change.price.signum() < 0) {
      throw new IllegalArgumentException("Invalid order values or membership");
    }
    P2pWireCodec.unsigned(change.oid);
    Entry previous = ordersById.get(change.oid);
    if (!change.present) {
      if (previous != null) {
        adjust(previous.bid, previous.price, previous.size.negate());
        ordersById.remove(change.oid);
      }
      return;
    }
    // The reference protocol resizes a known oid in place; incoming price/side are not amendments.
    boolean bid = previous == null ? change.bid : previous.bid;
    BigDecimal price = previous == null ? normalize(change.price) : previous.price;
    if (price.signum() <= 0) throw new IllegalArgumentException("Nonpositive order price");
    if (previous != null) adjust(previous.bid, previous.price, previous.size.negate());
    Entry next = new Entry(bid, price, change.size);
    ordersById.put(change.oid, next);
    adjust(bid, price, change.size);
  }

  private void adjust(boolean bid, BigDecimal price, BigDecimal delta) {
    NavigableMap<BigDecimal, BigDecimal> levels = bid ? bids : asks;
    BigDecimal size = levels.getOrDefault(price, BigDecimal.ZERO).add(delta);
    if (size.signum() < 0) throw new IllegalStateException("Negative aggregate");
    if (size.signum() == 0) levels.remove(price);
    else levels.put(price, size);
  }

  private static BigDecimal normalize(BigDecimal value) { return value.stripTrailingZeros(); }

  public int getOrderCount() { return ordersById.size(); }

  public View snapshot(long receivedAtMicros, long receivedAtNanos) {
    validate();
    return new View(coin, bids, asks, receivedAtMicros, receivedAtNanos);
  }

  private static final class Entry {
    final boolean bid;
    final BigDecimal price;
    final BigDecimal size;
    Entry(boolean bid, BigDecimal price, BigDecimal size) {
      this.bid = bid;
      this.price = price;
      this.size = size;
    }
  }

  /** Immutable detached numeric levels. DTOs are freshly allocated per observer. */
  public static final class View {
    public final String coin;
    public final NavigableMap<BigDecimal, BigDecimal> bids;
    public final NavigableMap<BigDecimal, BigDecimal> asks;
    public final long receivedAtMicros;
    public final long receivedAtNanos;
    private View(String coin, NavigableMap<BigDecimal, BigDecimal> bids,
        NavigableMap<BigDecimal, BigDecimal> asks, long micros, long nanos) {
      this.coin = coin;
      this.bids = Collections.unmodifiableNavigableMap(new TreeMap<>(bids));
      this.asks = Collections.unmodifiableNavigableMap(new TreeMap<>(asks));
      this.receivedAtMicros = micros;
      this.receivedAtNanos = nanos;
    }

    public OrderBook toOrderBook(Instrument instrument) {
      return new OrderBook(new Date(receivedAtMicros / 1000),
          levels(asks, OrderType.ASK, instrument), levels(bids, OrderType.BID, instrument));
    }

    private List<LimitOrder> levels(Map<BigDecimal, BigDecimal> levels, OrderType side,
        Instrument instrument) {
      List<LimitOrder> result = new ArrayList<>();
      levels.forEach((price, size) -> result.add(new LimitOrder(side, size, instrument, null,
          new Date(receivedAtMicros / 1000), price)));
      return Collections.unmodifiableList(result);
    }

    public Ticker toTicker(Instrument instrument) {
      Ticker.Builder builder = new Ticker.Builder().instrument(instrument)
          .timestamp(new Date(receivedAtMicros / 1000));
      if (!bids.isEmpty()) builder.bid(bids.firstKey()).bidSize(bids.firstEntry().getValue());
      if (!asks.isEmpty()) builder.ask(asks.firstKey()).askSize(asks.firstEntry().getValue());
      return builder.build();
    }
  }
}
