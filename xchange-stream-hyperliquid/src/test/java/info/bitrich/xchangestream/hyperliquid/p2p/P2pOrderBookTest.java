package info.bitrich.xchangestream.hyperliquid.p2p;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import org.junit.Test;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.dto.marketdata.OrderBook;
import org.knowm.xchange.dto.marketdata.Ticker;

public class P2pOrderBookTest {
  @Test public void completeSeedsAndDetachedViewsRejectCrossedAndLockedBooks() {
    for (String bid : Arrays.asList("100", "101")) {
      P2pOrderBook book = new P2pOrderBook("BTC");
      assertThrows(IllegalArgumentException.class, () -> book.seed(Arrays.asList(
          SyntheticWire.order(1, true, "BTC", bid, "1"),
          SyntheticWire.order(2, false, "BTC", "100", "1"))));
      assertThrows(IllegalArgumentException.class, () -> book.snapshot(0, 0));
    }
  }

  @Test public void temporaryCrossingIsAllowedUntilCompleteFrameValidation() {
    P2pOrderBook book = new P2pOrderBook("BTC");
    book.seed(Arrays.asList(SyntheticWire.order(1, true, "BTC", "99", "1"),
        SyntheticWire.order(2, false, "BTC", "100", "1")));
    book.apply(SyntheticWire.order(3, true, "BTC", "101", "1"));
    assertThrows(IllegalArgumentException.class, book::validate);
    book.apply(SyntheticWire.change(BigInteger.valueOf(2), false, false, "BTC", "0", "0"));
    book.apply(SyntheticWire.order(4, false, "BTC", "102", "1"));
    book.validate();
    assertEquals(0, new BigDecimal("101").compareTo(book.snapshot(0, 0).bids.firstKey()));
    assertEquals(0, new BigDecimal("102").compareTo(book.snapshot(0, 0).asks.firstKey()));
  }

  @Test public void numericLevelsMergeAndKnownOidResizesOnly() {
    P2pOrderBook book = new P2pOrderBook("BTC");
    book.seed(Arrays.asList(SyntheticWire.order(1, true, "BTC", "50000.0", "1.20"),
        SyntheticWire.order(2, true, "BTC", "50000", "2.3")));
    book.apply(SyntheticWire.order(1, false, "BTC", "99999", "4.0"));
    P2pOrderBook.View view = book.snapshot(123000, 100);
    assertEquals(1, view.bids.size());
    assertEquals(0, view.bids.firstKey().compareTo(new BigDecimal("50000")));
    assertEquals(0, view.bids.firstEntry().getValue().compareTo(new BigDecimal("6.3")));
    assertTrue(view.asks.isEmpty());
    book.apply(SyntheticWire.change(BigInteger.valueOf(99), false, false, "BTC", "0", "0"));
    assertEquals(2, book.getOrderCount());
    book.apply(SyntheticWire.change(BigInteger.ONE, false, false, "BTC", "0", "0"));
    assertEquals(1, book.getOrderCount());
    assertEquals(0, book.snapshot(0, 0).bids.firstEntry().getValue().compareTo(new BigDecimal("2.3")));
    assertEquals(0, view.bids.firstEntry().getValue().compareTo(new BigDecimal("6.3")));
  }

  @Test public void dtoTimestampsUseExchangeMicrosTruncatedToMillis() {
    P2pOrderBook book = new P2pOrderBook("BTC");
    book.seed(Arrays.asList(SyntheticWire.order(1, true, "BTC", "99", "1"),
        SyntheticWire.order(2, false, "BTC", "101", "2")));
    long blockTimeMicros = 1_700_000_000_123_456L;
    P2pOrderBook.View view = book.snapshot(blockTimeMicros, 987_654_321L);
    FuturesContract instrument = new FuturesContract("BTC/USDC/PERP");
    OrderBook dto = view.toOrderBook(instrument);
    long expectedMillis = blockTimeMicros / 1000;
    assertEquals(expectedMillis, dto.getTimeStamp().getTime());
    dto.getBids().forEach(order -> assertEquals(expectedMillis, order.getTimestamp().getTime()));
    dto.getAsks().forEach(order -> assertEquals(expectedMillis, order.getTimestamp().getTime()));
    assertEquals(expectedMillis, view.toTicker(instrument).getTimestamp().getTime());
  }

  @Test public void blockTimestampPreservesUnsignedMicrosAndDetachedProvenance() {
    P2pOrderBook book = new P2pOrderBook("BTC");
    book.seed(Arrays.asList(SyntheticWire.order(1, true, "BTC", "99", "1"),
        SyntheticWire.order(2, false, "BTC", "101", "2")));
    BigInteger micros = P2pWireCodec.MAX_U64;
    P2pOrderBook.View view = book.snapshot(micros, 123, BigInteger.valueOf(101), 2);
    FuturesContract instrument = new FuturesContract("BTC/USDC/PERP");
    OrderBook first = view.toOrderBook(instrument);
    OrderBook second = view.toOrderBook(instrument);
    assertEquals(micros, view.createdAtMicros);
    assertEquals(123, view.receivedAtNanos);
    assertTrue(first.getTimeStamp() instanceof P2pOrderBook.BlockTimestamp);
    P2pOrderBook.BlockTimestamp timestamp = (P2pOrderBook.BlockTimestamp) first.getTimeStamp();
    long expectedMillis = micros.divide(BigInteger.valueOf(1000)).longValueExact();
    assertEquals(expectedMillis, timestamp.getTime());
    assertEquals(micros, timestamp.getCreatedAtMicros());
    assertEquals(BigInteger.valueOf(101), timestamp.getBlockHeight());
    assertEquals(2, timestamp.getFramesAtHeight());
    assertNotSame(first.getTimeStamp(), second.getTimeStamp());
    timestamp.setTime(0); // DTO Date mutability cannot change immutable provenance or retained views.
    assertEquals(micros, timestamp.getCreatedAtMicros());
    assertEquals(expectedMillis, second.getTimeStamp().getTime());
    assertEquals(expectedMillis, first.getBids().get(0).getTimestamp().getTime());
    assertEquals(expectedMillis, first.getAsks().get(0).getTimestamp().getTime());
    assertEquals(expectedMillis, view.toTicker(instrument).getTimestamp().getTime());
    assertEquals(expectedMillis, view.toOrderBook(instrument).getTimeStamp().getTime());
  }

  @Test public void snapshotWithoutAppliedDiffCannotClaimCursorProvenance() {
    P2pOrderBook book = new P2pOrderBook("BTC");
    FuturesContract instrument = new FuturesContract("BTC/USDC/PERP");
    P2pOrderBook.View unknown = book.snapshot(null, 123, null, 0);
    assertNull(unknown.createdAtMicros);
    assertNull(unknown.toOrderBook(instrument).getTimeStamp());
    assertNull(unknown.toTicker(instrument).getTimestamp());
    assertEquals(123, unknown.receivedAtNanos);
    assertFalse(book.snapshot(1234567, 123).toOrderBook(instrument).getTimeStamp()
        instanceof P2pOrderBook.BlockTimestamp);
    assertThrows(IllegalArgumentException.class, () -> book.snapshot(null, 123, BigInteger.ONE, 1));
    assertThrows(IllegalArgumentException.class, () -> book.snapshot(BigInteger.ONE, 123, null, 1));
    assertThrows(IllegalArgumentException.class, () -> book.snapshot(BigInteger.ONE, 123, BigInteger.ONE, 0));
  }

  @Test public void fullDepthDetachedSortedViewsAndBboWithoutInventedLast() {
    P2pOrderBook book = new P2pOrderBook("BTC");
    for (int i = 1; i <= 60; i++) {
      book.apply(SyntheticWire.order(i, true, "BTC", String.valueOf(100 - i), "1"));
      book.apply(SyntheticWire.order(1000 + i, false, "BTC", String.valueOf(100 + i), "2"));
    }
    P2pOrderBook.View view = book.snapshot(1234567, 999);
    FuturesContract instrument = new FuturesContract("BTC/USDC/PERP");
    OrderBook first = view.toOrderBook(instrument);
    OrderBook second = view.toOrderBook(instrument);
    assertEquals(60, first.getBids().size());
    assertEquals(60, first.getAsks().size());
    assertEquals(new BigDecimal("99"), first.getBids().get(0).getLimitPrice());
    assertEquals(new BigDecimal("101"), first.getAsks().get(0).getLimitPrice());
    assertNotSame(first.getBids().get(0), second.getBids().get(0));
    assertNotSame(first.getAsks().get(0), second.getAsks().get(0));
    // The host sorts both DTO sides in place. DTO ownership must not leak into retained views.
    Collections.reverse(first.getBids());
    Collections.reverse(first.getAsks());
    first.getBids().sort(Comparator.comparing(o -> o.getLimitPrice(), Comparator.reverseOrder()));
    first.getAsks().sort(Comparator.comparing(o -> o.getLimitPrice()));
    assertEquals(new BigDecimal("99"), first.getBids().get(0).getLimitPrice());
    assertEquals(new BigDecimal("101"), first.getAsks().get(0).getLimitPrice());
    first.getBids().clear();
    first.getAsks().clear();
    assertEquals(60, second.getBids().size());
    assertEquals(60, second.getAsks().size());
    assertEquals(60, view.toOrderBook(instrument).getBids().size());
    assertThrows(UnsupportedOperationException.class, () -> view.bids.clear());
    assertThrows(UnsupportedOperationException.class, () -> view.asks.clear());
    first.getTimeStamp().setTime(0);
    assertEquals(1234, second.getTimeStamp().getTime());
    Ticker ticker = view.toTicker(instrument);
    assertEquals(new BigDecimal("99"), ticker.getBid());
    assertEquals(new BigDecimal("101"), ticker.getAsk());
    assertEquals(BigDecimal.ONE, ticker.getBidSize());
    assertEquals(new BigDecimal("2"), ticker.getAskSize());
    assertNull(ticker.getLast());
  }

  @Test public void emptyAndOneSidedDtosAreSortableAndDetachedFromLaterChanges() {
    P2pOrderBook book = new P2pOrderBook("HYPE");
    FuturesContract instrument = new FuturesContract("HYPE/USDC/PERP");
    OrderBook empty = book.snapshot(0, 0).toOrderBook(instrument);
    empty.getBids().sort(Comparator.comparing(o -> o.getLimitPrice()));
    empty.getAsks().sort(Comparator.comparing(o -> o.getLimitPrice()));
    book.apply(SyntheticWire.order(1, true, "HYPE", "20", "2"));
    P2pOrderBook.View retained = book.snapshot(2000, 1000);
    OrderBook oneSided = retained.toOrderBook(instrument);
    oneSided.getBids().sort(Comparator.comparing(o -> o.getLimitPrice()));
    oneSided.getAsks().sort(Comparator.comparing(o -> o.getLimitPrice()));
    book.apply(SyntheticWire.order(1, true, "HYPE", "20", "3"));
    assertTrue(empty.getBids().isEmpty());
    assertEquals(new BigDecimal("2"), oneSided.getBids().get(0).getOriginalAmount());
    assertEquals(new BigDecimal("2"), retained.toOrderBook(instrument).getBids().get(0).getOriginalAmount());
    assertEquals(new BigDecimal("3"), book.snapshot(3000, 2000).toOrderBook(instrument)
        .getBids().get(0).getOriginalAmount());
  }

  @Test public void zeroSizeRemainsKnownForResizeAndInvalidChangesFail() {
    P2pOrderBook book = new P2pOrderBook("BTC");
    book.apply(SyntheticWire.order(1, true, "BTC", "10", "0"));
    assertEquals(1, book.getOrderCount());
    assertTrue(book.snapshot(0, 0).bids.isEmpty());
    book.apply(SyntheticWire.order(1, false, "BTC", "20", "1"));
    assertEquals(0, new BigDecimal("10").compareTo(book.snapshot(0, 0).bids.firstKey()));
    assertThrows(IllegalArgumentException.class,
        () -> book.apply(SyntheticWire.order(2, true, "BTC", "0", "1")));
    assertThrows(IllegalArgumentException.class,
        () -> book.apply(SyntheticWire.order(2, true, "BTC", "10", "-1")));
    assertThrows(IllegalArgumentException.class,
        () -> book.apply(SyntheticWire.order(2, true, "ETH", "10", "1")));
  }
}
