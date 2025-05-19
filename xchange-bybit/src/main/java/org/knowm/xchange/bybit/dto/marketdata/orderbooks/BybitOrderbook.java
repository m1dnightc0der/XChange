package org.knowm.xchange.bybit.dto.marketdata.orderbooks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;

@Getter
public class BybitOrderbook {

  private final List<BybitPublicOrder> asks;

  private final List<BybitPublicOrder> bids;
  private final String ts;
  private final String cts;

  @JsonCreator
  public BybitOrderbook(
      @JsonProperty("a") List<BybitPublicOrder> asks,
      @JsonProperty("b") List<BybitPublicOrder> bids,
      @JsonProperty("ts") String ts,
      @JsonProperty("cts") String cts)
  {

    this.asks = asks;
    this.bids = bids;
    this.ts = ts;
    this.cts = cts;
  }

  @Override
  public String toString() {
    return "BybitOrderbookResponse{ts=" + ts+ ", cts=" + cts + ", asks=" + asks + ", bids=" + bids + '}';
  }
}
