package org.knowm.xchange.okex.v5.dto.marketdata;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OkexOrderbook {

  private final List<OkexPublicOrder> asks;

  private final List<OkexPublicOrder> bids;
  private final Date ts;

  @JsonCreator
  public OkexOrderbook(
      @JsonProperty("asks") List<OkexPublicOrder> asks,
      @JsonProperty("bids") List<OkexPublicOrder> bids,
      @JsonProperty("ts") Date ts) {

    this.asks = asks;
    this.bids = bids;
    this.ts = ts;
  }

  public List<OkexPublicOrder> getAsks() {
    return asks;
  }

  public List<OkexPublicOrder> getBids() {
    return bids;
  }

  public Date getTs() {
    return ts;
  }

  @Override
  public String toString() {
    return "OkexOrderbookResponse{ts=" + ts + "asks=" + asks + ", bids=" + bids + '}';
  }
}
