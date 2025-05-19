package org.knowm.xchange.bybit.dto.marketdata.trades;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigDecimal;
import java.util.Date;

public class BybitTrade {

  private final String execId;
  private final String symbol;
  private final BigDecimal price;
  private final String side;
  private final BigDecimal size;
  private final Date time;
  private final Boolean blockTrade;


  public BybitTrade(
      @JsonProperty("execId") String execId,
      @JsonProperty("symbol") String symbol,
      @JsonProperty("price") BigDecimal price,
      @JsonProperty("size") BigDecimal size,
      @JsonProperty("side") String side,
      @JsonProperty("time") Date time,
      @JsonProperty("isBlockTrade") Boolean blockTrade
      ) {
    this.execId = execId;
    this.symbol = symbol;
    this.price = price;
    this.size = size;
    this.side = side;
    this.time = time;
    this.blockTrade = blockTrade;
  }

  public String getExecId() {
    return execId;
  }

  public String getSymbol() {
    return symbol;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public BigDecimal getSize() {
    return size;
  }

  public String getSide() {
    return side;
  }

  public Date getTime() {
    return time;
  }
  public Boolean getBlockTrade() {
    return blockTrade;
  }
  @Override
  public String toString() {
    return "BybitTrade{"
        + "execId='"
        + execId
        + '\''
        + ", symbol="
        + symbol
        + ", price="
        + price
        + ", size="
        + size
        + ", side="
        + side
        + ", time="
        + time
        + ", blockTrade="
        + blockTrade
        + '}';
  }
}
