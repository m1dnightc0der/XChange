package org.knowm.xchange.binance.dto.trade;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;
import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class BinanceOrder {

  public final String symbol;
  public final long orderId;
  public final String clientOrderId;
  public final BigDecimal price;
  public final BigDecimal origQty;
  public final BigDecimal executedQty;
  public final BigDecimal cummulativeQuoteQty;
  public final OrderStatus status;
  public final String timeInForce;
  public final OrderType type;
  public final OrderSide side;
  public final BigDecimal stopPrice;
  public final BigDecimal icebergQty;
  public final BigDecimal avgPrice;
  public final long time;
  public final long updateTime;

  public BinanceOrder(
      @JsonProperty("symbol") String symbol,
      @JsonProperty("orderId") long orderId,
      @JsonProperty("clientOrderId") String clientOrderId,
      @JsonProperty("price") BigDecimal price,
      @JsonProperty("origQty") BigDecimal origQty,
      @JsonProperty("executedQty")
      @JsonAlias("cumQty")
      BigDecimal executedQty,
      @JsonProperty("cummulativeQuoteQty")
      @JsonAlias({"cumQuote", "cumBase"})
      BigDecimal cummulativeQuoteQty,
      @JsonProperty("status") OrderStatus status,
      @JsonProperty("timeInForce") String timeInForce,
      @JsonProperty("type") OrderType type,
      @JsonProperty("side") OrderSide side,
      @JsonProperty("stopPrice") BigDecimal stopPrice,
      @JsonProperty("icebergQty") BigDecimal icebergQty,
      @JsonProperty("time") Long time,
      @JsonProperty("updateTime") Long updateTime,
      @JsonProperty(value = "avgPrice", required = false) BigDecimal avgPrice) {
    this.symbol = symbol;
    this.orderId = orderId;
    this.clientOrderId = clientOrderId;
    this.price = price;
    this.origQty = origQty;
    this.executedQty = executedQty;
    this.cummulativeQuoteQty = cummulativeQuoteQty;
    this.status = status;
    this.timeInForce = timeInForce;
    this.type = type;
    this.side = side;
    this.stopPrice = stopPrice;
    this.icebergQty = icebergQty;
    this.time = time == null ? 0L : time;
    this.updateTime = updateTime == null ? this.time : updateTime;
    this.avgPrice = avgPrice;
  }

  public TimeInForce getTimeInForceEnum() {
    return timeInForce == null ? null : TimeInForce.getTimeInForce(timeInForce);
  }

  @JsonIgnore
  public TimeInForce getTimeInForce() {
    return getTimeInForceEnum();
  }

  public Date getTime() {
    long timestamp = time == 0L ? updateTime : time;
    return new Date(timestamp);
  }
}
