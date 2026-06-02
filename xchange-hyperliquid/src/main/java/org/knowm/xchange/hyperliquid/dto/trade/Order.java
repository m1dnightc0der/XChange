package org.knowm.xchange.hyperliquid.dto.trade;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.math.BigDecimal;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Order {
  @JsonProperty("tif")
  private String tif;

  public TimeInForce getTimeInForceEnum() {
    return tif == null ? null : TimeInForce.parseTimeInForce(tif);
  }

  @JsonIgnore
  public TimeInForce getTimeInForce() {
    return getTimeInForceEnum();
  }

  /** true for reduce-only orders only */
  @JsonProperty("reduceOnly")
  private boolean reduceOnly;


  /** Price in base currency */
  @JsonProperty("limitPx")
  private String price;

  public BigDecimal getPrice() {
    try {
      return new BigDecimal(price);
    } catch (NumberFormatException e) {
      return null;
    }
  }


  /** order type, "limit", "market", "stop_limit", "stop_market" */
  @JsonProperty("orderType")
  private OrderType orderType;


  /** Unique order identifier */
  @JsonProperty("oid")
  private String oid;

  /** Unique order identifier */
  @JsonProperty("cloid")
  private String cloid;

  /** The timestamp (seconds since the Unix epoch, with millisecond precision) */
  @JsonProperty("timestamp")
  private long timestamp; //      <- millis

  /** Unique instrument identifier */
  @JsonProperty("coin")
  private String coin;

  /** direction, buy or sell */
  @JsonProperty("side")
  private Side side;

  /** The timestamp (seconds since the Unix epoch, with millisecond precision) */
  @JsonProperty("creation_timestamp")
  private long creationTimestamp; //      <- millis

  /** Size of the order */
  @JsonProperty("sz")
  private BigDecimal sz;


  /** Orignal size of the order */
  @JsonProperty("origSz")
  private BigDecimal origSz;


  /**
   * It represents the requested order size. For perpetual and futures the amount is in USD units,
   * for options it is amount of corresponding cryptocurrency contracts, e.g., BTC or ETH.
   */

  /** stop price (Only for future stop orders) */
  @JsonProperty("triggerPx")
  private BigDecimal triggerPx;

  /** Whether the stop order has been triggered (Only for stop orders) */
  @JsonProperty("isTrigger")
  private Boolean isTrigger;

  /** Trigger condition for conditional orders (e.g., "N/A" for non-triggered orders) */
  @JsonProperty("triggerCondition")
  private String triggerCondition;

  /** Child orders associated with this order */
  @JsonProperty("children")
  private List<Object> children;

  /** Whether this is a position take-profit/stop-loss order */
  @JsonProperty("isPositionTpsl")
  private Boolean isPositionTpsl;
}
