package org.knowm.xchange.okex.dto.trade;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

/* Author: Max Gao (gaamox@tutanota.com) Created: 09-06-2021 */
/** <a href="https://www.okex.com/docs-v5/en/#rest-api-trade-place-order">...</a> * */
@Builder
public class OkexOrderRequest {
  @JsonProperty("instId")
  public String instrumentId;

  @JsonProperty("tdMode")
  public String tradeMode;

  @JsonProperty("ccy")
  public String marginCurrency;

  @JsonProperty("clOrdId")
  public String clientOrderId;

  @JsonProperty("tag")
  public String tag;

  @JsonProperty("side")
  public String side;

  @JsonProperty("posSide")
  public String posSide;

  @JsonProperty("ordType")
  public String orderType;

  @JsonProperty("sz")
  public String amount;

  @JsonProperty("px")
  public String price;

  @JsonProperty("reduceOnly")
  public boolean reducePosition;

  @JsonProperty("slTriggerPx")
  public String stopLossTriggerPrice;

  @JsonProperty("slOrdPx")
  public String stopLossLimitPrice;

  @JsonProperty("tpTriggerPx")
  public String takeProfitTriggerPrice;

  @JsonProperty("tpOrdPx")
  public String takeProfitLimitPrice;

}
