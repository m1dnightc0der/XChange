package org.knowm.xchange.bybit.dto.trade;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;


@Builder
public class BybitCancelOrderRequest {
  @JsonProperty("category")
  private String category;

  @JsonProperty("symbol")
  private String symbol;

  @JsonProperty("orderId")
  private String orderId;

  @JsonProperty("orderLinkId")
  private String orderLinkId;
}
