package org.knowm.xchange.okex.dto.trade;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;

/** Author: Max Gao (gaamox@tutanota.com) Created: 10-06-2021 */
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OkexAmendOrderRequest {
  @JsonProperty("instId")
  private String instrumentId;

  @JsonProperty("cxlOnFail")
  private Boolean cancelOnFail;

  @JsonProperty("ordId")
  private String orderId;

  @JsonProperty("clOrdId")
  private String clientOrderId;

  @JsonProperty("reqId")
  private String requestId;

  @JsonProperty("newSz")
  private String amendedAmount;

  @JsonProperty("newPx")
  private String amendedPrice;

  @JsonProperty("pxAmendType")
  private String priceAmendType;
}
