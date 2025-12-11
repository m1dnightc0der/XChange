package org.knowm.xchange.hyperliquid.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import si.mazi.rescu.ExceptionalReturnContentException;

/** V represents result class of the queried endpoint */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class HyperliquidResponse<V> {

  @JsonProperty("status")
  private String status;


  @JsonProperty("response")
  private V result;


  @JsonProperty("order")
  private V order;

  public HyperliquidResponse() {}

  public HyperliquidResponse(
      V result,
V order,
      String status) {




    this.result = result;
    this.status = status;
    this.order=order;

  }
}
