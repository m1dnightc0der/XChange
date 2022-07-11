package org.knowm.xchange.ftx.dto.trade;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.math.BigDecimal;

public class FtxModifyOrderRequestPayload {
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final BigDecimal price;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private final BigDecimal size;

  private final String clientId;

  public FtxModifyOrderRequestPayload(
      @JsonProperty("price") BigDecimal price,
      @JsonProperty("size") BigDecimal size,
      @JsonProperty("clientId") String clientId) {
    this.price = price;
    this.size = size;
    this.clientId = clientId;
  }

  public BigDecimal getPrice() {
    return price;
  }

  public BigDecimal getSize() {
    return size;
  }

  public String getClientId() {
    return clientId;
  }

  @Override
  public String toString() {
    return "FtxModifyOrderRequestPayload{"
        + "price="
        + price
        + ", size="
        + size
        + ", clientId='"
        + clientId
        + '\''
        + '}';
  }
}
