package org.knowm.xchange.hyperliquid.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.knowm.xchange.hyperliquid.dto.trade.OrderType;
import org.knowm.xchange.hyperliquid.dto.trade.Side;
import org.knowm.xchange.hyperliquid.dto.trade.TimeInForce;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class Response<V> {

    @JsonProperty("type")
  private String type;

  /** true for reduce-only orders only */
  @JsonProperty("data")
  private V data;

    public Response() {}

    public Response(
            V data,

            String type) {




        this.type = type;
        this.data = data;

    }
}
