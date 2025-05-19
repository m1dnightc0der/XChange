package org.knowm.xchange.bybit.dto.marketdata.trades;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.knowm.xchange.bybit.dto.marketdata.orderbooks.BybitPublicOrder;

import java.util.List;

@Builder
@Jacksonized
@Value
public class BybitTradeResponse {

  private final List<BybitTrade> list;
  private final String category;


  @JsonCreator
  public BybitTradeResponse(
      @JsonProperty("list") List<BybitTrade> list,

      @JsonProperty("category") String category)
  {


    this.list = list;
    this.category = category;

  }

  @Override
  public String toString() {
    return "BybitTradeResponse{category=" + category+ ", list=" + list +'}';
  }


}
