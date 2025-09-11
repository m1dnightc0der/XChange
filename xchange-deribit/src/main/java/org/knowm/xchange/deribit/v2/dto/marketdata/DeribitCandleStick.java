package org.knowm.xchange.deribit.v2.dto.marketdata;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class DeribitCandleStick {

  private final List<BigDecimal> volume = new ArrayList<>();
  private final List<Long> ticks = new ArrayList<>();
  private final List<BigDecimal> open = new ArrayList<>();
  private final List<BigDecimal> low = new ArrayList<>();
  private final List<BigDecimal> high = new ArrayList<>();
  private final List<BigDecimal> cost = new ArrayList<>();
  private final List<BigDecimal> close = new ArrayList<>();
  /** The state of the candle stick response. */
  @JsonProperty("status")
  private String status;

  @JsonProperty("volume")
  public void setVolume(List<BigDecimal> volume) {
   this.volume.addAll(volume);
  }

  @JsonProperty("ticks")
  public void setTicks(List<Long> ticks) {
    this.ticks.addAll(ticks);
  }

  @JsonProperty("open")
  public void setOpen(List<BigDecimal> open) {
    this.open.addAll(open);
  }

  @JsonProperty("low")
  public void setLow(List<BigDecimal> low) {
    this.low.addAll(low);
  }

  @JsonProperty("high")
  public void setHigh(List<BigDecimal> high) {
    this.high.addAll(high);
  }

  @JsonProperty("cost")
  public void setCost(List<BigDecimal> cost) {
    this.cost.addAll(cost);
  }

  @JsonProperty("close")
  public void setClose(List<BigDecimal> close) {
    this.close.addAll(close);
  }
}
