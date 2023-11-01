package info.bitrich.xchangestream.binance.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.knowm.xchange.binance.BinanceAdapters;
import org.knowm.xchange.instrument.Instrument;

public class ProductBinanceWebSocketTransaction extends BaseBinanceWebSocketTransaction {

  protected final String symbol;

  public ProductBinanceWebSocketTransaction(
      @JsonProperty("e") String eventType,
      @JsonProperty("E") String eventTime,
      @JsonProperty("s") String symbol) {
    super(eventType, eventTime);
    this.symbol = symbol;
  }

  public String getSymbol() {
    return symbol;
  }
}
