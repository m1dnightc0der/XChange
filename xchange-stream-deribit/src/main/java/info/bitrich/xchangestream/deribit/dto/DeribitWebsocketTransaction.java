package info.bitrich.xchangestream.deribit.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DeribitWebsocketTransaction<T> {
  private final String method;
  private final T params;

  public DeribitWebsocketTransaction(
      @JsonProperty("method") String method, @JsonProperty("params") T params) {
    this.method = method;
    this.params = params;
  }

  public String getMethod() {
    return method;
  }

  public T getParams() {
    return params;
  }
}
