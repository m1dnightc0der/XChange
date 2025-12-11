package org.knowm.xchange.hyperliquid.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * DTO for Hyperliquid API error responses
 */
public class HyperliquidException extends RuntimeException {

    @JsonProperty("error")
    private String error;

    @JsonProperty("code")
    private Integer code;

    public HyperliquidException() {}

    public HyperliquidException(String error) {
        super(error);
        this.error = error;
    }

    public HyperliquidException(String error, Integer code) {
        super(error);
        this.error = error;
        this.code = code;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    @Override
    public String toString() {
        return "HyperliquidException{" +
                "error='" + error + '\'' +
                ", code=" + code +
                '}';
    }
}