package info.bitrich.xchangestream.okex.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.knowm.xchange.okex.dto.trade.OkexOrderRequest;

import java.math.BigDecimal;
import java.util.LinkedList;
import java.util.List;

@Data
public class OkexOrderMessage {
  private String op ;
  private Long id;
  List<OrderArg> args;

  @Data
  @AllArgsConstructor
  public static class OrderArg {
    private final String instIdCode;
    private final String tdMode;
    private final String ccy;
    private final String clOrdId;
    private final String tag;
    private final String side;
    private final String posSide;
    private final String ordType;
    private final String sz;
    private final String px;
    private final String pxUsd;
    private final String pxVol;
    private final Boolean reduceOnly;
    private final String tgtCcy;
    private final Boolean banAmend;
    private final String quickMgnType;
    private final String stpId;
    private final String stpMode;


  }


}
