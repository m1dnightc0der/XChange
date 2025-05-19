package info.bitrich.xchangestream.deribit.dto;

import java.util.LinkedList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
public class DeribitLoginMessage {
  private String method = "public/auth";

  LoginArg params;
  @Data
  @AllArgsConstructor
  public static class LoginArg {
    private String client_id;
    private String client_secret;

    private String grant_type;
    // Unix Epoch time, the unit is seconds
    private String timestamp;
    // https://www.okx.com/docs-v5/en/#websocket-api-login
    private String sign;
  }
}
