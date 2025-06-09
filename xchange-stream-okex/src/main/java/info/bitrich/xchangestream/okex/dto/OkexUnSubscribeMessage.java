package info.bitrich.xchangestream.okex.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.knowm.xchange.okex.dto.OkexInstType;

import java.util.List;

@Data
@AllArgsConstructor
public class OkexUnSubscribeMessage {
  private final String op;
  private final List<OkexSubscribeMessage.SubscriptionTopic> args;
  //private final String channel;
  //private final String instId;

}
