package info.bitrich.xchangestream.deribit.dto;

import java.util.List;
import lombok.*;



@Data
@AllArgsConstructor
public class DeribitSubscribeMessage {
  private final String method;
  private final SubscriptionTopic params;

  @Data
  @AllArgsConstructor
  public static class SubscriptionTopic {
    private final List<String> channels;
  }
}
