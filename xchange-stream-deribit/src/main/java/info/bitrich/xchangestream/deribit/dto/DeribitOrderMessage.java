package info.bitrich.xchangestream.deribit.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class DeribitOrderMessage {
  private final String method;

  private final Long id;
  private final OrderArg params;
  @Data
  @AllArgsConstructor
  public static class OrderArg {
    private final String instrument_name;
    private final BigDecimal amount;
    private final BigDecimal price;
    private final Integer contracts;
    private final String type;
    private final String time_in_force;
    private final BigDecimal max_show;
    private final boolean post_only;
    private final boolean reject_post_only;
    private final boolean reduce_only;
    private final BigDecimal trigger_offset;
    private final String trigger;
    private final String advanced;
    private final Integer valid_until;
    private final String linked_order_type;
    private final String trigger_fill_condition;

    private final boolean mmp;
    private final String label;

  }
}
