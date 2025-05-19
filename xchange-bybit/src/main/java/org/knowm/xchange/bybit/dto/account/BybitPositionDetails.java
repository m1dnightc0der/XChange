package org.knowm.xchange.bybit.dto.account;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Data;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import org.knowm.xchange.bybit.dto.BybitCategorizedPayload;
import org.knowm.xchange.bybit.dto.trade.details.BybitOrderDetail;
import org.knowm.xchange.bybit.dto.trade.details.BybitOrderDetails.BybitLinearOrderDetails;
import org.knowm.xchange.bybit.dto.trade.details.BybitOrderDetails.BybitSpotOrderDetails;
import org.knowm.xchange.bybit.dto.trade.details.linear.BybitLinearOrderDetail;
import org.knowm.xchange.bybit.dto.trade.details.spot.BybitSpotOrderDetail;

@Data
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "category", visible = true)
@JsonSubTypes({
  @Type(value = BybitPositionDetails.class, name = "linear"),
  @Type(value = BybitPositionDetails.class, name = "inverse"),
  @Type(value = BybitPositionDetails.class, name = "option"),
})
public class BybitPositionDetails<T extends BybitPosition> extends BybitCategorizedPayload<T> {

  @JsonProperty("nextPageCursor")
  String nextPageCursor;

}
