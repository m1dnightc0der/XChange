package org.knowm.xchange.deribit.v2.dto.marketdata;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.math.BigDecimal;
import org.junit.Test;

public class DeribitTickerTest {

  @Test
  public void deserializeDeliveredTickerWithExpiredEstimatedDeliveryPrice() throws Exception {
    String json = "{\"timestamp\":1779868812392,\"state\":\"delivered\"," 
        + "\"interest_rate\":0,\"index_price\":75864.73,\"instrument_name\":\"BTC-27MAY26-66000-C\","
        + "\"last_price\":null,\"min_price\":0.1145,\"max_price\":0.144,\"open_interest\":0.0,"
        + "\"mark_price\":0.1294,\"best_ask_price\":0.0,\"best_bid_price\":0.0,\"mark_iv\":72.05,"
        + "\"bid_iv\":0.0,\"ask_iv\":0.0,\"underlying_price\":75807.47,\"underlying_index\":\"SYN.EXPIRY\","
        + "\"estimated_delivery_price\":\"expired\",\"delivery_price\":75807.47,\"best_ask_amount\":0.0,\"best_bid_amount\":0.0}";

    ObjectMapper mapper = new ObjectMapper();
    DeribitTicker ticker = mapper.readValue(json, DeribitTicker.class);

    assertThat(ticker.getState()).isEqualTo("delivered");
    assertThat(ticker.getInstrumentName()).isEqualTo("BTC-27MAY26-66000-C");
    assertThat(ticker.getDeliveryPrice()).isEqualTo(new BigDecimal("75807.47"));
    assertThat(ticker.getEstimatedDeliveryPrice()).isNull();
  }

  @Test
  public void deserializeTradeTest() throws Exception {

    // given
    InputStream is =
        DeribitTrade.class.getResourceAsStream(
            "/org/knowm/xchange/deribit/v2/dto/marketdata/example-ticker.json");

    // when
    ObjectMapper mapper = new ObjectMapper();
    DeribitTicker ticker = mapper.readValue(is, DeribitTicker.class);

    // then
    assertThat(ticker).isNotNull();

    assertThat(ticker.getUnderlyingPrice()).isEqualTo(new BigDecimal("5433.06"));
    assertThat(ticker.getUnderlyingIndex()).isEqualTo("SYN.BTC-3MAY19");
    assertThat(ticker.getTimestamp().getTime()).isEqualTo(1556125162701L);
    assertThat(ticker.getStats()).isNotNull();
    assertThat(ticker.getState()).isEqualTo("open");
    assertThat(ticker.getSettlementPrice()).isEqualTo(new BigDecimal("0.01"));
    assertThat(ticker.getOpenInterest()).isEqualTo(new BigDecimal("0.5"));
    assertThat(ticker.getMinPrice()).isEqualTo(new BigDecimal("0.0005"));
    assertThat(ticker.getMaxPrice()).isEqualTo(new BigDecimal("0.0625"));
    assertThat(ticker.getMarkPrice()).isEqualTo(new BigDecimal("0.01236567"));
    assertThat(ticker.getMarkIv()).isEqualTo(new BigDecimal("68.48"));
    assertThat(ticker.getLastPrice()).isEqualTo(new BigDecimal("0.0075"));
    assertThat(ticker.getInterestRate()).isEqualTo(new BigDecimal("0"));
    assertThat(ticker.getInstrumentName()).isEqualTo("BTC-3MAY19-5000-P");
    assertThat(ticker.getIndexPrice()).isEqualTo(new BigDecimal("5438.82"));
    assertThat(ticker.getGreeks()).isNotNull();
    assertThat(ticker.getBidIv()).isEqualTo(new BigDecimal("62.83"));
    assertThat(ticker.getBestBidPrice()).isEqualTo(new BigDecimal("0.01"));
    assertThat(ticker.getBestBidAmount()).isEqualTo(new BigDecimal("5"));
    assertThat(ticker.getBestAskPrice()).isEqualTo(new BigDecimal("0.0135"));
    assertThat(ticker.getBestAskAmount()).isEqualTo(new BigDecimal("5"));
    assertThat(ticker.getAskIv()).isEqualTo(new BigDecimal("71.09"));
  }
}
