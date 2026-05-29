package org.knowm.xchange.binance;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.knowm.xchange.binance.dto.BinanceException;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.FundsExceededException;
import org.knowm.xchange.exceptions.OrderAmountUnderMinimumException;
import org.knowm.xchange.exceptions.OrderNotValidException;

public class BinanceErrorAdapterTest {

  @Test
  public void testBinanceExceptionWithCode2019MarginInsufficient() {
    final ExchangeException adaptedException =
        BinanceErrorAdapter.adapt(new BinanceException(-2019, "Margin is insufficient."));

    assertThat(adaptedException).isInstanceOf(FundsExceededException.class);
    assertThat(adaptedException).hasMessageContaining("Margin is insufficient.");
  }

  @Test
  public void testBinanceExceptionWithCode1013MsgLotSize() {
    final ExchangeException adaptedException =
        BinanceErrorAdapter.adapt(new BinanceException(-1013, "LOT_SIZE"));

    assertThat(adaptedException).isInstanceOf(OrderNotValidException.class);
  }

  @Test
  public void testBinanceExceptionWithCode1013MsgMinNotional() {
    final ExchangeException adaptedException =
        BinanceErrorAdapter.adapt(new BinanceException(-1013, "MIN_NOTIONAL"));

    assertThat(adaptedException).isInstanceOf(OrderAmountUnderMinimumException.class);
  }
}
