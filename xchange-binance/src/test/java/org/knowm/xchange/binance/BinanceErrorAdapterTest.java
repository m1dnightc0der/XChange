package org.knowm.xchange.binance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.knowm.xchange.binance.dto.BinanceException;
import org.knowm.xchange.exceptions.ExchangeException;
import org.knowm.xchange.exceptions.ExchangeSecurityException;
import org.knowm.xchange.exceptions.FundsExceededException;
import org.knowm.xchange.exceptions.OperationTimeoutException;

public class BinanceErrorAdapterTest {

  private static final String RESOURCE =
      "/org/knowm/xchange/binance/usdm-futures-error-mapping.csv";

  @Test
  public void documentedUsdMFuturesErrorsHaveUniqueCompleteMappings() throws Exception {
    Set<String> keys = new HashSet<>();
    for (String[] row : mappingRows()) {
      assertThat(row).hasSize(7);
      assertThat(row[3]).isNotBlank();
      assertThat(row[0]).doesNotContain("..");
      assertThat(keys.add(row[0] + ":" + row[2])).as("unique code/message key").isTrue();
      assertThatCode(() -> Class.forName("org.knowm.xchange.exceptions." + row[3]))
          .doesNotThrowAnyException();
    }
  }

  @Test
  public void everyDocumentedUsdMFuturesErrorMapsToManifestClassWithCanonicalEvidence()
      throws Exception {
    for (String[] row : mappingRows()) {
      int code = Integer.parseInt(row[0]);
      String rawMessage = rawMessage(row[1], row[2]);
      BinanceException source = new BinanceException(code, rawMessage);

      ExchangeException adapted = BinanceErrorAdapter.adapt(source);

      Class<?> expected = Class.forName("org.knowm.xchange.exceptions." + row[3]);
      assertThat(adapted.getClass())
          .as("%s %s (%s)", row[0], row[1], row[2])
          .isEqualTo(expected);
      assertThat(adapted).hasMessage("Binance error " + code + ": " + rawMessage);
      assertThat(adapted).hasCause(source);
    }
  }

  @Test
  public void nullMessageIsSafeAndPreservesCodeAndCause() {
    BinanceException source = new BinanceException(-999999, null);

    ExchangeException adapted = BinanceErrorAdapter.adapt(source);

    assertThat(adapted.getClass()).isEqualTo(ExchangeException.class);
    assertThat(adapted).hasMessage("Binance error -999999: No error details provided");
    assertThat(adapted).hasCause(source);
  }

  @Test
  public void legacyExactMappingsRemainUnchanged() {
    assertThat(BinanceErrorAdapter.adapt(new BinanceException(-1002, "UNAUTHORIZED")))
        .isExactlyInstanceOf(ExchangeSecurityException.class);
    assertThat(BinanceErrorAdapter.adapt(new BinanceException(-1021, "INVALID_TIMESTAMP")))
        .isExactlyInstanceOf(OperationTimeoutException.class);
    assertThat(BinanceErrorAdapter.adapt(new BinanceException(-1122, "INVALID_SYMBOL_STATUS")))
        .isExactlyInstanceOf(ExchangeSecurityException.class);
    for (int code : new int[] {-1010, -2010, -2011}) {
      assertThat(BinanceErrorAdapter.adapt(new BinanceException(code, "insufficient balance")))
          .as("legacy insufficient-balance mapping for %s", code)
          .isExactlyInstanceOf(FundsExceededException.class);
    }
    assertThat(BinanceErrorAdapter.adapt(new BinanceException(-2010, "NEW_ORDER_REJECTED")))
        .isExactlyInstanceOf(ExchangeException.class);
  }

  @Test
  public void undocumentedBoundaryCodesRemainExactlyGeneric() {
    for (int code : new int[] {-1009, -2029, -4090, -5042, 7}) {
      BinanceException source = new BinanceException(code, "future code");
      ExchangeException adapted = BinanceErrorAdapter.adapt(source);
      assertThat(adapted.getClass()).as("code %s", code).isEqualTo(ExchangeException.class);
      assertThat(adapted).hasCause(source);
    }
  }

  private static String rawMessage(String symbol, String messageKey) {
    switch (messageKey) {
      case "internal_error":
        return "Internal error; unable to process your request. Please try again.";
      case "not_authorized":
        return "You are not authorized to execute this request.";
      case "min_notional":
        return "Filter failure: MIN_NOTIONAL";
      case "unknown_order_sent":
        return "Unknown order sent.";
      default:
        return symbol;
    }
  }

  private static List<String[]> mappingRows() throws Exception {
    InputStream stream = BinanceErrorAdapterTest.class.getResourceAsStream(RESOURCE);
    assertThat(stream).as(RESOURCE).isNotNull();
    List<String[]> rows = new ArrayList<>();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))) {
      String line;
      boolean header = true;
      while ((line = reader.readLine()) != null) {
        if (header) {
          header = false;
          continue;
        }
        if (!line.trim().isEmpty()) {
          rows.add(line.split(",", -1));
        }
      }
    }
    return rows;
  }
}
