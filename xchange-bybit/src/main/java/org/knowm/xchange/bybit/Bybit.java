package org.knowm.xchange.bybit;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.knowm.xchange.bybit.dto.BybitResult;
import org.knowm.xchange.bybit.dto.marketdata.instruments.BybitInstrumentInfo;
import org.knowm.xchange.bybit.dto.marketdata.instruments.BybitInstrumentsInfo;
import org.knowm.xchange.bybit.dto.marketdata.orderbooks.BybitOrderbook;
import org.knowm.xchange.bybit.dto.marketdata.tickers.BybitTicker;
import org.knowm.xchange.bybit.dto.marketdata.tickers.BybitTickers;
import org.knowm.xchange.bybit.dto.marketdata.trades.BybitTrade;
import org.knowm.xchange.bybit.dto.marketdata.trades.BybitTradeResponse;
import org.knowm.xchange.bybit.service.BybitException;

@Path("/v5/market")
@Produces(MediaType.APPLICATION_JSON)
public interface Bybit {
  String instrumentsPath = "/instruments-info"; // Stated as 20 req/2 sec
  String tickersPath = "/tickers"; // Stated as 20 req/2 sec

  Map<String, List<Integer>> publicPathRateLimits =
      new HashMap<String, List<Integer>>() {
        {
          put(instrumentsPath, Arrays.asList(8, 1));
          put(tickersPath, Arrays.asList(8, 1));
        }
      };


  /**
   * @apiSpec <a href="https://bybit-exchange.github.io/docs/v5/market/tickers">API</a>
   */
  @GET
  @Path(tickersPath)
  BybitResult<BybitTickers<BybitTicker>> getTicker24h(
      @QueryParam("category") String category, @QueryParam("symbol") String symbol)
      throws IOException, BybitException;

  /**
   * @apiSpec <a href="https://bybit-exchange.github.io/docs/v5/market/instrument">API</a>
   */
  @GET
  @Path(instrumentsPath)
  BybitResult<BybitInstrumentsInfo<BybitInstrumentInfo>> getInstrumentsInfo(
      @QueryParam("category") String category,
      @QueryParam("limit") String limit) throws IOException, BybitException;

  /**
   * @apiSpec <a href="https://bybit-exchange.github.io/docs/v5/market/tickers">API</a>
   */
  @GET
  @Path("/orderbook")
  BybitResult<BybitOrderbook> getOrderbook(
      @QueryParam("category") String category, @QueryParam("symbol") String symbol, @QueryParam("limit") Integer limit)
      throws IOException, BybitException;

  @GET
  @Path("/recent-trade")
  BybitResult<BybitTradeResponse> getTrades(
      @QueryParam("category") String category, @QueryParam("symbol") String symbol)
      throws IOException, BybitException;

  @GET
  @Path(tickersPath)
  BybitResult<BybitTickers<BybitTicker>> getTickers(
      @QueryParam("category") String category)
      throws IOException, BybitException;

}
