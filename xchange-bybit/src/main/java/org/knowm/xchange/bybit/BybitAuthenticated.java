package org.knowm.xchange.bybit;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.knowm.xchange.bybit.dto.BybitResult;
import org.knowm.xchange.bybit.dto.account.BybitPosition;
import org.knowm.xchange.bybit.dto.account.BybitPositionDetails;
import org.knowm.xchange.bybit.dto.account.allcoins.BybitAllCoinsBalance;
import org.knowm.xchange.bybit.dto.account.feerates.BybitFeeRates;
import org.knowm.xchange.bybit.dto.account.walletbalance.BybitWalletBalance;
import org.knowm.xchange.bybit.dto.trade.BybitAmendOrderPayload;
import org.knowm.xchange.bybit.dto.trade.BybitCancelOrderRequest;
import org.knowm.xchange.bybit.dto.trade.BybitOrderResponse;
import org.knowm.xchange.bybit.dto.trade.BybitPlaceOrderPayload;
import org.knowm.xchange.bybit.dto.trade.details.BybitOrderDetail;
import org.knowm.xchange.bybit.dto.trade.details.BybitOrderDetails;
import org.knowm.xchange.bybit.service.BybitException;
import si.mazi.rescu.ParamsDigest;
import si.mazi.rescu.SynchronizedValueFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.knowm.xchange.bybit.service.BybitDigest.*;

@Path("/v5") @Produces(MediaType.APPLICATION_JSON) public interface BybitAuthenticated {
  String positionsPath = "/position/list";
  String cancelOrderPath = "/order/cancel";
  String orderStatusPath = "/order/realtime";

  Map<String, List<Integer>> privatePathRateLimits = new HashMap<String, List<Integer>>() {
    {
      put(positionsPath, Arrays.asList(5, 1));
    }
  };

  /**
   * @apiSpec <a href="https://bybit-exchange.github.io/docs/v5/account/wallet-balance">API</a>
   */
  @GET @Path("/account/wallet-balance") BybitResult<BybitWalletBalance> getWalletBalance(@HeaderParam(X_BAPI_API_KEY) String apiKey,
      @HeaderParam(X_BAPI_SIGN) ParamsDigest signature, @HeaderParam(X_BAPI_TIMESTAMP) SynchronizedValueFactory<Long> timestamp,
      @QueryParam("accountType") String accountType) throws IOException, BybitException;

  /**
   * @apiSpec <a href="https://bybit-exchange.github.io/docs/v5/asset/all-balance">API</a>
   */
  @GET @Path("/asset/transfer/query-account-coins-balance") BybitResult<BybitAllCoinsBalance> getAllCoinsBalance(
      @HeaderParam(X_BAPI_API_KEY) String apiKey, @HeaderParam(X_BAPI_SIGN) ParamsDigest signature,
      @HeaderParam(X_BAPI_TIMESTAMP) SynchronizedValueFactory<Long> timestamp, @QueryParam("accountType") String accountType)
      throws IOException, BybitException;

  /**
   * @apiSpec <a href="https://bybit-exchange.github.io/docs/v5/account/fee-rate">API</a>
   */
  @GET @Path("/account/fee-rate") BybitResult<BybitFeeRates> getFeeRates(@HeaderParam(X_BAPI_API_KEY) String apiKey,
      @HeaderParam(X_BAPI_SIGN) ParamsDigest signature, @HeaderParam(X_BAPI_TIMESTAMP) SynchronizedValueFactory<Long> timestamp,
      @QueryParam("category") String category, @QueryParam("symbol") String symbol) throws IOException, BybitException;

  /**
   * @apiSpec <a href="https://bybit-exchange.github.io/docs/v5/order/open-order">API</a>
   */
  @GET @Path("/order/realtime") BybitResult<BybitOrderDetails<BybitOrderDetail>> getOpenOrders(@HeaderParam(X_BAPI_API_KEY) String apiKey,
      @HeaderParam(X_BAPI_SIGN) ParamsDigest signature, @HeaderParam(X_BAPI_TIMESTAMP) SynchronizedValueFactory<Long> timestamp,
      @QueryParam("category") String category, @QueryParam("orderId") String orderId) throws IOException, BybitException;

  /**
   * @apiSpec <a href="https://bybit-exchange.github.io/docs/v5/order/create-order">API</a>
   */
  @POST @Path("/order/create") @Consumes(MediaType.APPLICATION_JSON) BybitResult<BybitOrderResponse> placeMarketOrder(
      @HeaderParam(X_BAPI_API_KEY) String apiKey, @HeaderParam(X_BAPI_SIGN) ParamsDigest signature,
      @HeaderParam(X_BAPI_TIMESTAMP) SynchronizedValueFactory<Long> timestamp, BybitPlaceOrderPayload payload) throws IOException, BybitException;

  /**
   * @apiSpec <a href="https://bybit-exchange.github.io/docs/v5/order/create-order">API</a>
   */
  @POST @Path("/order/create") @Consumes(MediaType.APPLICATION_JSON) BybitResult<BybitOrderResponse> placeLimitOrder(
      @HeaderParam(X_BAPI_API_KEY) String apiKey, @HeaderParam(X_BAPI_SIGN) ParamsDigest signature,
      @HeaderParam(X_BAPI_TIMESTAMP) SynchronizedValueFactory<Long> timestamp, BybitPlaceOrderPayload payload) throws IOException, BybitException;

  @POST @Path(cancelOrderPath) @Consumes(MediaType.APPLICATION_JSON) BybitResult<BybitOrderResponse> cancelOrder(
      @HeaderParam(X_BAPI_API_KEY) String apiKey, @HeaderParam(X_BAPI_SIGN) ParamsDigest signature,
      @HeaderParam(X_BAPI_TIMESTAMP) SynchronizedValueFactory<Long> timestamp, BybitCancelOrderRequest requestPayload)
      throws IOException, BybitException;

  @GET @Path(positionsPath) @Consumes(MediaType.APPLICATION_JSON) BybitResult<BybitPositionDetails<BybitPosition>> getPositions(
      @HeaderParam(X_BAPI_API_KEY) String apiKey, @HeaderParam(X_BAPI_SIGN) ParamsDigest signature,
      @HeaderParam(X_BAPI_TIMESTAMP) SynchronizedValueFactory<Long> timestamp, @QueryParam("category") String category,
      @QueryParam("settleCoin") String settleCoin, @QueryParam("symbol") String symbol, @QueryParam("cursor") String cursor,@QueryParam("limit") Integer limit)

      throws IOException, BybitException;

  /**
   * @apiSpec <https://bybit-exchange.github.io/docs/v5/order/open-order>API</a>
   */
  @GET @Path(orderStatusPath) @Consumes(MediaType.APPLICATION_JSON)  BybitResult<BybitOrderDetails<BybitOrderDetail>> getPendingOrders(
      @HeaderParam(X_BAPI_API_KEY) String apiKey, @HeaderParam(X_BAPI_SIGN) ParamsDigest signature,
      @HeaderParam(X_BAPI_TIMESTAMP) SynchronizedValueFactory<Long> timestamp, @QueryParam("category") String category,
      @QueryParam("settleCoin") String settleCoin, @QueryParam("symbol") String symbol, @QueryParam("cursor") String cursor,
      @QueryParam("orderId") String orderId) throws IOException, BybitException;

  /**
   * @apiSpec <https://bybit-exchange.github.io/docs/v5/order/amend-order">API</a>
   */
  @POST @Path("/order/amend") @Consumes(MediaType.APPLICATION_JSON) BybitResult<BybitOrderResponse> amendOrder(
      @HeaderParam(X_BAPI_API_KEY) String apiKey, @HeaderParam(X_BAPI_SIGN) ParamsDigest signature,
      @HeaderParam(X_BAPI_TIMESTAMP) SynchronizedValueFactory<Long> timestamp, BybitAmendOrderPayload payload) throws IOException, BybitException;
}
