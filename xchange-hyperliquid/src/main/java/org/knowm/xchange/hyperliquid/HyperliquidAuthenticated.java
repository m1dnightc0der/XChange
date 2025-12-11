package org.knowm.xchange.hyperliquid;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
// TODO: Create these DTO classes
// import org.knowm.xchange.hyperliquid.dto.account.HyperliquidUserState;
// import org.knowm.xchange.hyperliquid.dto.trade.HyperliquidOpenOrders;
// import org.knowm.xchange.hyperliquid.dto.trade.HyperliquidOrderResponse;
// import org.knowm.xchange.hyperliquid.dto.trade.HyperliquidCancelResponse;
// import org.knowm.xchange.hyperliquid.dto.trade.HyperliquidWithdrawResponse;
// import org.knowm.xchange.hyperliquid.dto.trade.HyperliquidTransferResponse;
import org.knowm.xchange.hyperliquid.dto.HyperliquidResponse;
import org.knowm.xchange.hyperliquid.dto.trade.Order;
import si.mazi.rescu.ParamsDigest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * REST interface for Hyperliquid authenticated (private) API endpoints
 * Handles trading operations, account management, and user-specific data
 */
@Path("/exchange")
@Produces(MediaType.APPLICATION_JSON)
public interface HyperliquidAuthenticated {

    /**
     * Get user account state including positions, balances, and margin info
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing type and user address
     * @return User state data
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object getUserState(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Get open orders for the authenticated user
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing type and user address
     * @return Open orders data
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    List<Order> getOpenOrders(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Place a new order (limit or market)
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing order details
     * @return Order placement response
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object placeOrder(
        @HeaderParam("Content-Type") String contentType,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Cancel a specific order by order ID
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing order cancellation details
     * @return Cancellation response
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    HyperliquidResponse cancelOrder(
        @HeaderParam("Content-Type") String contentType,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Cancel multiple orders by coin or cancel all orders
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing cancellation criteria
     * @return Cancellation response
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object cancelOrders(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Modify an existing order (price, size, etc.)
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing order modification details
     * @return Modification response
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object modifyOrder(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Withdraw funds from the exchange
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing withdrawal details
     * @return Withdrawal response
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object withdraw(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Transfer funds between spot and perpetual accounts
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing transfer details
     * @return Transfer response
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object transfer(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Update leverage for a specific asset
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing leverage update details
     * @return Order response (leverage changes are treated as orders)
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object updateLeverage(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Update isolated margin for a position
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing margin update details
     * @return Order response (margin changes are treated as orders)
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object updateIsolatedMargin(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Schedule a cancel for an order (cancel after a specific time)
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing schedule cancel details
     * @return Order response
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object scheduleCancel(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Vault transfer operation (for vault-related functionality)
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing vault transfer details
     * @return Transfer response
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object vaultTransfer(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Set referral code for the user
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing referral code details
     * @return Order response
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object setReferrer(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;
}