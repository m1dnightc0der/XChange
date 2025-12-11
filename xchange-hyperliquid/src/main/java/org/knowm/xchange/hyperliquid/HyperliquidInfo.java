package org.knowm.xchange.hyperliquid;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.knowm.xchange.hyperliquid.dto.HyperliquidResponse;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidMeta;
import org.knowm.xchange.hyperliquid.dto.trade.Order;
import si.mazi.rescu.ParamsDigest;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * REST interface for Hyperliquid info API endpoints
 * Used for querying account information, open orders, user state, etc.
 */
@Path("/info")
@Produces(MediaType.APPLICATION_JSON)
public interface HyperliquidInfo {

    /**
     * Get user account state including positions, balances, and margin info (public endpoint)
     * @param contentType Content type header
     * @param requestBody Request containing type, user address, and dex
     * @return User state data
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object getUserState(
        @HeaderParam("Content-Type") String contentType,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Get open orders for a user (public endpoint, no auth required)
     * @param contentType Content type header
     * @param requestBody Request containing type, user address, and dex
     * @return Open orders data
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    List<Order> getOpenOrders(
        @HeaderParam("Content-Type") String contentType,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Get user fills/trades history
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication
     * @param requestBody Request containing type and user address
     * @return User fills data
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object getUserFills(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Generic info endpoint for all query types
     * @param contentType Content type header
     * @param signature EIP-712 signature for authentication (optional for some queries)
     * @param requestBody Request containing type and parameters
     * @return Query response data
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object query(
        @HeaderParam("Content-Type") String contentType,
        @HeaderParam("Authorization") ParamsDigest signature,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Generic info endpoint for public queries (no authentication required)
     * @param contentType Content type header
     * @param requestBody Request containing type and parameters
     * @return Query response data
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    Object queryPublic(
        @HeaderParam("Content-Type") String contentType,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Get exchange metadata including asset universe
     * @param contentType Content type header
     * @param requestBody Request containing type="meta" and optional dex parameter
     * @return Exchange metadata
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    HyperliquidMeta getMeta(
        @HeaderParam("Content-Type") String contentType,
        Map<String, Object> requestBody
    ) throws IOException;
}