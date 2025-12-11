package org.knowm.xchange.hyperliquid;

import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidAllMids;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidL2Book;
import org.knowm.xchange.hyperliquid.dto.marketdata.HyperliquidCandleSnapshot;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Map;

/**
 * REST interface for Hyperliquid public API
 */
@Path("/info")
@Produces(MediaType.APPLICATION_JSON)
public interface Hyperliquid {

    /**
     * Get mid prices for all coins
     * @return Map of coin symbols to mid prices
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    HyperliquidAllMids getAllMids(@HeaderParam("Content-Type") String contentType,Map<String, Object> requestBody) throws IOException;

    /**
     * Get order book for a specific coin
     * @param requestBody Request containing coin and optional precision
     * @return L2 order book data
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    HyperliquidL2Book getL2Book(
        @HeaderParam("Content-Type") String contentType,
        Map<String, Object> requestBody
    ) throws IOException;

    /**
     * Get candlestick data for a coin
     * @param requestBody Request containing coin, interval, startTime, endTime
     * @return Candlestick data
     * @throws IOException
     */
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    HyperliquidCandleSnapshot getCandleSnapshot(
        @HeaderParam("Content-Type") String contentType,
        Map<String, Object> requestBody
    ) throws IOException;
}