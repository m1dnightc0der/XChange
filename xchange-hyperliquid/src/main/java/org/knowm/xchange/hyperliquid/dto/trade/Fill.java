package org.knowm.xchange.hyperliquid.dto.trade;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.math.BigDecimal;

/**
 * Represents a fill (trade execution) on Hyperliquid
 * Returned by the userFillsByTime API endpoint
 *
 * Example response:
 * {
 *   "coin": "ETH",
 *   "px": "4372.7",
 *   "sz": "0.003",
 *   "side": "A",
 *   "time": 1760009147803,
 *   "startPosition": "0.0",
 *   "dir": "Open Short",
 *   "closedPnl": "0.0",
 *   "hash": "0xbdacb390aeff6c5cbf26042d236cd402047b007649f28b2e61755ee36df34647",
 *   "oid": 192129729628,
 *   "crossed": true,
 *   "fee": "0.005903",
 *   "tid": 382663056801577,
 *   "feeToken": "USDC",
 *   "twapId": null
 * }
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Fill {

    /**
     * Coin/asset name (e.g., "BTC", "ETH")
     */
    @JsonProperty("coin")
    private String coin;

    /**
     * Fill price as string
     */
    @JsonProperty("px")
    private String price;

    /**
     * Get fill price as BigDecimal
     */
    public BigDecimal getPriceAsBigDecimal() {
        try {
            return new BigDecimal(price);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Fill size as string
     */
    @JsonProperty("sz")
    private String size;

    /**
     * Get fill size as BigDecimal
     */
    public BigDecimal getSizeAsBigDecimal() {
        try {
            return new BigDecimal(size);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Side of the trade
     * "A" = Ask (sell/short)
     * "B" = Bid (buy/long)
     */
    @JsonProperty("side")
    private String side;

    /**
     * Unix timestamp in milliseconds when the fill occurred
     */
    @JsonProperty("time")
    private long time;

    /**
     * Position size before this fill as string
     */
    @JsonProperty("startPosition")
    private String startPosition;

    /**
     * Get start position as BigDecimal
     */
    public BigDecimal getStartPositionAsBigDecimal() {
        try {
            return new BigDecimal(startPosition);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Direction of the trade
     * Examples: "Open Long", "Open Short", "Close Long", "Close Short"
     */
    @JsonProperty("dir")
    private String direction;

    /**
     * Realized PnL from this fill as string
     * Only non-zero when closing a position
     */
    @JsonProperty("closedPnl")
    private String closedPnl;

    /**
     * Get closed PnL as BigDecimal
     */
    public BigDecimal getClosedPnlAsBigDecimal() {
        try {
            return new BigDecimal(closedPnl);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Transaction hash on the blockchain
     */
    @JsonProperty("hash")
    private String hash;

    /**
     * Order ID that was filled
     */
    @JsonProperty("oid")
    private Long orderId;

    /**
     * Whether this was a crossing (taker) order
     * true = taker, false = maker
     */
    @JsonProperty("crossed")
    private Boolean crossed;

    /**
     * Fee paid for this fill as string
     */
    @JsonProperty("fee")
    private String fee;

    /**
     * Get fee as BigDecimal
     */
    public BigDecimal getFeeAsBigDecimal() {
        try {
            return new BigDecimal(fee);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Trade ID - unique identifier for this fill
     */
    @JsonProperty("tid")
    private Long tradeId;

    /**
     * Token used to pay the fee (e.g., "USDC")
     */
    @JsonProperty("feeToken")
    private String feeToken;

    /**
     * TWAP (Time-Weighted Average Price) order ID if applicable
     * null if not part of a TWAP order
     */
    @JsonProperty("twapId")
    private Long twapId;

    /**
     * Check if this fill is for a buy order
     * @return true if side is "B" (bid/buy)
     */
    public boolean isBuy() {
        return "B".equals(side);
    }

    /**
     * Check if this fill is for a sell order
     * @return true if side is "A" (ask/sell)
     */
    public boolean isSell() {
        return "A".equals(side);
    }

    /**
     * Check if this is a maker fill
     * @return true if not crossed (maker)
     */
    public boolean isMaker() {
        return crossed != null && !crossed;
    }

    /**
     * Check if this is a taker fill
     * @return true if crossed (taker)
     */
    public boolean isTaker() {
        return crossed != null && crossed;
    }
}
