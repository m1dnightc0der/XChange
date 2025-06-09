package org.knowm.xchange.okex.dto.marketdata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;

import java.util.List;

@Getter
public class OkexOrderbook {

    private final List<OkexPublicOrder> asks;

    private final List<OkexPublicOrder> bids;
    private final String ts;
    private final Long checksum;
    private final Long prevSeqId;
    private final Long seqId;

    @JsonCreator
    public OkexOrderbook(
            @JsonProperty("asks") List<OkexPublicOrder> asks,
            @JsonProperty("bids") List<OkexPublicOrder> bids,
            @JsonProperty("ts") String ts,
            @JsonProperty("checksum") Long checksum,
            @JsonProperty("prevSeqId") Long prevSeqId,
            @JsonProperty("seqId") Long seqId

    ) {


        this.asks = asks;
        this.bids = bids;
        this.ts = ts;
        this.checksum = checksum;
        this.prevSeqId = prevSeqId;
        this.seqId = seqId;
    }

    @Override
    public String toString() {
        return "OkexOrderbookResponse{" + "asks=" + asks + ", bids=" + bids + '}';
    }
}
