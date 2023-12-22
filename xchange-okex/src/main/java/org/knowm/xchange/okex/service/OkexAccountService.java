package org.knowm.xchange.okex.service;

import org.knowm.xchange.client.ResilienceRegistries;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.derivative.FuturesContract;
import org.knowm.xchange.derivative.OptionsContract;
import org.knowm.xchange.dto.account.AccountInfo;
import org.knowm.xchange.instrument.Instrument;
import org.knowm.xchange.okex.OkexAdapters;
import org.knowm.xchange.okex.OkexExchange;
import org.knowm.xchange.okex.dto.OkexException;
import org.knowm.xchange.okex.dto.OkexResponse;
import org.knowm.xchange.okex.dto.account.*;
import org.knowm.xchange.service.account.AccountService;
import org.knowm.xchange.service.trade.params.DefaultWithdrawFundsParams;
import org.knowm.xchange.service.trade.params.WithdrawFundsParams;

import java.io.IOException;
import java.util.List;

import static org.knowm.xchange.okex.OkexAdapters.*;

/** Author: Max Gao (gaamox@tutanota.com) Created: 08-06-2021 */
public class OkexAccountService extends OkexAccountServiceRaw implements AccountService {

  public OkexAccountService(OkexExchange exchange, ResilienceRegistries resilienceRegistries) {
    super(exchange, resilienceRegistries);
  }

  public AccountInfo getAccountInfo() throws IOException {
    // null to get assets (with non-zero balance), remaining balance, and available amount in the
    // account.
    OkexResponse<List<OkexWalletBalance>> tradingBalances = getWalletBalances(null);
    OkexResponse<List<OkexAssetBalance>> assetBalances = getAssetBalances(null);
    OkexResponse<List<OkexAccountPositionRisk>> positionRis = getAccountPositionRisk();
    return new AccountInfo(
        OkexAdapters.adaptOkexBalances(tradingBalances.getData()),
        OkexAdapters.adaptOkexAssetBalances(assetBalances.getData()),
        OkexAdapters.adaptOkexAccountPositionRisk(positionRis.getData())
    );
  }


  public OkexResponse<List<OkexSetLeverageResponse>> setLeverage(Instrument instrument, Integer leverage, String marginMode, String positionSide)
      throws OkexException, IOException {


    if (instrument instanceof FuturesContract ) {
      return setLeverage(OkexAdapters.adaptInstrument(instrument), "",leverage.toString(),  marginMode,  positionSide);


    } else if (instrument instanceof CurrencyPair) {
 if(positionSide== null || (positionSide!= null && !positionSide.equals("short"))) {
        return setLeverage("", instrument.getCounter().getCurrencyCode(), leverage.toString(), marginMode, "");
      } else {
 return setLeverage("", instrument.getBase().getCurrencyCode(), leverage.toString(),  marginMode,  "");
 }
    } else {
      return null;
    }
  }


  public OkexResponse<List<OkexSetLeverageResponse>> getLeverage(Instrument instrument,String marginMode )
      throws OkexException, IOException {

    return getLeverage(OkexAdapters.adaptInstrument(instrument), marginMode);

  }

  @Override
  public String withdrawFunds(WithdrawFundsParams params) throws IOException {
    if (params instanceof DefaultWithdrawFundsParams) {
      DefaultWithdrawFundsParams defaultParams = (DefaultWithdrawFundsParams) params;
      String address = defaultParams.getAddressTag() != null ? defaultParams.getAddress() + ":" + defaultParams.getAddressTag() : defaultParams.getAddress();
      OkexResponse<List<OkexWithdrawalResponse>> okexResponse = assetWithdrawal(
              defaultParams.getCurrency().getCurrencyCode(),
              defaultParams.getAmount().toPlainString(),
              ON_CHAIN_METHOD,
              address,
              defaultParams.getCommission() != null ? defaultParams.getCommission().toPlainString() : null,
              null,
              null
      );
      if (!okexResponse.isSuccess())
        throw new OkexException(okexResponse.getMsg(), Integer.parseInt(okexResponse.getCode()));

      return okexResponse.getData().get(0).getWithdrawalId();
    }
    throw new IllegalStateException("Don't know how to withdraw: " + params);
  }
}
