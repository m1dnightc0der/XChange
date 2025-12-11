package org.knowm.xchange.service.trade.params.orders;

import org.knowm.xchange.instrument.Instrument;

public class DefaultQueryClientOrderIDParamInstrument extends DefaultQueryClientOrderIDParam
    implements ClientOrderIdQueryParamInstrument {
  private Instrument instrument;

  public DefaultQueryClientOrderIDParamInstrument(Instrument instrument, String clientOid) {
    super(clientOid);
    this.instrument = instrument;
  }

  @Override
  public Instrument getInstrument() {
    return instrument;
  }

  @Override
  public void setInstrument(Instrument instrument) {
    this.instrument = instrument;
  }
}
