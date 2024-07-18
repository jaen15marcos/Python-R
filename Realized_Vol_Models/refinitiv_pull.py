import refinitiv.data as rd
import refinitiv.data.eikon as ek
import refinitiv.dataplatform as rdp
import data_collection
from refinitiv.dataplatform import historical_pricing


# Open Refinitiv session
rd.open_session()

#Get Leavers
def get_historical_sp500_constituents(start_date, end_date):
    """Get historical constituents of S&P 500 (leavers)"""
    fields = ['TR.IndexJLConstituentChangeDate', 'TR.IndexJLConstituentName', 
              'TR.IndexJLConstituentRIC', 'TR.IndexJLConstituentComName']
    rics, err = ek.get_data(instruments=".SPX", fields=fields, 
                            parameters={'IC':'L', 'SDate':start_date, 'EDate':end_date})
    return rics

def get_current_sp500_constituents():
    """Get current S&P 500 constituents"""
    rics, err = ek.get_data(instruments=".SPX", 
                            fields=["TR.IndexConstituentRIC", "TR.IndexConstituentName"], 
                            parameters={'SDate':'0D', 'EDate':'-1CY'})
    return rics

def get_sp500_constituents_on_date(date):
    """Get S&P 500 constituents on a specific date"""
    rics, err = ek.get_data(f'0#.SPX({date})', 
                            fields=["TR.IndexConstituentRIC", "TR.IndexConstituentName"], 
                            parameters={'SDate': date, 'Period': 'FY0'})
    return rics

def get_intraday_price_data(rics_list, start_date, end_date, interval=historical_pricing.Intervals.THIRTY_MINUTES):
    """Get historical intraday price data for given RICs"""
    fields_list = ['OPEN_PRC', 'HIGH_1', 'LOW_1', 'TRDPRC_1']
    response = rdp.historical_pricing.summaries.Definition(
        rics_list,
        start=start_date,
        end=end_date,
        fields=fields_list,
        interval=interval
    ).get_data()
    return response

def get_daily_price_data(ric, start_date, end_date):
    """Get daily price data for a given RIC"""
    fields = ['TR.CLOSEPRICE.Date', 'TR.CLOSEPRICE', 'TR.OPENPRICE.Date', 'TR.OPENPRICE', 
              'TR.HIGHPRICE.Date', 'TR.HIGHPRICE', 'TR.LOWPRICE.Date', 'TR.LOWPRICE']
    df = rd.get_data(ric, fields=fields, 
                     parameters={'SDate':start_date, 'EDate':end_date, 'Frq':'D'})
    return df

def get_market_data(ric, start_date, end_date):
    """Get various market data for a given RIC"""
    fields = ['TR.Volume.Date', 'TR.Volume', 'TR.BetaFiveYear.Date', 'TR.BetaFiveYear', 
              'TR.CompanyMarketCap.Date', 'TR.CompanyMarketCap', 'TR.PriceTargetMean.Date', 
              'TR.PriceTargetMean', 'TR.ASKPRICE.Date', 'TR.ASKPRICE', 'TR.BIDPRICE.Date', 
              'TR.BIDPRICE', 'TR.F.ComShrOutsTot.Date', 'TR.F.ComShrOutsTot', 
              'TR.F.TotAssets.Date', 'TR.F.TotAssets']
    df = rd.get_data(ric, fields=fields, 
                     parameters={'SDate':start_date, 'EDate':end_date, 'Frq':'D'})
    return df

def get_earnings_data(ric):
    """Get earnings data for a given RIC"""
    df = rd.get_data(ric, ['TR.EPSActValue.Date', 'TR.EPSActValue'], 
                     parameters={'SDate':'0', 'EDate':'-45', 'Period':'FQ0', 'Frq':'FQ'})
    return df

def get_economic_calendar():
    """Get economic calendar data"""
    df, err = ek.get_data(['USFOMC=ECI'], 
                          ['DSPLY_NMLL', 'RELEVANCE', 'ACT_REL_ST', 'ECI_ACT_DT', 'ACT_VAL_NS', 
                           'UNIT_PREFX', 'FCAST_PRD', 'RPT_UNITS', 'ECON_ACT', 'ECON_PRIOR', 
                           'RTR_POLL', 'FCAST_SEST', 'FCAST_ACRY', 'NDOR_1'], 
                          {'Frq':'M'})
    return df

def get_economic_indicators():
    """Get economic indicators data"""
    df, err = rd.get_data('0#ECONALLUS', ['DSPLY_NAME', 'GN_TXT16_3', 'GN_TXT16_4', 'OFFC_CODE2'])
    df.rename(columns={'DSPLY_NAME':'Indicator', 'GN_TXT16_3':'GMT Date',
                       'GN_TXT16_4':'Actual', 'OFFC_CODE2':'Reuters Poll'}, inplace=True)
    return df

# Example usage
if __name__ == "__main__":
    start_date = '2014-01-01'
    end_date = '2024-07-01'
    ric = collected_data['refinitiv_identifiers'][['Instrument']].values.flatten().tolist()  # output of data_collection

    historical_constituents = get_historical_sp500_constituents(start_date, end_date)
    current_constituents = get_current_sp500_constituents()
    constituents_2014 = get_sp500_constituents_on_date('2014-01-01')
    
    intraday_data = get_intraday_price_data([ric], '2024-05-01', '2024-05-04')
    daily_data = get_daily_price_data(ric, start_date, end_date)
    market_data = get_market_data(ric, start_date, end_date)
    earnings_data = get_earnings_data(ric)
    economic_calendar = get_economic_calendar()
    economic_indicators = get_economic_indicators()

