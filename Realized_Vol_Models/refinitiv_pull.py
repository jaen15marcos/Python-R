Project Realized Volatility 
1.	GET Historical Constituents of S&P 500 in Refinitiv (Get Leavers)
import refinitiv.data as rd
import refinitiv.data.eikon as ek
rd.open_session()

#Define Fields to get
f = ['TR.IndexJLConstituentChangeDate','TR.IndexJLConstituentName', 'TR.IndexJLConstituentRIC','TR.IndexJLConstituentComName']

rics, err = ek.get_data(instruments=".SPX", fields=f, parameters={'IC':'L', 'SDate':'2004-07-01', 'EDate': '2024-07-01'})
Get Current 
rics, err = ek.get_data(instruments=".SPX", fields=["TR.IndexConstituentRIC", "TR.IndexConstituentName"], parameters={'SDate':'0D', 'EDate':'-1CY'})
Historical Constituents on a given Day
rics, err =ek.get_data('0#.SPX(2014-01-01)', fields=["TR.IndexConstituentRIC","TR.IndexConstituentName"], parameters={'SDate': '2014-01-01', 'Period': 'FY0'})

2.	Get Historical 30-minute intraday price data for each constituent of the S&P 500 from 2014 to 2024

import refinitiv.dataplatform as rdp

rics_list = ['VOD.L','RR.L']
start_date_ts = '01-05-2024'
end_date_ts = '04-05-2024'
fields_list = ['OPEN_PRC','HIGH_1','LOW_1','TRDPRC_1']
response = rdp.historical_pricing.summaries.Definition(rics_list,
                                                   start=start_date_ts,
                                                   end=end_date_ts,
                                                   fields = fields_list,
                                                 interval= historical_pricing.Intervals.THIRTY_MINUTES).get_data()

or 

df = rd.get_data(ric, fields=['TR.CLOSEPRICE.Date', 'TR.CLOSEPRICE', 'TR.OPENPRICE.Date', 'TR.OPENPRICE', 'TR.HIGHPRICE.Date', 'TR.HIGHPRICE', 'TR.LOWPRICE.Date', 'TR.LOWPRICE'], parameters = {'SDate':'2014-01-01'
                 , 'EDate': '2024-07-01'
                 , 'Frq':'D'})
3.	Getting Market Data from Refinitiv
import refinitiv.data as rd
rd.open_session()


df = rd.get_data(ric, fields=['TR.Volume.Date', 'TR.Volume', 'TR.BetaFiveYear.Date', 'TR.BetaFiveYear', 'TR.CompanyMarketCap.Date', 'TR.CompanyMarketCap', 'TR.PriceTargetMean.Date', 'TR.PriceTargetMean','TR.ASKPRICE.Date', 'TR.ASKPRICE', 'TR.BIDPRICE.Date', 'TR.BIDPRICE', 'TR.F.ComShrOutsTot.Date', 'TR.F.ComShrOutsTot','TR.F.TotAssets.Date', 'TR.F.TotAssets'], parameters = {'SDate':'2014-01-01', 'EDate': '2014-07-01', 'Frq':'D'})

4.	Get Earning Dates Data
df = rd.get_data(ric,['TR.EPSActValue.Date','TR.EPSActValue'],parameters = {'SDate':'0','EDate':'-45', 'Period':'FQ0', 'Frq':'FQ'})
5.	Economic Calendar
ek.get_data(['USFOMC=ECI'],['DSPLY_NMLL','RELEVANCE','ACT_REL_ST','ECI_ACT_DT','ACT_VAL_NS','UNIT_PREFX','FCAST_PRD','RPT_UNITS','ECON_ACT','ECON_PRIOR','RTR_POLL','FCAST_SEST','FCAST_ACRY','NDOR_1'],{'Frq':'M'})

or
df,err = rd.get_data('0#ECONALLUS',['DSPLY_NAME','GN_TXT16_3','GN_TXT16_4','OFFC_CODE2'])
df.rename(columns={'DSPLY_NAME':'Indicator', 'GN_TXT16_3':'GMT Date','GN_TXT16_4':'Actual','OFFC_CODE2':'Reuters Poll'}, inplace=True)
