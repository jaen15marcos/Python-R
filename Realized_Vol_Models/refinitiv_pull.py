###Historical Constituents of S&P 500 in Refinitiv (Get Leavers)
import refinitiv.data as rd
import refinitiv.data.eikon as ek
rd.open_session()


f = ['TR.IndexJLConstituentChangeDate','TR.IndexJLConstituentName', 'TR.IndexJLConstituentRIC','TR.IndexJLConstituentComName'] #Define Fields to get
rics, err = ek.get_data(instruments=".SPX", fields=f, parameters={'IC':'L', 'SDate':'2004-07-01', 'EDate': '2024-07-01'})

##
##Current Constituents of S&P 500 in Refinitiv
##
rics, err = ek.get_data(instruments=".SPX", fields=["TR.IndexConstituentRIC", "TR.IndexConstituentName"], parameters={'SDate':'0D', 'EDate':'-1CY'})
Historical Constituents on a given Day
rics, err =ek.get_data('0#.SPX(2014-01-01)', fields=["TR.IndexConstituentRIC","TR.IndexConstituentName"], parameters={'SDate': '2014-01-01', 'Period': 'FY0'})

##
###Historical 30-minute intraday price data for each constituent of the S&P 500 from 2014 to 2024
##
import refinitiv.dataplatform as rdp

rics_list = [] #list with S&P 500 RICs
start_date_ts = '01-05-2024'
end_date_ts = '04-05-2024'
fields_list = ['OPEN_PRC','HIGH_1','LOW_1','TRDPRC_1']
response = rdp.historical_pricing.summaries.Definition(rics_list,
                                                   start=start_date_ts,
                                                   end=end_date_ts,
                                                   fields = fields_list,
                                                 interval= historical_pricing.Intervals.THIRTY_MINUTES).get_data()
##
###Alternatively, for only closing_price, opening_price, max_price and min_price
##
df = rd.get_data(rics_list, fields=['TR.CLOSEPRICE.Date', 'TR.CLOSEPRICE', 'TR.OPENPRICE.Date', 'TR.OPENPRICE', 'TR.HIGHPRICE.Date', 'TR.HIGHPRICE', 'TR.LOWPRICE.Date', 'TR.LOWPRICE'], parameters = {'SDate':'2014-01-01'
                 , 'EDate': '2024-07-01'
                 , 'Frq':'D'}
