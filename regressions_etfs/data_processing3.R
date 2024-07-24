########################################
## CSA ETF Research
########################################
#
# FUNCTIONS JOINS FILE (USING JOIN KEY)
#
# Author: Marcos Jaen Cortes, Regulatory Strategy and Research
# Created August 2023
#
# This script contains all my custom functions that may be used across the 
# project to join data from different sources
########################################



library(dplyr)
library(janitor)
library(tidyr)
library(tidyverse)
library(fuzzyjoin)
library(stringr)
library(zoo)
library(data.table)
library(qdap)


ifs.file <- 'T://Strategy_Operations//Economics Analysis//Data Sources folder mirrored on OSCER//Research and Analysis Policy Projects//CSA ETF Research//IFS//ifs.csv'

map.file <- 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\map.csv'

file.join.key <- 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\join.key.csv'


map <- fread(map.file) 
#map <- map[,-c(40:144)] 


map <- map %>% distinct(sym, listing_mkt, date, cusip, .keep_all = T) %>% group_by(sym, listing_mkt)  %>% arrange(date) %>% mutate(cusip = ifelse(cusip %in% "", tail(cusip, 1), cusip)) %>% filter(!cusip %in% "")

join.key <- fread(file.join.key)
map.join.key <- merge(map, join.key, by.x = c("sym", "listing_mkt"), 
                                      by.y = c("sym", "listing_mkt"), all.x=T, allow.cartesian = T)  %>% select(-cusip.y) %>% rename(cusip = cusip.x) %>%
  distinct(sym, listing_mkt, cusip, date, number_of_quotes, closing_price, number_of_trades, fund.name, fund.manager, nrd.number, .keep_all = TRUE) %>% mutate(date = as.Date(date))
  
refinitiv <- fread('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\Tertiary Sources\\refinitiv_full.csv')   %>%
   select('Instrument', 'Date', 'Price Close') %>% rename('Price.Close' = 'Price Close' )

refinitiv.join.key <- join.key[,c(1:2, 6)]

refinitiv.ts <- fread('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\Tertiary Sources\\refinitiv_full_ts_updated.csv')


refinitiv.join.key <- merge(refinitiv.join.key, refinitiv, by.x = "RIC", by.y = 'Instrument', all.x=T,  allow.cartesian=TRUE) %>% filter(sym != 'NA') %>% mutate(date = as.Date(Date)) %>% select(-Date) %>% distinct()


map.refinitiv <- merge(map.join.key, refinitiv.join.key, by.x = c("sym", 'listing_mkt', 'date'), by.y = c("sym", 'listing_mkt', 'date'), all.x=T) %>% distinct() %>% 
  rename(refinitiv.closing_price = Price.Close)

refinitiv.ts <- merge(refinitiv.ts, join.key[,c(1:2, 6)], by.x = c('Instrument'), by.y = c('RIC'), all.x=T, allow.cartesian=TRUE) %>% distinct() %>% select(-Instrument)


map.refinitiv <- merge(map.refinitiv, refinitiv.ts, by.x = c("sym", 'listing_mkt', 'date'), by.y = c("sym", 'listing_mkt', 'Date'), all.x=T) %>% distinct() %>%  rename(RIC = RIC.x) %>% select(-RIC.y)


# map.refinitiv <- map.refinitiv %>% ungroup() %>% mutate(diff.price = abs(Price.Close - closing_price)/closing_price) %>% group_by(sym, listing_mkt, date, cusip, number_of_trades, number_of_quotes, avg_lag_bw_trade_and_NBBO_quote)  %>% slice(which.min(diff.price)) %>% select(-diff.price) %>% ungroup()

# #map.refinitiv <- map.refinitiv[,c(1:4,35,62:70,73:80, 82, 83, 84, 86:91)] %>% rename(refinitiv.date = date, refinitiv.closing_price = Price.Close, 
#                                           refinitiv.volume = Volume, refinitiv.company.common.name = Company.Common.Name, 
#                                           refinitiv.fund.type = Fund.Type, refinitiv.fund.type1 = Fund.Type.1,  refinitiv.fund.company = Fund.Company,
#                                           refinitiv.fund.city = Fund.City, refinitiv.fund.country = Fund.County, refinitiv.fund.perid = Fund.PermID,
#                                           refinitiv.Net.Asset.Value.Per.Share...Actual = Net.Asset.Value.Per.Share...Actual, refinitiv.Share.Class = Share.Class,
#                                           refinitiv.Classification.Sector.Name = Classification.Sector.Name,
#                                           refinitiv.Price...NAV...Actual = Price...NAV...Actual, refinitiv.Instrument.Shares = Instrument.Shares,
#                                           refinitiv.Company.Market.Capitalization.Local.Currency = Company.Market.Capitalization.Local.Currency, refinitiv.INSTRUMENTMARKETCAPITALIZATIONLOCALCURN = INSTRUMENTMARKETCAPITALIZATIONLOCALCURN,
#                                           refinitiv.Issue.Default.Shares.Outstanding = Issue.Default.Shares.Outstanding, refinitiv.Number.of.Shares = Number.of.Shares,
#                                           refinitiv.Total.Number.of.Shares = Total.Number.of.Shares)


map.refinitiv.check <- map.refinitiv %>% group_by(sym, listing_mkt, RIC)  %>% drop_na(refinitiv.closing_price) %>% 
  select(sym, listing_mkt, RIC, date, closing_price, refinitiv.closing_price) %>% mutate(date = as.Date(date))%>% filter(date < as.Date(	
    '2021-12-10'))  %>% summarise(refinitiv.closing_price = mean(refinitiv.closing_price), closing_price = mean(closing_price)) %>% 
  mutate(error = 100* (abs(refinitiv.closing_price - closing_price)/closing_price)) %>% filter(error > 1)  %>% select(sym, listing_mkt, RIC, error)

map.refinitiv <- map.refinitiv %>% anti_join(map.refinitiv.check, by=c("sym", "listing_mkt")) 

#map.refinitiv <- map.refinitiv %>% select(-closing_price, -cusip)
map.refinitiv <- merge(map.join.key, map.refinitiv[,c(1:3,93:95)],  by.x = c("sym", 'listing_mkt', 'date'), by.y = c("sym", 'listing_mkt', 'date'), all.x=T)



ifs <- fread(ifs.file)
map.refinitiv$year <- as.character(map.refinitiv$year)
ifs$year <- as.character(ifs$year)
map.refinitiv.ifs <- merge(map.refinitiv, ifs, by.x = c("nrd.number", "fund.manager", "fund.name", "year"), 
                     by.y = c("nrd.number", "fund.manager", "fund.name", "year"), all.x=T)

ifs$authorized.participants <-  gsub("and ",",",ifs$authorized.participants)
ifs$authorized.participants <-  gsub("; ",",",ifs$authorized.participants)
ifs$authorized.participants <-  gsub("AND ",",",ifs$authorized.participants)
ifs <- ifs %>% filter(!authorized.participants %in% c("", "No", "0", "No ETF Series" , "not applicable", "No ETF series", "none", "Not applicable") )

map.refinitiv.ifs.join <- map.refinitiv.ifs %>% distinct(sym, listing_mkt, year, authorized.participants, allocation.methodology, daily.disclosure) %>% group_by(sym, listing_mkt) %>%
  fill(authorized.participants, .direction = "downup") %>%
  fill(authorized.participants, .direction = "updown") %>%
  fill(allocation.methodology, .direction = "downup") %>%
  fill(allocation.methodology, .direction = "updown") %>%
  fill(daily.disclosure, .direction = "downup") %>%
  fill(daily.disclosure, .direction = "updown")
map.refinitiv.ifs.join['number_of_ap'] <-  lengths(regmatches(map.refinitiv.ifs.join$authorized.participants, gregexpr(",", map.refinitiv.ifs.join$authorized.participants))) + 1 + lengths(regmatches(map.refinitiv.ifs.join$authorized.participants, gregexpr(" and ", map.refinitiv.ifs.join$authorized.participants)))

map.refinitiv.ifs <- merge(map.refinitiv.ifs, map.refinitiv.ifs.join,  by.x = c("sym", 'listing_mkt', 'year'), by.y = c("sym", 'listing_mkt', 'year'), all.x=T, allow.cartesian = TRUE)
map.refinitiv.ifs <- map.refinitiv.ifs %>% select(-allocation.methodology.x, -daily.disclosure.x, -authorized.participants.x) %>% 
  rename(authorized.participants = authorized.participants.y, allocation.methodology = allocation.methodology.y, daily.disclosure = daily.disclosure.y) 

#calculations <- map.refinitiv.ifs
#map.refinitiv.ifs <- map.refinitiv.ifs[,c(1:20, 25:28, 39:47, 52, 62,64:57,67,68:72,78:81,84, 102:132,  225:227)] 

bloomberg <- read.csv('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\Tertiary Sources\\bloomberg_data.csv') %>% group_by(X, Dates) %>% dplyr::select(-X.NAME.) %>% rename(sym = X, variable = Dates) %>% distinct()


 
bloomberg.longer <-pivot_longer(bloomberg, cols=c(-1,-2), names_to = c("date")) %>% filter(value != "#N/A N/A") 
# 
bloomberg.longer$date  <- str_remove(bloomberg.longer$date, 'X')  
bloomberg.longer$date  <- as.Date(bloomberg.longer$date,format="%Y.%m.%d")  
# 
bloomberg.longer$sym <- gsub( " .*$", "", bloomberg.longer$sym) #1190
# 
# ###
bloomberg.longer.sym <- bloomberg.longer %>% ungroup() %>%  distinct(sym)
securities <- map %>% distinct(sym, listing_mkt)
# 
fuzzy.join.test <- stringdist_inner_join(securities, bloomberg.longer.sym, by = 'sym') %>%
  mutate(distance = 1 - RecordLinkage::levenshteinSim(sym.x, sym.y))
# 
 fuzzy.join.test.p <- fuzzy.join.test %>% filter(distance == 0)
# 
 fuzzy.join.test <- fuzzy.join.test  %>% anti_join(fuzzy.join.test.p,by= c("sym.x", "listing_mkt", "sym.y")) %>% filter(!(sym.y %in% unique(fuzzy.join.test.p$sym.y))) %>% filter(distance <= 0.2000000)
# 
 fuzzy.join.test.p <- rbind(fuzzy.join.test.p, fuzzy.join.test) %>% select(-distance)
# 
bloomberg.longer <- merge(bloomberg.longer, fuzzy.join.test.p, by.x = c('sym'), by.y = c('sym.y')) %>% rename(sym.bloomberg = sym ,sym = sym.x) %>% select(sym, listing_mkt, date, variable, value) %>% pivot_wider(names_from = variable, values_from = value)
# 
# 
 bloomberg.longer <- bloomberg.longer %>% rename(bloomberg.closing_price = PX_LAST, bloomberg.NAV = FUND_NET_ASSET_VAL, 
                                                bloomberg.volume = PX_VOLUME, bloomberg.market.cap = CUR_MKT_CAP, bloomberg.nav.per.share = BS_NET_ASSET_VALUE_PER_SHARE, 
                                                 bloomberg.outstanding.shares = BS_SH_OUT, bloomberg.hist.market.cap = HISTORICAL_MARKET_CAP, bloomberg.num.trades = NUM_TRADES)
# 
 bloomberg.longer$bloomberg.closing_price   <- as.numeric(as.character(bloomberg.longer$bloomberg.closing_price)) 
 bloomberg.longer$bloomberg.NAV   <- as.numeric(as.character(bloomberg.longer$bloomberg.NAV)) 
 bloomberg.longer$bloomberg.volume   <- as.numeric(as.character(bloomberg.longer$bloomberg.volume)) 
 bloomberg.longer$bloomberg.market.cap   <- as.numeric(as.character(bloomberg.longer$bloomberg.market.cap)) 
# 
# 
 bloomberg.longer$bloomberg.num.trades   <- as.numeric(as.character(bloomberg.longer$bloomberg.num.trades)) 
 bloomberg.longer$bloomberg.nav.per.share   <- as.numeric(as.character(bloomberg.longer$bloomberg.nav.per.share)) 
bloomberg.longer$bloomberg.hist.market.cap   <- as.numeric(as.character(bloomberg.longer$bloomberg.hist.market.cap)) 
bloomberg.longer$bloomberg.outstanding.shares   <- as.numeric(as.character(bloomberg.longer$bloomberg.outstanding.shares))
# 
bloomberg.longer$date  <- as.Date(bloomberg.longer$date) 
map$date  <- as.Date(map$date) 
# 
map.bloomberg <- merge(map, bloomberg.longer, by = c('sym', 'listing_mkt', 'date'))
# 
map.bloomberg.check <- map.bloomberg %>% group_by(sym, listing_mkt)  %>% drop_na(bloomberg.closing_price) %>%
   select(sym, listing_mkt, closing_price, bloomberg.closing_price) %>% summarise(bloomberg.closing_price = mean(bloomberg.closing_price), closing_price = mean(closing_price)) %>% 
   mutate(error = 100* (abs(bloomberg.closing_price - closing_price)/closing_price)) %>% filter(error > 1)  %>% select(sym, listing_mkt, error)

map.bloomberg <- map.bloomberg %>% anti_join(map.bloomberg.check, by=c("sym", "listing_mkt")) 

#map.bloomberg <- map.bloomberg[,c(1:3,39:46)] 

#map.refinitiv.ifs <- map.refinitiv.ifs %>% select(-c("Base Currency" ))
#map.refinitiv.ifs.bloomberg <- merge(map.refinitiv.ifs, map.bloomberg[,c(1:3,52,54)],  by.x = c("sym", 'listing_mkt', 'date'), by.y = c("sym", 'listing_mkt', 'date'), all.x=T)

map.refinitiv.ifs <-  map.refinitiv.ifs.bloomberg %>% filter(!(sym %in% c("BKCC","QQCC","CNCC","BBIG.U", "CGRN.U", "CHPS.U", "DAMG.U", "DANC.U", "EPCA.U", "EPGC.U", "EPZA.U", "FCGB.U", "FCIG.U", "FCIQ.U", "FCUL.U", "FCUQ.U", "FETH.U", "FLX.B", 
                                                                                    "HESG",   "HUM.U",  "HYLD.U", "SBT.U",  "TUED.U", "USCC.U", "XFS.U",  "ZPR.U",
                                                                                    "HISU.U", "RBOT.U", "HXDM.U", "HXT.U",  "DLR.U",  "ZUS.V",  "HTB.U",  "BPRF.U", "LIFE.U", "ZUP.U",  "ZSML.U", 
                                                                                    "ZMID.U", "CALL.U", "FCRR.U", "BITI.U", "BTCQ.U", "FCUD.U", "ETHQ.U", "FBTC.U", "FCUV.U", "FCMO.U", "ETHY.U"))) #Issue with NAV

tableau <- map.refinitiv.ifs %>% distinct(sym, listing_mkt, year, fund.type) %>% group_by(sym, listing_mkt) %>%
  fill(fund.type, .direction = "downup") %>%
  fill(fund.type, .direction = "updown")


map.refinitiv.ifs <- merge(map.refinitiv.ifs, tableau,  by.x = c("sym", 'listing_mkt', 'year'), by.y = c("sym", 'listing_mkt', 'year'), all.x=T, allow.cartesian = TRUE) %>%distinct()
map.refinitiv.ifs <- map.refinitiv.ifs %>% select(-fund.type.x) %>% 
  rename(fund.type = fund.type.y) 



map.refinitiv.ifs <- map.refinitiv.ifs %>% mutate(fund.type = ifelse(fund.type %in% c("Alternative mutual fund", 'ETF, alternative mutual fund', 'Mutual fund', "Money market") &  !fund.type %in% c("NA"), "Not", 
                                                                     ifelse(fund.type %in% c("ETF"),"ETF", "Null")))

map.refinitiv.ifs <- map.refinitiv.ifs[,-c(95:246)] 
broker.trades <- fread('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\broker.trades.csv')

broker.trades <- broker.trades %>% select(-po) %>%  group_by(sym, listing_mkt, date) %>%
  pivot_wider(names_from = institution.name,
              values_from = c(b_volume, b_total_value, b_num_trades, s_volume, s_total_value, s_num_trades, s_mm, s_mm_volume, s_mm_total_value, b_mm, b_mm_volume, b_mm_total_value))

#output <- merge(map.refinitiv.ifs, broker.trades, by=c('sym', 'listing_mkt', 'date'), all.x = T, allow.cartesian = T) 

map.refinitiv.ifs$year <- format(as.Date(map.refinitiv.ifs$date, format="%Y-%m-%d"),"%Y")
#output$Outstanding.Shares <- ifelse(is.na(output$Outstanding.Shares) == T & is.na(output$Issue.Market.Cap) == F, output$Issue.Market.Cap/output$closing_price, output$Outstanding.Shares)
fwrite(map.refinitiv.ifs, 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\full.data.final.csv', row.names = FALSE)
output <- fread('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\full.data.final.csv')
output.selected$year <- format(as.Date(output.selected$date, format="%Y-%m-%d"),"%Y")
fwrite(output, 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\full.data.final.csv', row.names = FALSE)
