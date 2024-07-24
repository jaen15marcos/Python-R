########################################
## CSA ETF Research
########################################
#
# FUNCTIONS JOINS FILE
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

#Takes raw IFS data and extracts ETF data 
file1 <- 'T://Strategy_Operations//Economics Analysis//Data Sources folder mirrored on OSCER//Research and Analysis Policy Projects//CSA ETF Research//IFS//ifs.csv'

file2 <- 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\trading_etfs.csv'

file3 <- 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\IFS\\ifs_clean_cusips.csv'

ifs.etf <- read.csv(file1) %>% filter(grepl('etf|mutual|money', fund.type, 
                                            ignore.case = T), year == 2022) %>% filter(!grepl('no etf', designated.broker, ignore.case = T)) %>% distinct() 

map.etfs <- read.csv(file2)

ifs.etf.w.cusips <- ifs.etf %>% drop_na(fund.cusip) %>% mutate(fund.cusip = strsplit(as.character(fund.cusip), ",")) %>% 
  unnest(fund.cusip) %>% distinct() 

fuzzy.join.ifs.etf.w.cusips <- stringdist_inner_join(ifs.etf.w.cusips, map.etfs, by = 'fund.cusip') %>%
  mutate(distance = 1 - RecordLinkage::levenshteinSim(fund.cusip.x, fund.cusip.y))  %>%
  arrange(distance) 

perfect.fuzzy.join.ifs.etf.w.cusips <- fuzzy.join.ifs.etf.w.cusips %>% filter(distance == 0) %>% 
  select(RIC, sym, listing_mkt, fund.cusip.y, fund.name.x, year, nrd.number, fund.manager, fund.category, fund.class, 
         fund.type, strategy, authorized.participants, designated.broker, prime.broker, distance) %>% 
  rename(fund.cusip = fund.cusip.y, fund.name = fund.name.x) %>% distinct()

map.etfs <- map.etfs %>% anti_join(perfect.fuzzy.join.ifs.etf.w.cusips, 
                                   by=c("RIC", "sym", "listing_mkt", "fund.cusip"))

remaining.ifs.etf.w.cusips <- anti_join(ifs.etf.w.cusips, perfect.fuzzy.join.ifs.etf.w.cusips, by=c('fund.cusip','nrd.number', 'fund.name', 'fund.manager'))

remaining.ifs.etf.w.cusips$fund.cusip <- gsub("[[:space:]]", "", remaining.ifs.etf.w.cusips$fund.cusip) #eliminate spaces
map.etfs$fund.cusip <- gsub("[[:space:]]", "", map.etfs$fund.cusip)

remaining.fuzzy.join.ifs.etf.w.cusips <- stringdist_inner_join(remaining.ifs.etf.w.cusips, map.etfs, by = 'fund.cusip') %>%
  mutate(distance = 1 - RecordLinkage::levenshteinSim(fund.cusip.x, fund.cusip.y))  %>%
  arrange(distance) 

p.remaining.fuzzy.join.ifs.etf.w.cusips <- remaining.fuzzy.join.ifs.etf.w.cusips %>%  filter(distance == 0) %>% 
  select(RIC, sym, listing_mkt, fund.cusip.y, fund.name.x, year, nrd.number, fund.manager, fund.category, fund.class, 
         fund.type, strategy, authorized.participants, designated.broker, prime.broker, distance) %>% 
  rename(fund.cusip = fund.cusip.y, fund.name = fund.name.x) %>% distinct()

perfect.fuzzy.join.ifs.etf.w.cusips <- rbind(perfect.fuzzy.join.ifs.etf.w.cusips,p.remaining.fuzzy.join.ifs.etf.w.cusips) %>% distinct()

#remaining.fuzzy.join.ifs.etf.w.cusips <- remaining.fuzzy.join.ifs.etf.w.cusips %>%  filter(distance != 0)

map.etfs <- map.etfs %>% anti_join(perfect.fuzzy.join.ifs.etf.w.cusips, 
                                   by=c("RIC", "sym", "listing_mkt", "fund.cusip"))

ifs.etf <- read.csv(file1) %>% filter(grepl('etf|mutual|money', fund.type, 
                                            ignore.case = T)) %>% 
  filter(!grepl('no etf', designated.broker, ignore.case = T)) %>% distinct() %>% group_by(year,nrd.number,fund.name, fund.manager) %>% mutate(fund.cusip = na.locf(fund.cusip,  na.rm = FALSE)) %>% 
  ungroup()


ifs.etf.w.cusips <- ifs.etf %>% anti_join(perfect.fuzzy.join.ifs.etf.w.cusips, 
                                          by=c('fund.cusip','nrd.number', 'fund.name', 'fund.manager'))

ifs.etf.w.cusips <- ifs.etf.w.cusips %>% drop_na(fund.cusip) %>% mutate(fund.cusip = strsplit(as.character(fund.cusip), ",")) %>% unnest(fund.cusip) %>% distinct() 

ifs.etf.w.cusips$fund.cusip <- gsub("[[:space:]]", "", ifs.etf.w.cusips$fund.cusip) 

fuzzy.join.ifs.etf.w.cusips <- stringdist_inner_join(ifs.etf.w.cusips, map.etfs, by = 'fund.cusip') %>%
  mutate(distance = 1 - RecordLinkage::levenshteinSim(fund.cusip.x, fund.cusip.y))  %>%
  arrange(distance) %>%  filter(distance == 0) %>% select(RIC, sym, listing_mkt, fund.cusip.y, fund.name.x, year, nrd.number, fund.manager, fund.category, fund.class, fund.type, strategy, authorized.participants, designated.broker, prime.broker, distance) %>% rename(fund.cusip = fund.cusip.y, fund.name = fund.name.x) %>% distinct()

perfect.fuzzy.join.ifs.etf.w.cusips <- rbind(perfect.fuzzy.join.ifs.etf.w.cusips,fuzzy.join.ifs.etf.w.cusips) %>% distinct()

map.etfs <- map.etfs %>% anti_join(perfect.fuzzy.join.ifs.etf.w.cusips, 
                                   by=c("RIC", "sym", "listing_mkt", "fund.cusip"))
manual.input.ifs.cusips <- read.csv(file3)

manual.input.ifs.cusips$fund.cusip <- gsub("[[:space:]]", "", manual.input.ifs.cusips$fund.cusip) 

fuzzy.join.ifs.etf.w.cusips <- stringdist_inner_join(manual.input.ifs.cusips, map.etfs, by = 'fund.cusip') %>%
  mutate(distance = 1 - RecordLinkage::levenshteinSim(fund.cusip.x, fund.cusip.y))  %>%
  arrange(distance) %>%  filter(distance == 0) %>% distinct() %>% rename(fund.cusip = fund.cusip.y, fund.name = fund.name.x) %>% select(RIC, sym, listing_mkt, fund.cusip, fund.name, distance)

fuzzy.join.ifs.etf.w.cusips <-merge(x=fuzzy.join.ifs.etf.w.cusips,y=ifs.etf, 
                                    by = 'fund.name') %>% select(RIC, sym, listing_mkt, fund.cusip.x, fund.name, year, nrd.number, fund.manager, fund.category, fund.class, fund.type, strategy, authorized.participants, designated.broker, prime.broker, distance) %>% rename(fund.cusip = fund.cusip.x) %>% filter(distance == 0)  %>% distinct()

perfect.fuzzy.join.ifs.etf.w.cusips  <- rbind(perfect.fuzzy.join.ifs.etf.w.cusips,fuzzy.join.ifs.etf.w.cusips) %>% distinct()

map.etfs <- map.etfs %>% anti_join(perfect.fuzzy.join.ifs.etf.w.cusips, 
                                   by=c("RIC", "sym", "listing_mkt", "fund.cusip"))


#length(unique(perfect.fuzzy.join.ifs.etf.w.cusips$fund.name))

ifs.etf <- read.csv(file1) %>% filter(grepl('etf|mutual|money', fund.type, 
                                            ignore.case = T)) %>% filter(!grepl('no etf', designated.broker, ignore.case = T)) %>% distinct()

ifs.etf <- ifs.etf %>% anti_join(perfect.fuzzy.join.ifs.etf.w.cusips, 
                                 by=c('fund.cusip','nrd.number', 'fund.name', 'fund.manager'))

ifs.etf$fund.name <- toupper(ifs.etf$fund.name )

fuzzy.join.ifs.etf.by.name <- stringdist_inner_join(ifs.etf, map.etfs, by = 'fund.name') %>%
  mutate(distance = 1 - RecordLinkage::levenshteinSim(fund.name.x, fund.name.y))  %>%
  arrange(distance) %>% select(RIC, sym, listing_mkt, fund.cusip.y, fund.name.x, year, nrd.number, fund.manager, fund.category, fund.class, fund.type, strategy, authorized.participants, designated.broker, prime.broker, distance) %>% rename(fund.cusip = fund.cusip.y, fund.name = fund.name.x) %>% filter(distance == 0) %>% distinct()

map.etfs <- map.etfs %>% anti_join(fuzzy.join.ifs.etf.by.name, 
                                   by=c("RIC", "sym", "listing_mkt", "fund.cusip"))

perfect.fuzzy.join.ifs.etf.w.cusips.n.names <- rbind(perfect.fuzzy.join.ifs.etf.w.cusips,fuzzy.join.ifs.etf.by.name) %>% distinct()



perfect.fuzzy.join.ifs.etf.w.cusips.n.names['number_of_ap'] <-  lengths(regmatches(perfect.fuzzy.join.ifs.etf.w.cusips.n.names$authorized.participants, gregexpr(",", perfect.fuzzy.join.ifs.etf.w.cusips.n.names$authorized.participants))) + 1 + lengths(regmatches(perfect.fuzzy.join.ifs.etf.w.cusips.n.names$authorized.participants, gregexpr(" and ", perfect.fuzzy.join.ifs.etf.w.cusips.n.names$authorized.participants)))

file4 <- 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\map.csv'

map <- read.csv(file4) %>% group_by(sym, listing_mkt, date) %>% distinct()

map.ifs <- merge(map, perfect.fuzzy.join.ifs.etf.w.cusips.n.names, by.x = c('sym', 'listing_mkt') , by.y = c('sym', 'listing_mkt'), all.x=T) %>% select(-year.y) 

#df_ref_map_ifs <- merge(x=df_ref_map_ifs, y=raw_ifs,  by.x=c("Fund.Name", 'year', "Fund.Type"), by.y =c("fund.name", 'year', 'fund.type'))

map.ifs <- map.ifs  %>%  group_by(sym, listing_mkt, date) %>% distinct(sym, listing_mkt, date, number_of_quotes, number_of_trades, .keep_all = TRUE) %>% mutate(year = format(as.Date(date, format="%Y-%m-%d"),"%Y"))

ifs.etf <- read.csv(file1) %>% filter(grepl('etf|mutual|money', fund.type, 
                                            ignore.case = T)) %>% filter(!grepl('no etf', designated.broker, ignore.case = T)) %>% distinct() 

map.ifs <- map.ifs %>% rename(map.year = year.x, map.closing_price = closing_price, map.number_of_trades = number_of_trades, ifs.year = year) 

#map.ifs.final <- merge(map.ifs, ifs.etf, by.x = c('nrd.number', 'fund.name', 'fund.manager', 'authorized.participants'), by.y = c('nrd.number', 'fund.name', 'fund.manager', 'authorized.participants')) %>% filter(map.year != 2019)

#map.ifs.final.tableau <- map.ifs.final[c(5:41, 1:4, 42:52, 67)] %>% ungroup() %>% group_by(sym, listing_mkt, cusip, date, number_of_quotes, closing_price, number_of_trades, fund.name, fund.manager, nrd.number) %>% distinct(sym, listing_mkt, cusip, date, number_of_quotes, closing_price, number_of_trades, fund.name, fund.manager, nrd.number, .keep_all = TRUE) %>% rename(fund.cusip = fund.cusip.x, fund.category = fund.category.x,  fund.class = fund.class.x,  fund.type =  fund.type.x, strategy = strategy.x, designated.broker = designated.broker.x, prime.broker = prime.broker.x)


map.ifs <- map.ifs %>% ungroup() %>% distinct(sym, listing_mkt, cusip, date, number_of_quotes, map.closing_price, map.number_of_trades, fund.name, fund.manager, nrd.number, number_of_ap, .keep_all = TRUE) 


### check if number of ap change by year, also check for consistency between fund.names going back


#map.ifs <- read.csv("T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\map+ifs.csv") %>% 

#map.ifs.final.tableau

#map.refinitiv <-  read.csv('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\map+refinitiv.csv')

refinitiv <- read.csv('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\Tertiary Sources\\refinitiv_full.csv') 


refinitiv_list <- read.csv('C:\\Users\\MjaenCortes\\Desktop\\ETF Data\\etfs_new.csv') %>% filter(listing_mkt != 'NA')

tta.list <- read.csv('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\TTa\\DS_WS_list_etf_ca.csv') %>% select(-X) %>% mutate(Start.Date = as.Date(Start.Date)) %>% filter(Start.Date <= as.Date('2023-01-01'))

tta.list.delete <- read.csv('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\TTa\\DS_WS_list_etf_ca.csv') %>% select(-X) %>% mutate(Start.Date = as.Date(Start.Date)) %>% filter(Start.Date > as.Date('2022-12-31'))

refinitiv_list <- merge(refinitiv_list, tta.list, by.x = "RIC.new", by.y = 'RIC', all.x=T)

refinitiv.merged <- merge(refinitiv, refinitiv_list, by.x = "Instrument", by.y = 'B', all.x=T) %>% filter(sym != 'NA') %>% mutate(Date = as.Date(Date))

refinitiv.merged.trouble.ric <- refinitiv.merged %>% distinct(sym, listing_mkt, Instrument) %>% group_by(Instrument) %>% filter(n() > 1) %>% filter(sym %in% c("HAU","HAU.U","HBG.U","HBG","HFMU","HFMU.U","HFY.U", "HFY" ,"HYBR","HFP","DWG","MOM"))


map <-  read.csv('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\map.csv') %>% group_by(sym, listing_mkt, date) %>% distinct() %>%  ungroup() %>% mutate(date = as.Date(date))

map.refinitiv <- merge(map, refinitiv.merged, by.x = c("sym", 'listing_mkt', 'date'), by.y = c("sym", 'listing_mkt', 'Date'), all.x=T)

map.refinitiv <- map.refinitiv %>% ungroup() %>% mutate(diff.price = abs(Price.Close - closing_price)/closing_price) %>% group_by(sym, listing_mkt, date, cusip, number_of_trades, number_of_quotes, avg_lag_bw_trade_and_NBBO_quote)  %>% slice(which.min(diff.price)) %>% select(-diff.price) %>% ungroup()

map.refinitiv <- map.refinitiv %>% rename(refinitiv.date = date, refinitiv.closing_price = Price.Close, refinitiv.Net.Asset.Value = Net.Asset.Value, refinitiv.NAV = NAV, 
                                          refinitiv.volume = Volume, refinitiv.market.cap = Company.Market.Cap, refinitiv.issue.market.cap = Issue.Market.Cap, 
                                          refinitiv.outstanding.shares = Outstanding.Shares, refinitiv.company.shares = Company.Shares,  refinitiv.company.common.name = Company.Common.Name, 
                                          refinitiv.fund.cusip = fund.cusip, refinitiv.ric = Instrument, refinitiv.cusip = CUSIP, refinitiv.fund.name = Fund.Name,
                                          refinitiv.fund.type = Fund.Type, refinitiv.fund.type1 = Fund.Type.1,  refinitiv.fund.company = Fund.Company,
                                          refinitiv.fund.city = Fund.City, refinitiv.fund.country = Fund.County, refinitiv.fund.perid = Fund.PermID,
                                          refinitiv.Net.Asset.Value.Per.Share...Actual = Net.Asset.Value.Per.Share...Actual, refinitiv.Share.Class = Share.Class,
                                          refinitiv.Launch.Date = Launch.Date, refinitiv.Domicile = Domicile.x, refinitiv.Classification.Sector.Name = Classification.Sector.Name,
                                          refinitiv.Price...NAV...Actual = Price...NAV...Actual, refinitiv.Company.Shares.1 = Company.Shares.1, refinitiv.Instrument.Shares = Instrument.Shares,
                                          refinitiv.Company.Market.Capitalization.Local.Currency = Company.Market.Capitalization.Local.Currency, refinitiv.INSTRUMENTMARKETCAPITALIZATIONLOCALCURN = INSTRUMENTMARKETCAPITALIZATIONLOCALCURN,
                                          refinitiv.Issue.Default.Shares.Outstanding = Issue.Default.Shares.Outstanding, refinitiv.Number.of.Shares = Number.of.Shares,
                                          refinitiv.Total.Number.of.Shares = Total.Number.of.Shares, refinitiv.Name_DS = Name_DS, refinitiv.Start.Date = Start.Date,
                                          refinitiv.Currency = Currency, refinitiv.Full.Name = Full.Name, refinitiv.Activity = Activity, refinitiv.Name_WS = Name_WS,
                                          refinitiv.ISIN = ISIN)  %>% select(-RIC.new, -RIC.old, -fund.name.new, -fund.name.old, -Symbol, -Domicile.y, -Exchange_Symbol, -Exchange, -Hist.)

map.refinitiv$refinitiv.closing_price   <- as.numeric(as.character(map.refinitiv$refinitiv.closing_price)) 

map.refinitiv.check <- map.refinitiv %>% group_by(sym, listing_mkt)  %>% drop_na(refinitiv.closing_price) %>%
  select(sym, listing_mkt, closing_price, refinitiv.closing_price) %>% summarise(refinitiv.closing_price = mean(refinitiv.closing_price), closing_price = mean(closing_price)) %>% 
    mutate(error = 100* (abs(refinitiv.closing_price - closing_price)/closing_price)) %>% filter(error > 1)  %>% select(sym, listing_mkt, error)

map.refinitiv <- map.refinitiv %>% anti_join(map.refinitiv.check, by=c("sym", "listing_mkt")) 

map.ifs.refinitiv <- merge(map.ifs, map.refinitiv[c(1:4, 38:84)], by.x = c('sym', 'listing_mkt', 'date', 'cusip'), by.y = c('sym', 'listing_mkt', 'refinitiv.date', 'cusip'), all.x=T) 


#check <- map.ifs.refinitiv.final %>% mutate(year = format(as.Date(date, format="%Y-%m-%d"),"%Y")) %>%filter(year == 2022)  %>% ungroup() %>% distinct(sym, listing_mkt)

bloomberg <- read.csv('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\Tertiary Sources\\bloomberg_data.csv') %>% group_by(X, Dates) %>% select(-X.NAME.) %>% rename(sym = X, variable = Dates) %>% distinct()

library(dplyr)
library(tidyr)


bloomberg.longer <-pivot_longer(bloomberg, cols=c(-1,-2), names_to = c("date")) %>% filter(value != "#N/A N/A") 

bloomberg.longer$date  <- str_remove(bloomberg.longer$date, 'X')  
bloomberg.longer$date  <- as.Date(bloomberg.longer$date,format="%Y.%m.%d")  

bloomberg.longer$sym <- gsub( " .*$", "", bloomberg.longer$sym) #1190

###
bloomberg.longer.sym <- bloomberg.longer %>% ungroup() %>%  distinct(sym)
securities <- map.ifs.refinitiv %>% distinct(sym, listing_mkt)

fuzzy.join.test <- stringdist_inner_join(securities, bloomberg.longer.sym, by = 'sym') %>%
  mutate(distance = 1 - RecordLinkage::levenshteinSim(sym.x, sym.y))

fuzzy.join.test.p <- fuzzy.join.test %>% filter(distance == 0)

fuzzy.join.test <- fuzzy.join.test  %>% anti_join(fuzzy.join.test.p,by= c("sym.x", "listing_mkt", "sym.y")) %>% filter(!(sym.y %in% unique(fuzzy.join.test.p$sym.y))) %>% filter(distance <= 0.2000000)

fuzzy.join.test.p <- rbind(fuzzy.join.test.p, fuzzy.join.test) %>% select(-distance)

bloomberg.longer <- merge(bloomberg.longer, fuzzy.join.test.p, by.x = c('sym'), by.y = c('sym.y')) %>% rename(sym.bloomberg = sym ,sym = sym.x) %>% select(sym, listing_mkt, date, variable, value) %>% pivot_wider(names_from = variable, values_from = value)


bloomberg.longer <- bloomberg.longer %>% rename(bloomberg.closing_price = PX_LAST, bloomberg.NAV = FUND_NET_ASSET_VAL, 
                                          bloomberg.volume = PX_VOLUME, bloomberg.market.cap = CUR_MKT_CAP, bloomberg.nav.per.share = BS_NET_ASSET_VALUE_PER_SHARE, 
                                          bloomberg.outstanding.shares = BS_SH_OUT, bloomberg.hist.market.cap = HISTORICAL_MARKET_CAP, bloomberg.num.trades = NUM_TRADES)

unique(bloomberg.longer$date) %in% unique(map.ifs.refinitiv$date)

bloomberg.longer$bloomberg.closing_price   <- as.numeric(as.character(bloomberg.longer$bloomberg.closing_price)) 
bloomberg.longer$bloomberg.NAV   <- as.numeric(as.character(bloomberg.longer$bloomberg.NAV)) 
bloomberg.longer$bloomberg.volume   <- as.numeric(as.character(bloomberg.longer$bloomberg.volume)) 
bloomberg.longer$bloomberg.market.cap   <- as.numeric(as.character(bloomberg.longer$bloomberg.market.cap)) 


bloomberg.longer$bloomberg.num.trades   <- as.numeric(as.character(bloomberg.longer$bloomberg.num.trades)) 
bloomberg.longer$bloomberg.nav.per.share   <- as.numeric(as.character(bloomberg.longer$bloomberg.nav.per.share)) 
bloomberg.longer$bloomberg.hist.market.cap   <- as.numeric(as.character(bloomberg.longer$bloomberg.hist.market.cap)) 
bloomberg.longer$bloomberg.outstanding.shares   <- as.numeric(as.character(bloomberg.longer$bloomberg.outstanding.shares))

map.ifs.refinitiv.bloomberg <- merge(map.ifs.refinitiv, bloomberg.longer, by = c('sym', 'listing_mkt', 'date'), all.x=T)

bloomberg.longer$date  <- as.Date(bloomberg.longer$date) 
map.ifs.refinitiv$date  <- as.Date(map.ifs.refinitiv$date) 

map.ifs.refinitiv.bloomberg <- merge(map.ifs.refinitiv, bloomberg.longer, by = c('sym', 'listing_mkt', 'date'), all.x=T)

map.ifs.refinitiv.bloomberg <- map.ifs.refinitiv.bloomberg %>% ungroup() %>% group_by(sym, listing_mkt, date) %>% select(-distance) 

write.csv(map.ifs.refinitiv.bloomberg, 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\full_dataset1.csv', row.names = FALSE)

map.ifs.refinitiv.final <- map.ifs.refinitiv.final %>% anti_join(map.ifs.refinitiv.check, by=c("sym", "listing_mkt"))

year <- map.ifs.refinitiv.bloomberg %>% filter(map.year == 2020) %>% ungroup() %>% distinct(sym, listing_mkt) #760 (Bloomberg)
year <- map.ifs.refinitiv.bloomberg %>% filter(map.year == 2021) %>% ungroup() %>% distinct(sym, listing_mkt) #845 (Bloomberg)
year <- map.ifs.refinitiv.bloomberg %>% filter(year == 2022) %>% ungroup() %>% distinct(sym, listing_mkt) #890 (Bloomberg)

year <- bloomberg.refinitiv.manual.concat %>% filter(year == 2022) %>% ungroup() %>% distinct(sym, listing_mkt) 
