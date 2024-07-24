library(tidyverse)
library(fuzzyjoin)
library(data.table)
library(qdap)
library(zoo)

# Helper functions
read_data <- function(file_path) {
  fread(file_path, encoding = "UTF-8")
}

clean_string <- function(x) {
  gsub("[[:space:]]", "", x)
}

process_map_data <- function(map) {
  map %>%
    distinct(sym, listing_mkt, date, cusip, .keep_all = TRUE) %>%
    group_by(sym, listing_mkt) %>%
    arrange(date) %>%
    mutate(cusip = ifelse(cusip == "", tail(cusip, 1), cusip)) %>%
    filter(cusip != "") %>%
    mutate(date = as.Date(date))
}



process_refinitiv_data <- function(refinitiv_data, tta_list) {
  refinitiv_data %>%
    merge(tta_list, by.x = "Instrument", by.y = 'B', all.x = TRUE) %>%
    filter(sym != 'NA') %>%
    mutate(Date = as.Date(Date))
}




merge_map_join_key <- function(map, join_key) {
  merge(map, join_key, 
        by.x = c("sym", "listing_mkt"), 
        by.y = c("sym", "listing_mkt"), 
        all.x = TRUE, 
        allow.cartesian = TRUE) %>%
    dplyr::select(-cusip.y) %>%
    rename(cusip = cusip.x) %>%
    distinct(sym, listing_mkt, cusip, date, number_of_quotes, closing_price, number_of_trades, fund.name, fund.manager, nrd.number, .keep_all = TRUE) %>%
    mutate(date = as.Date(date))
}

process_refinitiv_data <- function(refinitiv, join_key) {
  refinitiv_join_key <- join.key[,c('sym', 'listing_mkt', 'RIC')]
  
  merge(refinitiv_join_key, refinitiv, 
        by.x = "RIC", by.y = 'Instrument', 
        all.x = TRUE, allow.cartesian = TRUE) %>%
    filter(sym != 'NA') %>%
    mutate(date = as.Date(Date)) %>%
    dplyr::select(-Date) %>%
    distinct()
}

merge_map_refinitiv <- function(map_join_key, refinitiv_join_key, refinitiv_ts) {
  map_refinitiv <- merge(map_join_key, refinitiv_join_key, 
                         by.x = c("sym", 'listing_mkt', 'date'), 
                         by.y = c("sym", 'listing_mkt', 'date'), 
                         all.x = TRUE) %>%
    distinct() %>%
    rename(refinitiv.closing_price = Price.Close)
  
  merge(map_refinitiv, refinitiv_ts, 
        by.x = c("sym", 'listing_mkt', 'date'), 
        by.y = c("sym", 'listing_mkt', 'Date'), 
        all.x = TRUE) %>%
    distinct() %>%
    rename(RIC = RIC.x) %>%
    dplyr::select(-RIC.y)
}

check_refinitiv_data <- function(map_refinitiv) {
  map_refinitiv %>%
    group_by(sym, listing_mkt, RIC) %>%
    drop_na(refinitiv.closing_price) %>%
    dplyr::select(sym, listing_mkt, RIC, date, closing_price, refinitiv.closing_price) %>%
    mutate(date = as.Date(date)) %>%
    filter(date < as.Date('2021-12-10')) %>%
    summarise(refinitiv.closing_price = mean(refinitiv.closing_price), 
              closing_price = mean(closing_price)) %>%
    mutate(error = 100 * (abs(refinitiv.closing_price - closing_price) / closing_price)) %>%
    filter(error > 1) %>%
    dplyr::select(sym, listing_mkt, RIC, error)
}

process_ifs_data <- function(ifs) {
  ifs %>%
    mutate(
      authorized.participants = gsub("and |; |AND ", ",", authorized.participants),
      year = as.character(year)
    ) %>%
    filter(!authorized.participants %in% c("", "No", "0", "No ETF Series", "not applicable", "No ETF series", "none", "Not applicable"))
}

merge_map_refinitiv_ifs <- function(map_refinitiv, ifs) {
  map_refinitiv$year <- as.character(map_refinitiv$year)
  
  merge(map_refinitiv, ifs, 
        by.x = c("nrd.number", "fund.manager", "fund.name", "year"), 
        by.y = c("nrd.number", "fund.manager", "fund.name", "year"), 
        all.x = TRUE)
}

process_bloomberg_data <- function(bloomberg) {
  bloomberg %>%
    group_by(X, Dates) %>%
    dplyr::select(-X.NAME.) %>%
    rename(sym = X, variable = Dates) %>%
    distinct() %>%
    pivot_longer(cols = c(-1, -2), names_to = "date") %>%
    filter(value != "#N/A N/A") %>%
    mutate(
      date = as.Date(str_remove(date, 'X'), format = "%Y.%m.%d"),
      sym = gsub(" .*$", "", sym)
    )
}

fuzzy_join_bloomberg <- function(securities, bloomberg_longer_sym) {
  stringdist_inner_join(securities, bloomberg_longer_sym, by = 'sym') %>%
    mutate(distance = 1 - RecordLinkage::levenshteinSim(sym.x, sym.y)) %>%
    filter(distance <= 0.2) %>%
    dplyr::select(-distance)
}

log_message <- function(message) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat(paste0("[", timestamp, "] ", message, "\n"))
}


main_pipeline <- function(file_paths) {
  # Read data
  log_message("Collecting Data...")
  map <- read_data(file_paths$map) %>% process_map_data()
  join_key <- read_data(file_paths$join_key)
  refinitiv <- read_data(file_paths$refinitiv) %>%
    dplyr::select('Instrument', 'Date', 'Price Close') %>%
    rename('Price.Close' = 'Price Close')
  refinitiv_ts <- read_data(file_paths$refinitiv_ts)
  ifs <- read_data(file_paths$ifs) %>% process_ifs_data()
  bloomberg <- read.csv(file_paths$bloomberg)
  
  log_message("Processing Data...")
  # Process MAP data
  map_join_key <- merge_map_join_key(map, join_key)
  
  # Process Refinitiv data
  refinitiv_join_key <- process_refinitiv_data(refinitiv, join_key)
  refinitiv_ts <- merge(refinitiv_ts, join_key[, c(1:2, 6)], 
                        by.x = c('Instrument'), by.y = c('RIC'), 
                        all.x = TRUE, allow.cartesian = TRUE) %>%
    distinct() %>%
    dplyr::select(-Instrument)
  
  log_message("Merging Data...")
  # Merge MAP and Refinitiv data
  map_refinitiv <- merge_map_refinitiv(map_join_key, refinitiv_join_key, refinitiv_ts)
  
  # Check Refinitiv data
  map_refinitiv_check <- check_refinitiv_data(map_refinitiv)
  map_refinitiv <- map_refinitiv %>% 
    anti_join(map_refinitiv_check, by = c("sym", "listing_mkt"))
  
  # Merge with IFS data
  firstcol = which(colnames(ifs)=="exchange.traded")
  lastcol = which(colnames(ifs)=="daily.disclosure")
  
  map_refinitiv_ifs <- merge_map_refinitiv_ifs(map_refinitiv, ifs[,firstcol:lastcol])
  
  # Process Bloomberg data
  bloomberg_longer <- process_bloomberg_data(bloomberg)
  securities <- map %>% distinct(sym, listing_mkt)
  bloomberg_longer_sym <- bloomberg_longer %>% ungroup() %>% distinct(sym)
  
  fuzzy_join_test_p <- fuzzy_join_bloomberg(securities, bloomberg_longer_sym)
  
  bloomberg_longer <- merge(bloomberg_longer, fuzzy_join_test_p, 
                            by.x = c('sym'), by.y = c('sym.y')) %>%
    rename(sym.bloomberg = sym, sym = sym.x) %>%
    dplyr::select(sym, listing_mkt, date, variable, value) %>%
    pivot_wider(names_from = variable, values_from = value)
  
  log_message("Filtering Data...")
  # Final processing
  map_refinitiv_ifs_bloomberg <- merge(map_refinitiv_ifs, bloomberg_longer, 
                                       by = c('sym', 'listing_mkt', 'date'), 
                                       all.x = TRUE)
  
  map_refinitiv_ifs_bloomberg %>%
    filter(!sym %in% c("BKCC", "QQCC", "CNCC", "BBIG.U", "CGRN.U", "CHPS.U", "DAMG.U", "DANC.U", "EPCA.U", "EPGC.U", "EPZA.U", "FCGB.U", "FCIG.U", "FCIQ.U", "FCUL.U", "FCUQ.U", "FETH.U", "FLX.B", 
                       "HESG", "HUM.U", "HYLD.U", "SBT.U", "TUED.U", "USCC.U", "XFS.U", "ZPR.U",
                       "HISU.U", "RBOT.U", "HXDM.U", "HXT.U", "DLR.U", "ZUS.V", "HTB.U", "BPRF.U", "LIFE.U", "ZUP.U", "ZSML.U", 
                       "ZMID.U", "CALL.U", "FCRR.U", "BITI.U", "BTCQ.U", "FCUD.U", "ETHQ.U", "FBTC.U", "FCUV.U", "FCMO.U", "ETHY.U")) %>%
    mutate(year = format(as.Date(date, format = "%Y-%m-%d"), "%Y"),
           fund.type = case_when(
             fund.type %in% c("Alternative mutual fund", 'ETF, alternative mutual fund', 'Mutual fund', "Money market") & !fund.type %in% c("NA") ~ "Not",
             fund.type %in% c("ETF") ~ "ETF",
             TRUE ~ "Null"
           ))
}

# File paths
file_paths <- list(
  map = 'path/to/trading_etfs.csv',
  join_key = 'path/to/join.key.csv',
  refinitiv = 'path/to/refinitiv_full.csv',
  refinitiv_ts = 'path/to/refinitiv_ts.csv',
  ifs = 'path/to/ifs.csv',
  bloomberg = 'path/to/bloomberg_data.csv'
)

# Run pipeline
result <- main_pipeline(file_paths)

# Write result to CSV
#fwrite(result, 'path/to/output/full.data.final.csv', row.names = FALSE)
