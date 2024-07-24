library(tidyverse)
library(fuzzyjoin)
library(RecordLinkage)
library(zoo)
library(data.table)

# Helper functions (including previously defined ones)
read_csv_file <- function(file_path) {
  fread(file_path, encoding = "UTF-8")
}

clean_string <- function(x) {
  gsub("[[:space:]]", "", x)
}

fuzzy_join_cusip <- function(df1, df2) {
  stringdist_inner_join(df1, df2, by = 'fund.cusip') %>%
    mutate(distance = 1 - levenshteinSim(fund.cusip.x, fund.cusip.y)) %>%
    arrange(distance)
}

process_fuzzy_join <- function(joined_df) {
  joined_df %>%
    filter(distance == 0) %>%
    select(RIC, sym, listing_mkt, fund.cusip.y, fund.name.x, year, nrd.number, 
           fund.manager, fund.category, fund.class, fund.type, strategy, 
           authorized.participants, designated.broker, prime.broker, distance) %>%
    rename(fund.cusip = fund.cusip.y, fund.name = fund.name.x) %>%
    distinct()
}

# New helper functions
process_map_data <- function(map_data) {
  map_data %>%
    group_by(sym, listing_mkt, date) %>%
    distinct() %>%
    ungroup() %>%
    mutate(date = as.Date(date))
}

process_refinitiv_data <- function(refinitiv_data, tta_list) {
  refinitiv_data %>%
    merge(tta_list, by.x = "Instrument", by.y = 'B', all.x = TRUE) %>%
    filter(sym != 'NA') %>%
    mutate(Date = as.Date(Date))
}

merge_map_refinitiv <- function(map_data, refinitiv_data) {
  merge(map_data, refinitiv_data, 
        by.x = c("sym", 'listing_mkt', 'date'), 
        by.y = c("sym", 'listing_mkt', 'Date'), 
        all.x = TRUE) %>%
    ungroup() %>%
    mutate(diff.price = abs(Price.Close - closing_price) / closing_price) %>%
    group_by(sym, listing_mkt, date, cusip, number_of_trades, number_of_quotes, avg_lag_bw_trade_and_NBBO_quote) %>%
    slice(which.min(diff.price)) %>%
    select(-diff.price) %>%
    ungroup()
}

process_bloomberg_data <- function(bloomberg_data) {
  bloomberg_data %>%
    group_by(X, Dates) %>%
    select(-X.NAME.) %>%
    rename(sym = X, variable = Dates) %>%
    distinct() %>%
    pivot_longer(cols = c(-1, -2), names_to = c("date")) %>%
    filter(value != "#N/A N/A") %>%
    mutate(
      date = as.Date(str_remove(date, 'X'), format = "%Y.%m.%d"),
      sym = gsub(" .*$", "", sym)
    )
}

fuzzy_join_bloomberg <- function(securities, bloomberg_data) {
  stringdist_inner_join(securities, bloomberg_data, by = 'sym') %>%
    mutate(distance = 1 - levenshteinSim(sym.x, sym.y)) %>%
    filter(distance <= 0.2) %>%
    select(-distance)
}

# Main data processing pipeline
main_pipeline <- function(file_paths) {
  # Process IFS data
  ifs_etf <- read_csv_file(file_paths$ifs) %>%
    filter(grepl('etf|mutual|money', fund.type, ignore.case = TRUE),
           !grepl('no etf', designated.broker, ignore.case = TRUE)) %>%
    distinct()
  
  map_etfs <- read_csv_file(file_paths$map)
  
  # Process IFS data with CUSIPs
  ifs_etf_w_cusips <- ifs_etf %>%
    drop_na(fund.cusip) %>%
    mutate(fund.cusip = strsplit(as.character(fund.cusip), ",")) %>%
    unnest(fund.cusip) %>%
    distinct()
  
  # Fuzzy join based on CUSIP
  fuzzy_join_result <- fuzzy_join_cusip(ifs_etf_w_cusips, map_etfs)
  perfect_fuzzy_join <- process_fuzzy_join(fuzzy_join_result)
  
  # Process remaining data
  map_etfs <- map_etfs %>% anti_join(perfect_fuzzy_join, by = c("RIC", "sym", "listing_mkt", "fund.cusip"))
  remaining_ifs_etf <- anti_join(ifs_etf_w_cusips, perfect_fuzzy_join, 
                                 by = c('fund.cusip', 'nrd.number', 'fund.name', 'fund.manager'))
  
  # Clean CUSIP data
  remaining_ifs_etf$fund.cusip <- clean_string(remaining_ifs_etf$fund.cusip)
  map_etfs$fund.cusip <- clean_string(map_etfs$fund.cusip)
  
  # Repeat fuzzy join process for remaining data
  remaining_fuzzy_join <- fuzzy_join_cusip(remaining_ifs_etf, map_etfs)
  perfect_remaining_fuzzy_join <- process_fuzzy_join(remaining_fuzzy_join)
  
  perfect_fuzzy_join <- bind_rows(perfect_fuzzy_join, perfect_remaining_fuzzy_join) %>% distinct()
  
  # Process manual input CUSIP data
  manual_input_ifs_cusips <- read_csv_file(file_paths$manual_cusips) %>%
    mutate(fund.cusip = clean_string(fund.cusip))
  
  manual_fuzzy_join <- fuzzy_join_cusip(manual_input_ifs_cusips, map_etfs) %>%
    filter(distance == 0) %>%
    distinct() %>%
    rename(fund.cusip = fund.cusip.y, fund.name = fund.name.x) %>%
    select(RIC, sym, listing_mkt, fund.cusip, fund.name, distance)
  
  manual_fuzzy_join <- merge(manual_fuzzy_join, ifs_etf, by = 'fund.name') %>%
    select(RIC, sym, listing_mkt, fund.cusip.x, fund.name, year, nrd.number, 
           fund.manager, fund.category, fund.class, fund.type, strategy, 
           authorized.participants, designated.broker, prime.broker, distance) %>%
    rename(fund.cusip = fund.cusip.x) %>%
    filter(distance == 0) %>%
    distinct()
  
  perfect_fuzzy_join <- bind_rows(perfect_fuzzy_join, manual_fuzzy_join) %>% distinct()
  
  # Final processing of IFS data
  perfect_fuzzy_join <- perfect_fuzzy_join %>%
    mutate(number_of_ap = str_count(authorized.participants, ",") + 
             str_count(authorized.participants, " and ") + 1)
  
  # Process MAP data
  map_data <- read_csv_file(file_paths$map) %>% process_map_data()
  
  # Process Refinitiv data
  refinitiv_data <- read_csv_file(file_paths$refinitiv)
  refinitiv_list <- read_csv_file(file_paths$refinitiv_list) %>% filter(listing_mkt != 'NA')
  tta_list <- read_csv_file(file_paths$tta) %>% 
    select(-X) %>% 
    mutate(Start.Date = as.Date(Start.Date)) %>% 
    filter(Start.Date <= as.Date('2023-01-01'))
  
  refinitiv_data_processed <- process_refinitiv_data(refinitiv_data, refinitiv_list)
  
  # Merge MAP and Refinitiv data
  map_refinitiv <- merge_map_refinitiv(map_data, refinitiv_data_processed)
  
  # Process Bloomberg data
  bloomberg_data <- read_csv_file(file_paths$bloomberg) %>% process_bloomberg_data()
  
  securities <- map_refinitiv %>% distinct(sym, listing_mkt)
  bloomberg_joined <- fuzzy_join_bloomberg(securities, bloomberg_data %>% distinct(sym))
  
  bloomberg_data_processed <- merge(bloomberg_data, bloomberg_joined, by.x = c('sym'), by.y = c('sym.y')) %>%
    rename(sym.bloomberg = sym, sym = sym.x) %>%
    select(sym, listing_mkt, date, variable, value) %>%
    pivot_wider(names_from = variable, values_from = value)
  
  # Final merge
  final_data <- map_refinitiv %>%
    left_join(bloomberg_data_processed, by = c('sym', 'listing_mkt', 'date'))
  
  final_data
}

# File paths
file_paths <- list(
  ifs = 'path/to/ifs.csv',
  map = 'path/to/trading_etfs.csv',
  manual_cusips = 'path/to/ifs_clean_cusips.csv',
  refinitiv = 'path/to/refinitiv_full.csv',
  refinitiv_list = 'path/to/etfs_new.csv',
  tta = 'path/to/DS_WS_list_etf_ca.csv',
  bloomberg = 'path/to/bloomberg_data.csv'
)

# Execute main pipeline
result <- main_pipeline(file_paths)

# Write result to CSV
fwrite(result, 'path/to/output/full_dataset.csv')
