######
#Helper Functions for ETF Selection 
#& Variable Selection
######


# Load required libraries
pacman::p_load(dplyr, janitor, tidyr, tidyverse, fuzzyjoin, stringr, zoo, data.table, 
               fastDummies, corrr, arm, mclust, lmtest, leaps, fmsb, plm, broom, 
               knitr, performance, datawizard, lfe, lme4, parameters, MuMIn)

#List with file paths
#If you want to webscrape, instead of using disk, refer to python data_collection realized volatility scripts
file_paths <- list(
  move = 'path\to\move',
  vix = 'path\to\vix',
  corra = 'path\to\corra',
  tsx_returns = 'path\to\tsx_returns',
  treasury_yields = 'path\to\treasury_yields',
  full_data = 'path\to\fulldata'
)

# Read data
read_data <- function(file_path) {
  fread(file_path, encoding = "UTF-8")
}

# Filter data
filtered_data <- function(data, multi_series, currency, mkt.cap.cutoff) {
  data %>%
    mutate(
      Net.Asset.Value.refinitiv = as.numeric(Net.Asset.Value.refinitiv),
      fundata_nav = coalesce(fundata_nav, Net.Asset.Value.refinitiv),
      mt = coalesce(mt, closing_price),
      closing_price = coalesce(closing_price, mt),
      issue.default.shares.outstanding = Outstanding.Shares.refinitiv
    ) %>%
    drop_na(pqs) %>%
    filter(
      year != 2023,
      as.Date(date) >= as.Date('2019-06-30'),
      !as.character(date) %in% c('2022-07-15', '2022-04-07', '2021-11-12', '2022-04-08', '2020-02-27', '2020-07-27', '2022-08-23')
    ) %>% group_by(sym, listing_mkt, year) %>% filter(n()>234) %>% ungroup() %>%
    distinct(sym, listing_mkt, FundId, ISIN, date, .keep_all = TRUE) %>%
    {if(currency == 'CAD') filter(., !Currency == "US Dollar") else filter(., Currency == "US Dollar")} %>%
    {if(multi_series == 'N') 
      group_by(., sym, listing_mkt, FundId) %>%
        mutate(
          dummy.date = as.Date(dmy('31122022')),
          age = as.double(difftime(dummy.date, `Inception Date`, units = "days"))
        ) %>%
        filter(!is.na(age), age >= 365) %>%
        slice_max(age) %>%
        ungroup() %>%
        dplyr::select(-dummy.date, -age)
      else 
        group_by(., sym, listing_mkt, FundId) %>%
        filter(n_distinct(sym) > 1) %>%
        ungroup()
    } %>%
    group_by(sym, listing_mkt, FundId, year) %>%
    arrange(date) %>%
    mutate(Issue.Market.Cap = last(Issue.Market.Cap.refinitiv)) %>%
    filter(Issue.Market.Cap >= mkt.cap.cutoff) %>%
    ungroup() %>%
    distinct(sym, listing_mkt, FundId)
}

# Control variables
control_variables <- function(df) {
  daily_controls <- df %>%
    mutate(
      turnover = total_volume / issue.default.shares.outstanding,
      number_of_trades = log(number_of_trades),
      per_short_volume = short_volume / total_volume,
      avg_value = log(avg_value),
      hhi_volume = log(hhi_volume)
    ) %>%
    dplyr::select(sym, listing_mkt, FundId, date, year, avg_value, hhi_volume, 
           volatility_cv, turnover, number_of_trades, per_short_volume) %>%
    filter(year != 2019)
  
  yearly_controls <- df %>% 
    group_by(sym, listing_mkt, FundId, year) %>% 
    filter(!`Inception Date` %in% "" | is.na(`Inception Date`) == F) %>% 
    mutate(dummy.date = as.Date(dmy(paste0('3112',year)))) %>% 
    mutate(age = log(as.double(difftime(dummy.date, `Inception Date`, units ="days")))) %>% 
    ungroup() %>% dplyr::select(-dummy.date)
  
  yearly_controls <- yearly_controls %>% 
    group_by(sym, listing_mkt, FundId, year) %>% 
    arrange(date) %>% 
    mutate(size = log(last(Issue.Market.Cap.refinitiv)), MER = last(MER))
  
  ADV <- yearly_controls %>%
    group_by(sym, listing_mkt, FundId, year) %>%
    summarise(ADV = log(mean(total_volume, na.rm = TRUE)), .groups = "drop") %>%
    mutate(year = as.integer(year) + 1)
  
  yearly_controls %>%
    dplyr::distinct(sym, listing_mkt, FundId, year, MER, size, age) %>%
    mutate(year = as.integer(year) + 1) %>%
    left_join(ADV, by = c('sym', 'listing_mkt', 'FundId', 'year')) %>%
    distinct() %>%
    filter(year != 2023, year != 2024) %>%
    drop_na() %>%
    left_join(daily_controls, by = c('sym', 'listing_mkt', 'FundId', 'year')) %>%
    distinct()
}

# Market controls
market_controls <- function() {
  read_and_process <- function(file, cols, date_format) {
    fread(file) %>%
      dplyr::select(all_of(cols)) %>% rename_with(~"date", .cols = matches("^(date|DATE|Date)")) %>%
      mutate(date = as.Date(date, format = date_format)) %>%
      filter((date >= as.Date('2020-01-01')) & (date <= as.Date('2022-12-31')))
  }
  
  vix <- read_and_process(file_paths$vix, c("CLOSE", "DATE"), "%m/%d/%Y") %>%
    rename(vix = CLOSE)
  
  corra <- read_and_process(file_paths$corra, c("AVG.INTWO", "date")) %>%
    rename(d.corra = AVG.INTWO)
  
  treasury_yields <- read_and_process(file_paths$treasury_yields, 
                                      c("Date", "10.year.yield", "1.year.yield")) %>%
    filter(`10.year.yield` != "Bank holiday") %>%
    mutate(d.treasury_yield_spread = as.double(`10.year.yield`) - as.double(`1.year.yield`)) %>%
    dplyr::select(date, d.treasury_yield_spread)
  
  tsx_index <- read_and_process(file_paths$tsx_returns, c("date", "Open", "Adj.Close")) %>%
    mutate(tsx_return = (Adj.Close / Open) - 1) %>%
    dplyr::select(date, tsx_return)
  
  move <- read_and_process(file_paths$move, c("Date", "Adj Close")) %>%
    rename(Adj.Close = `Adj Close`)
  
  Reduce(function(...) merge(..., by = 'date', all = TRUE),
         list(treasury_yields, corra, vix, tsx_index, move)) %>%
    mutate(move_by_vix = as.numeric(Adj.Close) / vix) %>%
    dplyr::select(-Adj.Close)
}

# Final dataset for Regression
final_dataset <- function() {
  etfs <- filtered_data(read_data(file_paths$full_data), 'N', 'CAD', 10000000)
  print('***Task completed: Get List of ETFs***')
  df <- read_data(file_paths$full_data) %>% merge(., etfs, by=c('sym', 'listing_mkt', 'FundId'))
  fund_controls <- control_variables(df) %>% mutate(date = as.Date(date)) 
  print('***Task completed: Get Fund Controls***')
  mkt_controls <- market_controls() 
  print('***Task completed: Get Market Controls***')
  df %>% mutate(date = as.Date(date)) %>%
    dplyr::select(sym, listing_mkt, FundId, ISIN, date, year, pqs, pes_k, total_depth, 
           prs_k_1m:ppi_k_15m, mt, Net.Asset.Value.refinitiv, fundata_nav, CIFSC.CATEGORY, 
           number_of_ap.ifs, issue.default.shares.outstanding, closing_price, 
           daily.disclosure.ifs) %>%
    group_by(sym, listing_mkt) %>%
    arrange(date) %>%
    mutate(
      total_depth = log(total_depth),
      prem.disc.nav = ifelse((abs(mt-fundata_nav) < 1.35*abs(closing_price-fundata_nav)),
                             (mt/fundata_nav) - 1, (closing_price/fundata_nav) - 1),
      f.DoD.Outstanding.Shares = dplyr::lead(issue.default.shares.outstanding, 1) / 
        issue.default.shares.outstanding - 1
    ) %>%
    dplyr::select(-Net.Asset.Value.refinitiv, -mt, -closing_price, -fundata_nav) %>%
    ungroup() %>%
    left_join(mkt_controls, by = "date") %>%
    left_join(fund_controls, by = c("sym", "listing_mkt", "FundId", "year", "date")) %>%
    distinct()
}


# Main execution
print('***starting data processing***')
df <- final_dataset() %>% 
  drop_na(vix) %>%
  group_by(FundId, year) %>%
  filter(str_length(sym) == min(str_length(sym))) %>%
  filter(!all(is.na(.))) %>%
  ungroup() %>%
  group_by(FundId, year, sym) %>%
  mutate(pqs_sum = sum(pqs, na.rm = TRUE)) %>%
  ungroup() %>%
  group_by(FundId, year) %>%
  filter(pqs_sum == min(pqs_sum)) %>%
  ungroup() %>% dplyr::select(-pqs_sum) %>%
  filter((!number_of_ap.ifs %in% "" | !is.na(number_of_ap.ifs)),
         daily.disclosure.ifs %in% c('Yes', 'No')) %>% distinct() %>% drop_na() 
print("***Task Completed: Data Processing***") 
print(df)

#######HELPER FUNCTIONS for Data Inspection & Model Selection
#######

QR_decomposition <- function(df) {
  X <- as.matrix(df)
  qr.X <- qr(X, tol = 1e-9, LAPACK = FALSE)
  keep <- qr.X$pivot[seq_len(qr.X$rank)]
  as.data.frame(X[, keep])
}

model_selection <- function(f, data) {
  step(lm(f, data = data), direction = "both")
}

vif_func<-function(in_frame,thresh=10,trace=T,...){ #VIF> 10 are a sign for high, not tolerable correlation of model predictors (James et al.)
  #in_frame: df of predictors
  #tresh: threshold value to use for retaining variables
  #trace:  logical argument indicating if text output is returned as the stepwise selection progresses. 
  
  #The function calculates the VIF values for all explanatory variables, removes the variable with the highest value, 
  #and repeats until all VIF values are below the threshold. 
  
  if(any(!'data.frame' %in% class(in_frame))) in_frame<-data.frame(in_frame)
  
  #get initial vif value for all comparisons of variables
  vif_init<-NULL
  var_names <- names(in_frame)
  for(val in var_names){
    regressors <- var_names[-which(var_names == val)]
    form <- paste(regressors, collapse = '+')
    form_in <- formula(paste(val, '~', form))
    vif_init<-rbind(vif_init, c(val, VIF(lm(form_in, data = in_frame, ...))))
  }
  vif_max<-max(as.numeric(vif_init[,2]), na.rm = TRUE)
  
  if(vif_max < thresh){
    if(trace==T){ #print output of each iteration
      prmatrix(vif_init,collab=c('var','vif'),rowlab=rep('',nrow(vif_init)),quote=F)
      cat('\n')
      cat(paste('All variables have VIF < ', thresh,', max VIF ',round(vif_max,2), sep=''),'\n\n')
    }
    return(var_names)
  }
  else{
    
    in_dat<-in_frame
    
    #backwards selection of explanatory variables, stops when all VIF values are below 'thresh'
    while(vif_max >= thresh){
      
      vif_vals<-NULL
      var_names <- names(in_dat)
      
      for(val in var_names){
        regressors <- var_names[-which(var_names == val)]
        form <- paste(regressors, collapse = '+')
        form_in <- formula(paste(val, '~', form))
        vif_add<-VIF(lm(form_in, data = in_dat, ...))
        vif_vals<-rbind(vif_vals,c(val,vif_add))
      }
      max_row<-which(vif_vals[,2] == max(as.numeric(vif_vals[,2]), na.rm = TRUE))[1]
      
      vif_max<-as.numeric(vif_vals[max_row,2])
      
      if(vif_max<thresh) break
      
      if(trace==T){ #print output of each iteration
        prmatrix(vif_vals,collab=c('var','vif'),rowlab=rep('',nrow(vif_vals)),quote=F)
        cat('\n')
        cat('removed: ',vif_vals[max_row,1],vif_max,'\n\n')
        flush.console()
      }
      
      in_dat<-in_dat[,!names(in_dat) %in% vif_vals[max_row,1]]
      
    }
  }
    
    return(names(in_dat))
  }

remove_outliers <- function(df, std.away = 7) {
  df %>%
    dplyr::select(where(is.numeric)) %>%
    mutate(across(-1, ~replace(., abs(scale(.)) > std.away, NA)))
}

pairwise_corr <- function(df) {
  df %>%
    dplyr::select(where(is.numeric)) %>%
    cor() %>%
    as.data.frame() %>%
    mutate(var1 = rownames(.)) %>%
    pivot_longer(-var1, names_to = "var2", values_to = "value") %>%
    arrange(desc(value)) %>%
    group_by(value) %>%
    filter(row_number() == 1, value > 0.7, var1 != var2)
}

consecutive_deviations_from_nav <- function(data, refinitiv.data, output) {
  calc_prem_disc_nav <- if(refinitiv.data == "Y") {
    function(df) {
      df %>%
        mutate(
          a = ((coalesce(mt, closing_price) / coalesce(fundata_nav, Net.Asset.Value.refinitiv)) - 1) * 10000,
          b = ((coalesce(mt, closing_price) / coalesce(Net.Asset.Value.refinitiv, fundata_nav)) - 1) * 10000,
          prem.disc.nav = if_else(abs(a) >= abs(b), b, a)
        ) %>%
        dplyr::select(-a, -b)
    }
  } else {
    function(df) {
      df %>%
        mutate(prem.disc.nav = ((coalesce(mt, closing_price) / fundata_nav) - 1) * 10000)
    }
  }
  
  df1 <- data %>%
    calc_prem_disc_nav() %>%
    distinct(sym, listing_mkt, date, year, prem.disc.nav) %>%
    drop_na()
  
  summary_raw <- df1 %>%
    group_by(sym) %>%
    summarise(
      sd1plus = mean(prem.disc.nav) + sd(prem.disc.nav),
      sd2plus = mean(prem.disc.nav) + sd(prem.disc.nav) * 2,
      sd1minus = mean(prem.disc.nav) - sd(prem.disc.nav),
      sd2minus = mean(prem.disc.nav) - sd(prem.disc.nav) * 2,
      times_traded = n()
    ) %>%
    ungroup()
  
  summary_long <- df1 %>%
    left_join(summary_raw, by = 'sym') %>%
    distinct() %>%
    arrange(date) %>%
    mutate(id = as.numeric(factor(date)))
  
  filter_consecutive <- function(df, condition) {
    df %>%
      filter(condition) %>%
      group_by(sym) %>%
      arrange(date) %>%
      filter(with(rle((diff(id) == 1)), sum(lengths[values] >= 7)) >= 1) %>%
      mutate(consecutive = cumsum(c(1, diff(id) != 1))) %>%
      ungroup() %>%
      group_by(sym, consecutive) %>%
      filter(n() >= 7) %>%
      ungroup()
  }
  
  if(output == "sd") {
    filter_consecutive(summary_long, prem.disc.nav > sd2plus | prem.disc.nav < sd2minus)
  } else {
    filter_consecutive(summary_long, prem.disc.nav >= 200 | prem.disc.nav <= -200)
  }
}

###Check Staleness of datapoint via % Change Time-Series Graph


check_staleness <- function(df, var){
  if (!var %in% names(df)){
    print('****Error**** \ *****Input Variable not in df******') 
  } 
  #folder = './data/'
  #fshares_path <- fs::dir_ls(folder, glob='*.csv') # reading the share outstanding file,  fshares.csv 
  df_ShrOut <- df %>%
    arrange(sym, date) %>%
    rename(f_changeSO=var) %>%
    mutate(pct_f_changeSO = 100*f_changeSO)
  
  df_SO_sum <- df_ShrOut %>%
    group_by(sym) %>%
    summarise(nobs=n(),
              n0 = sum(pct_f_changeSO==0, na.rm=T),
              no_unique_val = n_distinct(pct_f_changeSO),
              pct_n0 = 100*n0/nobs,
              min=min(pct_f_changeSO, na.rm = T), 
              avg=mean(pct_f_changeSO, na.rm=T),
              median = median(pct_f_changeSO, na.rm=T),
              Q1 = quantile(pct_f_changeSO, 0.25, na.rm=T),
              Q3 = quantile(pct_f_changeSO, 0.75, na.rm=T),
              D1 = quantile(pct_f_changeSO, 0.10, na.rm=T),
              D9 = quantile(pct_f_changeSO, 0.9, na.rm=T),
              max = max(pct_f_changeSO, na.rm = T),
              sigma= sd(pct_f_changeSO, na.rm=T))
  
  df_ShrOut %>%
    ungroup() %>%
    mutate(date=as.Date(date)) %>%
    group_by(date) %>%
    summarise(avg = mean(pct_f_changeSO, na.rm=T)) %>%
    ggplot(aes(x=date, y=avg))+
    geom_line()
  
  #hist(df_SO_sum$avg, breaks = 30)
}
