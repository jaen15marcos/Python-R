
####################################
# Helper Functions 
##################################

#Function for data sample:

#Function filtered_data gets continuously trading FundIDs based on age from 2020-2022. 

#filtered_data(data = data, multi_series, currency, mkt.cap.cutoff)
#currency = 'CAD' or 'USD'
#mkt.cap.cutoff = int or float (e.g., 10000000)
#multi_series = 'Y' or 'N'


options(max.print=999999)
pacman::p_load(dplyr, janitor, tidyr, tidyverse, fuzzyjoin, stringr, zoo,data.table, fastDummies, corrr, arm, mclust, lmtest, leaps, fmsb, plm, broom, knitr, performance, datawizard, lfe, lme4, parameters, MuMIn) 

data <- fread('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\full.data.final.csv', encoding ="UTF-8")

filtered_data <- function(data = data, multi_series, currency, mkt.cap.cutoff){
     data$Net.Asset.Value <- as.numeric(data$Net.Asset.Value)
     data$fundata_nav <- ifelse(is.na(data$fundata_nav), data$Net.Asset.Value, data$fundata_nav)
     data$mt <- ifelse(is.na(data$mt), data$closing_price, data$mt)
     data$closing_price <- ifelse(is.na(data$closing_price), data$mt, data$closing_price)
     data$issue.default.shares.outstanding <- data$Outstanding.Shares.refinitiv
     df <- data %>% drop_na(pqs) %>% filter(!year == 2023, as.Date(date) >= as.Date('2019-06-30'), !as.character(date) %in% c('2022-07-15', '2022-04-07', '2021-11-12', '2022-04-08', '2020-02-27', '2020-07-27', '2022-08-23'))
  df <- df %>% filter(id == "Cont.Trading") %>% distinct(sym, listing_mkt, FundId, ISIN, date, .keep_all = T) #get funds that trade continuously each year  
 
  #df.2020 <- df %>%  filter(year == 2020) %>%  group_by(sym, listing_mkt, FundId, ISIN) %>% filter(n() >= 125) %>% ungroup() %>%  distinct()
  #df.2021  <- df %>%  filter(year == 2021) %>%  group_by(sym, listing_mkt, FundId, ISIN) %>% filter(n() >= 125) %>% ungroup() %>% distinct()
  #df.2022 <- df %>% filter(year == 2022) %>%  group_by(sym, listing_mkt, FundId, ISIN) %>% filter(n() >= 123) %>% ungroup() %>% distinct()
  #df <- rbind(df.2020, df.2021, df.2022) %>% distinct(sym, listing_mkt, FundId, ISIN, date, .keep_all = T) #get funds that trade continuously each year
    if (currency == 'CAD') {
  df <- df %>% group_by(sym, listing_mkt, FundId, Currency) %>% filter(!Currency == "US Dollar") %>% ungroup() #CAD currency
  } else {
  df <- df %>% group_by(sym, listing_mkt, FundId, Currency) %>% filter("US Dollar" %in% Currency) %>% ungroup() #US currency
  }
    if (multi_series == 'N') {
  df <- df %>% group_by(sym, listing_mkt, FundId) %>% mutate(dummy.date = as.Date(dmy('31122022'))) %>% mutate(age = as.double(difftime(dummy.date, `Inception Date`, units = c("days")))) %>% ungroup() %>% group_by(FundId, age, date) %>% arrange(age) %>% filter(is.na(age) == F, age >= 365) %>%filter(age == max(age)) %>% ungroup() %>% dplyr::select(-dummy.date, -age)#get oldest security in multiple series etf
  } else {
  df <- df %>% group_by(sym, listing_mkt, FundId) %>% filter(length(unique(sym)) > 1) %>% ungroup() #get multiple series funds
  }
  output <- df %>% distinct(sym, listing_mkt, FundId, date, year, Issue.Market.Cap.refinitiv) %>% group_by(sym, listing_mkt, FundId, year) %>% arrange(date) %>% mutate(Issue.Market.Cap = last(Issue.Market.Cap.refinitiv)) %>% dplyr::select(-date) %>% filter(Issue.Market.Cap >= mkt.cap.cutoff) %>% ungroup() %>% distinct(sym, listing_mkt, FundId)
  df <- merge(output, df, by = c('sym', 'listing_mkt', 'FundId'), all.x = T, allow.cartesian = T) %>% distinct() %>% filter(!year == 2023)
  #df <- df %>% group_by(sym, listing_mkt, FundId) %>% slice(which.max(rowSums(!is.na(df[,c(12:56,93:95)])))) %>% ungroup()
  return(df)
}
control_variables <- function(df){
  daily_fund_controls <- df %>% mutate(turnover = total_volume/issue.default.shares.outstanding, number_of_trades = log(number_of_trades), per_short_volume = short_volume/total_volume, avg_value = log(avg_value), hhi_volume = log(hhi_volume)) %>% dplyr::select(sym, listing_mkt, FundId, date, year, avg_value, hhi_volume,  volatility_cv, turnover, number_of_trades, per_short_volume) %>% filter(!year == 2019)
  #See Zurowska (2022) for hhi, probablu change volatility to ln() of the ratio of Intraday Hi/Low
  
  yearly_fund_controls <- data %>% group_by(sym, listing_mkt, FundId, year) %>% filter(!`Inception Date` %in% "" | is.na(`Inception Date`) == F) %>% mutate(dummy.date = as.Date(dmy(paste0('3112',year)))) %>% mutate(age = log(as.double(difftime(dummy.date, `Inception Date`, units = c("days"))))) %>% ungroup() %>% dplyr::select(-dummy.date)
  
  yearly_fund_controls <- yearly_fund_controls %>% group_by(sym, listing_mkt, FundId, year) %>% arrange(date) %>% mutate(size = log(last(Issue.Market.Cap.refinitiv)), MER = last(MER))
  
  ADV <- yearly_fund_controls %>% group_by(sym, listing_mkt, FundId, year) %>% summarise(ADV = log(mean(total_volume, na.rm = T)))
  ADV$year <- ADV$year + 1
  yearly_fund_controls$year <- yearly_fund_controls$year + 1
  yearly_fund_controls$year <- as.integer(yearly_fund_controls$year)
  yearly_fund_controls <- yearly_fund_controls %>% dplyr::select(sym, listing_mkt, FundId, date, year, MER, size, age) %>% merge(., ADV, by = c('sym', 'listing_mkt', 'FundId', 'year'), all.x = T, all.y = T, allow.cartesian = T) %>% distinct() %>% dplyr::select(-date) %>% filter(!year == 2023|!year == 2024) %>% distinct() %>% drop_na()
  
  fund.controls <- merge(daily_fund_controls, yearly_fund_controls, by = c('sym', 'listing_mkt', 'FundId', 'year'), allow.cartesian = T) %>% distinct()
  return(fund.controls)
}


market.controls  <- function(){
  vix_file = 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\Tertiary Sources\\VIX.csv'
  corra_file = 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\Tertiary Sources\\CORRA.CSV'
  treasury_yield_file = 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\Tertiary Sources\\Daily_Treasury_yield_spread.csv'
  tsx_index = 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\Tertiary Sources\\tsx_index.csv'
  move = 'T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\Tertiary Sources\\MOVE.csv'
  vix <- fread(vix_file) %>% dplyr::select(CLOSE, DATE) %>% rename("vix" = "CLOSE", 'date' = 'DATE') %>% mutate(date = format(as.Date(date, format='%m/%d/%Y'))) %>% filter(as.Date(date) >= as.Date('2019-01-01'), as.Date(date) <= as.Date('2022-12-31'))
corra <- fread(corra_file) %>% rename("d.corra" = "AVG.INTWO") %>% mutate(date = as.Date(date)) %>% filter(as.Date(date) >= as.Date('2019-01-01'), as.Date(date) <= as.Date('2022-12-31'))
corra$date <- as.Date(corra$date)
vix$date <- as.Date(vix$date)
treasury_yields <- fread(treasury_yield_file) %>% filter(!`10.year.yield` %in% "Bank holiday") %>% mutate(d.treasury_yield_spread = as.double(`10.year.yield`) - as.double(`1.year.yield`)) %>% rename("date" = "Date") %>% mutate(date = as.Date(date)) %>% filter(as.Date(date) >= as.Date('2019-01-01'), as.Date(date) <= as.Date('2022-12-31')) %>% dplyr::select(date, d.treasury_yield_spread) 
tsx_index <- fread(tsx_index) %>% dplyr::select(date, Open, Adj.Close) %>% mutate(date = as.Date(date), tsx_return = (Adj.Close/Open) - 1) %>% dplyr::select(date, tsx_return)
move <- fread(move) %>% dplyr::select(Date, `Adj Close`) %>% mutate(Date = as.Date(Date)) %>% rename("date" = "Date", "Adj.Close" = "Adj Close")
mkt.controls <- purrr::reduce(list(treasury_yields,corra,vix, tsx_index, move)
                              ,dplyr::left_join, by = 'date')
mkt.controls <- mkt.controls %>% mutate(move_by_vix = as.numeric(Adj.Close)/vix) %>% dplyr::select(-Adj.Close)
return(mkt.controls)
}
#condition = 'FY' or 'HY'
final.dataset <- function(){
  df <- filtered_data(data, 'N', 'CAD', 10000000)
  fund.controls <- control_variables(df)
  mkt.controls <- market.controls()
  #add daily.disclosure, authorized.participants, fund.type, 
  df <- df %>% dplyr::select(sym, listing_mkt, FundId, ISIN, date, year, pqs, pes_k, total_depth, prs_k_1m, prs_k_10m, prs_k_5m, prs_k_15m, ppi_k_5m, ppi_k_1m, ppi_k_10m, ppi_k_15m, mt, Net.Asset.Value, fundata_nav, CIFSC.CATEGORY,number_of_ap.ifs, issue.default.shares.outstanding, closing_price, daily.disclosure.ifs) %>% group_by(sym, listing_mkt) %>% arrange(date) %>%mutate(total_depth = log(total_depth), prem.disc.nav = ifelse((abs(mt-fundata_nav)<1.35*abs(closing_price-fundata_nav)),(mt/fundata_nav) - 1,(closing_price/fundata_nav) - 1 ),f.DoD.Outstanding.Shares = dplyr::lead(issue.default.shares.outstanding,1)/issue.default.shares.outstanding -1) %>% dplyr::select(-Net.Asset.Value, -mt, -closing_price, -fundata_nav) %>% ungroup()
  df <- merge(df, fund.controls, by = c("sym","listing_mkt","FundId","year","date")) %>% distinct()
  df <- merge(df, mkt.controls, by=c("date")) %>% distinct()
  return(df)
}

df <- final.dataset() %>% drop_na(vix)
df <- df %>% mutate(str_len = str_length(sym)) %>% group_by(FundId, year) %>% filter(str_length(sym) == min(str_len)) %>% filter(!all(is.na(.))) %>%   ungroup() %>% group_by(FundId, year, sym) %>% mutate(pqs_sum = sum(pqs, na.rm = T)) %>% ungroup() %>% group_by(FundId, year) %>% filter(pqs_sum == min(pqs_sum)) %>% ungroup() %>% dplyr::select(-str_len, -pqs_sum)

df <- df %>% filter((!number_of_ap.ifs %in% "" | is.na(number_of_ap.ifs) == F)) %>% filter(daily.disclosure.ifs %in% c('Yes', 'No')) %>% distinct()

years <- df %>% distinct(FundId, year) %>% group_by(year) %>% summarise(n())
dates <- df %>% distinct(FundId, date) %>% group_by(date) %>% summarise(n())


sym <- df %>% distinct(FundId, sym, year) %>% group_by(FundId, year) %>% summarise(n())

years <- df.50 %>% distinct(FundId, year) %>% group_by(year) %>% summarise(n())
dates <- df.50 %>% distinct(FundId, date) %>% group_by(date) %>% summarise(n())

summary.nas <- df.50 %>%   
  dplyr::select(sym, FundId, year, prem.disc.nav, forward.prem.disc.nav, f.DoD.Outstanding.Shares, Outstanding.Shares) %>%  # replace to your needs
  group_by(sym, FundId, year) %>%
  summarise_all(funs(sum(is.na(.))))

#check for unique fundID
#FSUSA0ANKR #CGL.C
#FSUSA0BAOP #ZST.L

#df <- df %>% filter(!sym %in% c('CGL.C', 'ZST.L')) #ZDJ causing perfect collinearity issues

#dfull.dates.year <- df.full %>% distinct(date, year) %>% group_by(year) %>% summarise(n())
#dfhalf.dates.year <- df.half %>% distinct(date, year) %>% group_by(year) %>% summarise(n())

#df.full.syms  <- df.full %>% distinct(FundId, sym) %>% group_by(FundId) %>% summarise(n())
#df.full.syms  <- df.half %>% distinct(FundId, sym) %>% group_by(FundId) %>% summarise(n())

QR.decomposition <- function(df){
  X <- as.matrix(df)
  #QR decomposition  to remover perfect collinearity
  qr.X <- qr(X, tol=1e-9, LAPACK = FALSE)
  (rnkX <- qr.X$rank)  ## 4 (number of non-collinear columns)
  (keep <- qr.X$pivot[seq_len(rnkX)])
  ## 1 2 4 5 
  X2 <- X[,keep]
  return(as.data.frame(X2))
}


model.selection <- function(f, data){
  # Initialize a model with all predictors
  both_model <- lm(f, data = data)
  # Both-direction stepwise regression
  both_model <- step(both_model, direction = "both")
  return(both_model)
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
    
    return(names(in_dat))
    
  }
  
}

remove.outliers<-function(df, std.away=7){
  dd <- df %>% dplyr::select(where(is.numeric))
  dd[,-1] <- lapply(dd[,-1],
                    function(x) replace(x,abs(scale(x))>std.away,NA)) #outliers and influential points 7 standard deviations away
  return(dd)
}


# Plot the Cook's Distance using the traditional 4/n criterion
cooksd <- cooks.distance(lm_regression)
sample_size <- nrow(df.filtered)
#plot(cooksd, pch="*", cex=2, main="Influential Obs by Cooks distance")  # plot cook's distance
#abline(h = 4/sample_size, col="blue")  # add cutoff line
#text(x=1:length(cooksd)+1, y=cooksd, labels=ifelse(cooksd>4/sample_size, names(cooksd),""), col="red")  # add labels
#We see the existence of many influential observations

# Removing Influential Observations
# influential row numbers
#influential <- as.numeric(names(cooksd)[(cooksd > (4/sample_size))])

#Alternatively, you can try to remove the top x outliers to have a look
top_x_outlier <- 2
influential <- as.numeric(names(sort(cooksd, decreasing = TRUE)[1:top_x_outlier]))

df.filtered_screen <- df.filtered[-influential, ]
df <- data %>% filter(is.na(closing_price) == F) %>% distinct(sym, date, closing_price) %>% group_by(sym) %>% arrange(date)  %>%
  fill(closing_price, .direction = "down")%>% ungroup() %>% pivot_wider(names_from = "sym", values_from = "closing_price") %>% mutate(year = format(as.Date(date, format='%Y.%m.%d'),"%Y")) %>% filter(year== 2019) %>% dplyr::select(-year)

pairwise.corr <- function(df){
  df.corr <- df %>% dplyr::select(where(is.numeric)) #%>% dplyr::select(-year)
  cor(df.corr) %>%
    as.data.frame() %>%
    mutate(var1 = rownames(.)) %>%
    gather(var2, value, -var1) %>%
    arrange(desc(value)) %>%
    group_by(value) %>%
    filter(row_number()==1) %>% filter(value > 0.7, !var1 == var2)
  return(cor(df.corr))
}

error_dates <- c()
results_df <- data.frame()
for(dates in unique(df.filtered$date)) {
  subset_df <- df.filtered %>% filter(dates == date)
  tryCatch({plm.nav <- plm(forward.prem.disc.nav ~ second.order + number_of_ap + prem.disc.nav + pqs + pes_k + 
                             hhi_volume + volatility_ratio_ln + turnover + MER + age +  
                             + size + per_short_volume + d.corra + vix + tsx_return + daily.disclosure  +covid + avg_volume, data=subset_df, 
                           index =c("MC", "date"), model="within", effect = "twoways")
  coeff <- plm.nav$coefficients
  variable <- names(plm.nav$coefficients)
  
  date_coeff <- data.frame(date = unique(subset_df$date), coeff, variable)
  results_df <- rbind(results_df, date_coeff)}, error = function(e) {
    error_dates <- c(error_dates, unique(subset_df$date))
  })
  
}

results <- data.frame(name = character(), year = character(), ar1_coefficient = numeric(), error_term = numeric(), stringsAsFactors = FALSE)
for (years in  unique(df$year)) {
  df.filtered <- df %>% filter(years==year)
  for(syms in unique(df.filtered$sym)) {
    subset_df <- df.filtered %>% filter(syms == sym)
    ts_data <- ts(subset_df$prem.disc.nav)
    
    arima_model <- arima(ts_data, order = c(1, 0, 0))
    
    ar1_coefficient <- arima_model$coef[1]
    
    error_terms <- residuals(arima_model)
    
    results <- rbind(results, data.frame(name = subset_df$FundId, year =  subset_df$year,ar1_coefficient = ar1_coefficient, error_term = mean(abs(error_terms))))
  }
}
results <- results %>% distinct()
fwrite(results, "C:\\Users\\MjaenCortes\\Downloads\\ar.csv", row.names = F)




results <- data.frame(name = character(), ar1_coefficient = numeric(), error_term = numeric(), stringsAsFactors = FALSE)

for (x in unique(df$FundId)) {
  subset_df <- df %>% filter(x == FundId)
  
  ts_data <- ts(subset_df$prem.disc.nav)
  
  arima_model <- arima(ts_data, order = c(1, 0, 0))
  
  ar1_coefficient <- arima_model$coef[1]
  
  error_terms <- residuals(arima_model)
  
  results <- rbind(results, data.frame(name = subset_df$FundId, ar1_coefficient = ar1_coefficient, error_term = mean(abs(error_terms))))
}



#data <- fread('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\full.data.final.csv', encoding ="UTF-8")
consecutive_deviations_from_nav <- function(data = data, refinitiv.data, output){
  #refinitiv.data:  "Y" or "N" to include (exclude) refinitiv nav data
  #output: "sd" or "200bps" for results to be 7 consecutive days with >2sd or 7 consecutive days with >200bps
  
  
  
  if(refinitiv.data == "Y") {
    df1 <- data %>% mutate(a = ((ifelse(is.na(mt), closing_price, mt)/ifelse(is.na(fundata_nav), Net.Asset.Value.refinitiv, fundata_nav)) - 1)*10000, b = ((ifelse(is.na(mt), closing_price, mt)/ifelse(is.na(Net.Asset.Value.refinitiv), fundata_nav, Net.Asset.Value.refinitiv)) - 1)*10000) %>% mutate(prem.disc.nav = ifelse(abs(a)>=abs(b), b, a)) %>% select(-a,-b) %>% distinct(sym, listing_mkt, date, year, prem.disc.nav) %>% drop_na()
  } else {
    df1 <- data %>% mutate(prem.disc.nav = ((ifelse(is.na(mt), closing_price, mt)/fundata_nav) - 1)*10000) %>% distinct(sym, listing_mkt, date, year, prem.disc.nav) %>% drop_na()
  }
  
  summary_raw <- df1 %>% group_by(sym) %>%
    summarise(sd1plus = mean(prem.disc.nav) + sd(prem.disc.nav),
              sd2plus = mean(prem.disc.nav) + sd(prem.disc.nav)*2,
              sd1minus = mean(prem.disc.nav) - sd(prem.disc.nav),
              sd2minus = mean(prem.disc.nav) - sd(prem.disc.nav)*2,
              times_traded = n()) %>% ungroup()
  
  summary_long <- merge(df1, summary_raw, by=c('sym')) %>% distinct() %>% arrange(date) %>% mutate(id = as.numeric(factor(date))) 
  
  summary_2sd <- summary_long %>% group_by(sym)  %>% filter(prem.disc.nav > sd2plus|prem.disc.nav < sd2minus)  %>% arrange(date) %>% filter(with(rle((diff(id) == 1)), sum(lengths[values] >= 7)) >= 1) %>% arrange(date) %>% mutate(consecutive = cumsum(c(1, diff(id) != 1))) %>% ungroup() %>% group_by(sym, consecutive) %>% filter(n() >= 7) %>% ungroup()
  
  summary_200bps <- summary_long %>% filter(prem.disc.nav >= 200|prem.disc.nav <= -200) %>% group_by(sym) %>% arrange(date) %>% filter(with(rle((diff(id) == 1)), sum(lengths[values] >= 7)) >= 1) %>% arrange(date) %>% mutate(consecutive = cumsum(c(1, diff(id) != 1))) %>% ungroup() %>% group_by(sym, consecutive) %>% filter(n() >= 7) %>% ungroup()
  
  if(output=="sd"){
    return(summary_2sd)
  } else {
    return(summary_200bps)
  }
}
summary_final <- consecutive_deviations_from_nav(data = data, refinitiv.data = "N", output = "sd") %>% merge(., join.key[,c(1,2,3,10, 13, 14,  31, 33:35, 38:39)], by=c('sym', 'listing_mkt'), all.x = T) %>% distinct() %>% mutate(flag = '2sd')

summary_final2 <- consecutive_deviations_from_nav(data = data, refinitiv.data = "N", output = "200bps") %>% merge(., join.key[,c(1,2,3,10, 13, 14,  31, 33:35, 38:39)], by=c('sym', 'listing_mkt'), all.x = T) %>% distinct() %>% mutate(flag = '200bps')

summary_final <- rbind(summary_final, summary_final2) %>% distinct()

