options(max.print=999999)
pacman::p_load(dplyr, janitor, tidyr, tidyverse, fuzzyjoin, stringr, zoo,data.table, fastDummies, corrr, 
               arm, mclust, lmtest, leaps, fmsb, plm, broom, knitr, performance, datawizard, lfe, lme4, 
               parameters, MuMIn, lattice, ggeffects, sandwich, tsibble) 

#df.50 <- fread('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\cont.trading.yearly.50.csv', encoding ="UTF-8")

df.full <- fread('T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\\Research and Analysis Policy Projects\\CSA ETF Research\\MCortes\\MAP Data\\cont.trading.yearly.csv', encoding ="UTF-8")


df <- df.full %>% mutate(size = exp(size), age = log(age), prem.disc.nav = (prem.disc.nav)*10000, volatility_ratio_ln = volatility_ratio_ln*100, 
                         per_short_volume = 100*per_short_volume, avg_volume= log(avg_volume), turnover = turnover*100, vix = log(vix), tsx_return = tsx_return*100, 
                         d.corra =  d.corra*100, pqs=10000*pqs, pes_k=10000*pes_k, prs_k_5m=10000*prs_k_5m, ppi_k_5m=10000*ppi_k_5m) %>% mutate(size = log(size/1000000))

df <- df %>% mutate(year = as.character(year)) %>% group_by(FundId, year) %>% 
  mutate(across(c(where(is.numeric), -c(number_of_ap, date, num_active_50, n_aps_daily, active_aps , MER, size, age, ADV, d.corra, vix, tsx_return, nrd.number)), 
                ~ DescTools::Winsorize(.x, minval = NULL, maxval = NULL, probs = c(0.0005, 0.9995),na.rm = TRUE, type = 1))) %>% ungroup()

df <- df %>% group_by(year) %>% 
  mutate(across(c(c(d.corra, vix, tsx_return)), 
                ~ DescTools::Winsorize(.x, minval = NULL, maxval = NULL, probs = c(0.0005, 0.9995),na.rm = TRUE, type = 1))) %>% ungroup()


df <- df %>% rename("MC"= "Morningstar Category", "Category"="Global Broad Category Group") %>% group_by(FundId, year) %>% mutate(second.order = number_of_ap*(prem.disc.nav), forward.prem.disc.nav = (dplyr::lead(prem.disc.nav, 1)), prem.disc.nav = (prem.disc.nav)) %>% ungroup() %>% filter(!allocation.methodology %in% "", !FundId %in% "", is.na(date) == F, is.na(volatility_ratio_ln) == F) %>% 
  mutate(buckets.num.aps = ifelse(number_of_ap== 1, "1", ifelse(number_of_ap <7, "2-6", ifelse(number_of_ap <11, "7-10", "11+"))))
df$allocation.methodology <-  ifelse(df$allocation.methodology %in% c("Enhanced, options or leverage (index or rules-based)", "Index-tracking, replication", "Rules-based, non-index tracking"), "Passive", df$allocation.methodology)

df <- df %>% group_by(sym, listing_mkt, year) %>% arrange(date) %>% dplyr::mutate(f.shares.bloomberg = dplyr::lead(shares.bloomberg,1)) %>% ungroup() %>% filter(!as.character(date) %in% c('2020-12-31', '2020-12-30'))

df <- df %>% group_by(sym, listing_mkt, year) %>% arrange(date) %>% dplyr::mutate(f.DoD.Shares.Bloomberg = abs((f.shares.bloomberg/shares.bloomberg) - 1))

df$covid <- ifelse(as.character(df$date) %in% c("2020-02-20", "2020-02-21", "2020-02-24", "2020-02-25", "2020-02-26", "2020-02-28", "2020-03-02", "2020-03-03","2020-03-04","2020-03-05","2020-03-06","2020-03-09",
                                                "2020-03-10","2020-03-11","2020-03-12","2020-03-13","2020-03-16","2020-03-17","2020-03-18","2020-03-19", "2020-03-20", "2020-03-23", "2020-03-24", "2020-03-25",
                                                "2020-03-26", "2020-03-27", "2020-03-30", "2020-03-31", "2020-04-01", "2020-04-02","2020-04-03", "2020-04-06", "2020-04-07", "2020-04-08", "2020-04-09"), 1, 0)

df <- df %>% filter(!sym %in% c('EAGB', 'EARK', 'TOCA', 'TOCM', 'ZINN'))
# df %>% distinct(FundId, year) %>% group_by(year) %>% summarise(n())
# 
# 
# df <- df %>% group_by(FundId, year) %>% 
#   mutate(across(c(where(is.numeric), -c(covid, number_of_ap, full_active, MER, size, age, ADV,d.corra, vix, tsx_return)), 
#                 ~ ((.x - min(.x, na.rm = T)) / (max(.x, na.rm = T) - min(.x, na.rm = T))))) %>% ungroup()
# 
# df <- df %>% group_by(year) %>% mutate(across(c(c(d.corra, vix, tsx_return)), 
#                                               ~ ((.x - min(.x, na.rm = T)) / (max(.x, na.rm = T) - min(.x, na.rm = T))))) %>% ungroup()
# 
# 
# df <- df %>% mutate(across(c(c(number_of_ap, full_active, MER, size, age, ADV)), 
#                            ~ ((.x - min(.x, na.rm = T)) / (max(.x, na.rm = T) - min(.x, na.rm = T)))))
# 
# is.na(df) <- sapply(df, is.infinite)

###
#Checks
###
nobs_by_fund <- df %>%
  count(FundId, sym) %>% arrange(n)

length(nobs_by_fund$FundId); length(unique(nobs_by_fund$FundId))
length(nobs_by_fund$sym); length(unique(nobs_by_fund$sym))

sym_count <- df %>%
  ungroup() %>%
  group_by(FundId) %>%
  summarise(sym_count=n_distinct(sym)) %>%
  filter(sym_count>1)

df %>%
  filter(FundId %in% sym_count$FundId) %>%
  dplyr::select(FundId, sym) %>%
  distinct()

df %>%
  ungroup() %>%
  group_by(FundId) %>%
  summarise(n_distinct(sym), n_distinct(date))

df %>%
  distinct(MC)

df %>%
  summarise_if(is.character, ~length(unique(.)))

unique(df$daily.disclosure)

prem_checks <- df%>%
  ungroup()%>%
  group_by(sym, date) %>%
  dplyr::select(prem.disc.nav, forward.prem.disc.nav) %>%
  arrange(sym)  

result <- prem_checks %>%
  group_by(sym) %>%
  summarize(onlyNA = all(is.na(prem.disc.nav))) %>% filter(onlyNA == TRUE)

prem_checks %>%
  group_by(sym) %>%
  summarise(mean = mean(prem.disc.nav, na.rm=T)) %>%
  ggplot(aes(x=mean))+
  geom_histogram()



data_checks <- data %>% distinct(sym, closing_price, date, Net.Asset.Value, mt)

date_checks <- df %>%
  dplyr::select(date, covid, vix) %>%
  distinct()

df %>%
  group_by(sym) %>%
  summarise(
    n=n(),
    mean_qs = mean(pqs, na.rm=T),
    mean_es = mean(pes_k, na.rk=T),
    mean_dev = mean(prem.disc.nav, na.rm=T)
  ) ->df_sum

##########################
#Final Models
#PQS - FE
###########################
input.year = 2022
df.filtered <- df %>% filter(year==input.year, grepl("Canadian|US|North American|Canada|Floating|Miscellaneous|Passive Inverse/Leveraged", MC, ignore.case=F), nrd.number %in% c(550, 1850,  2130,  3090,  3900,  4850, 17240, 28980)) #%>% filter(allocation.methodology %in% "Discretionary, active") # %>% filter(Category %in% c("Equity","Fixed Income"))
#df.filtered.cont <- df %>% group_by(FundId)  %>% filter(n() > 692) %>%  ungroup() %>% filter(Category %in% c("Equity","Fixed Income"))
plm.pqs <- plm(pqs ~  number_of_ap + hhi_volume +prem.disc.nav + 
                 volatility_ratio_ln  + turnover+ MER + age + +size + per_short_volume + 
                 d.corra + vix + tsx_return + factor(daily.disclosure) + covid + allocation.methodology + avg_volume, data = df.filtered,
               index = c("MC"),  model = "within")

summary(plm.pqs)
coeftest(plm.pqs, vcovHC(plm.pqs, method = "arellano")) #HC3 AND Arellano yield same results
summary(fixef(plm.pqs))

pool.pqs <- plm(pqs ~ number_of_ap+ + prem.disc.nav + 
                  hhi_volume + volatility_ratio_ln + turnover + MER + age +  
                  + size + per_short_volume + d.corra + vix + tsx_return + daily.disclosure + allocation.methodology +covid + avg_volume, data=df, 
                index =c("MC"), model="pooling") #age is not significant
summary(pool.pqs)
coeftest(pool.pqs, vcovHC(pool.pqs, type = "HC3")) #HC3 AND Arellano yield same results

summary(fixef(plm.pqs))
summary(fixef(plm.pqs, effect = "time"))

pool.pqs$aliased
pool.pqs$vcov #No collinearity

#####
#Effective Spreads
#PES_K - Fixed Effects
#####
input.year = 2022
df.filtered <- df %>% filter(year==input.year, grepl("Canadian|US|North American|Canada|Floating|Miscellaneous|Passive Inverse/Leveraged", MC, ignore.case=F), nrd.number %in% c(550, 1850,  2130,  3090,  3900,  4850, 17240, 28980)) #%>% filter(allocation.methodology %in% "Discretionary, active")
#df.filtered.cont <- df %>% group_by(FundId)  %>% filter(n() > 692) %>%  ungroup()
#allocation.methodology
plm.pes_k <- plm(pes_k ~ number_of_ap  + prem.disc.nav + 
                   hhi_volume + volatility_ratio_ln + turnover + MER + age +  
                   + size + per_short_volume + d.corra + vix + tsx_return + daily.disclosure +covid + allocation.methodology + avg_volume, data=df.filtered, 
                 index =c("MC"), model="within") #effecttwoways
summary(plm.pes_k)
coeftest(plm.pes_k, vcovHC(plm.pes_k, method = "arellano")) 
summary(fixef(plm.pes_k))



pool.pes_k <- plm(pes_k ~  number_of_ap + prem.disc.nav + 
                    hhi_volume + volatility_ratio_ln + turnover + MER + age +  
                    + size + per_short_volume + d.corra + vix + tsx_return + daily.disclosure + allocation.methodology + avg_volume +covid, data=df, 
                  index =c("MC"), model="pooling") #age is not significant
summary(pool.pes_k )
coeftest(pool.pes_k, vcovHC(pool.pes_k, type = "HC3")) #HC3 AND Arellano yield same results

#summary(pool.pes_k)
pool.pes_k$aliased
pool.pes_k$vcov #No collinearity



#####
#Next Day Deviation from Nav
#####
input.year = 2022
df.filtered <- df %>% filter(FundId != "FS0000GA12", year==input.year, grepl("Canadian|US|North American|Canada|Floating|Miscellaneous|Passive Inverse/Leveraged", MC, ignore.case=F), nrd.number %in% c(550, 1850,  2130,  3090,  3900,  4850, 17240, 28980))  #%>% filter(allocation.methodology %in% "Discretionary, active")# %>% filter(Category %in% c("Equity","Fixed Income"))
#df.filtered.cont <- df %>% group_by(FundId)  %>% filter(n() > 692) %>%  ungroup() %>% filter(Category %in% c("Equity","Fixed Income"))
#number_of_ap or buckets.num.aps
plm.nav <- plm(forward.prem.disc.nav ~  number_of_ap +  n_aps_daily + prem.disc.nav + pqs + pes_k + 
                 hhi_volume + volatility_ratio_ln + turnover + MER + age +  
                 + size + per_short_volume + d.corra + vix + tsx_return + daily.disclosure + allocation.methodology  +covid + avg_volume, data=df.filtered, 
               index =c("MC"), model="within")
summary(plm.nav)

coeftest(plm.nav, vcovHC(plm.nav, method = "arellano")) # Heteroskedasticity consistent coefficients (Arellano) - allows to obtain inference based on robust standard errors.
summary(fixef(plm.nav))
summary(fixef(plm.nav, effect = "time"))

pool.nav <- plm(forward.prem.disc.nav ~ second.order +  number_of_ap + prem.disc.nav + pqs + pes_k + 
                  hhi_volume + volatility_ratio_ln + turnover + MER + age +  
                  + size + per_short_volume + d.corra + vix + tsx_return + daily.disclosure + allocation.methodology+covid + avg_volume, data=df, 
                index =c("MC"), model="pooling")
summary(pool.nav)
coeftest(pool.nav, vcovHC(pool.nav, type = "HC3")) #HC3 AND Arellano yield same results


#####
#Present Day NAV
#####
input.year = 2022
df.filtered <- df %>% filter(year==input.year, grepl("Canadian|US|North American|Canada|Floating|Miscellaneous|Passive Inverse/Leveraged", MC, ignore.case=F))# %>% #filter(Category %in% c("Equity","Fixed Income"))

plm.nav.p <- plm(prem.disc.nav ~  number_of_ap + pqs + pes_k + 
                   hhi_volume + volatility_ratio_ln + turnover + MER + age +  
                   + size + per_short_volume + d.corra + vix + tsx_return + daily.disclosure + allocation.methodology+covid + avg_volume, data=df.filtered, 
                 index =c("MC"), model="within")
summary(plm.nav.p)
coeftest(plm.nav.p, vcovHC(plm.nav.p, method = "arellano")) # Heteroskedasticity consistent coefficients (Arellano) - allows to obtain inference based on robust standard errors.
summary(fixef(plm.nav.p))

lm.nav.p <- lm(prem.disc.nav ~ second.order +  number_of_ap + pqs + pes_k + 
                 hhi_volume + volatility_ratio_ln + turnover + MER + age +  
                 + size + per_short_volume + d.corra + vix + tsx_return + daily.disclosure + allocation.methodology+covid + avg_volume+ factor(MC), data = df.filtered)
#remove pqs due to potential multicollineatiry
summary(lm.nav.p)
lm.shares$coefficients[2:16] #coefficients look identical
check_collinearity(lm.nav.p) 


coeftest(plm.nav.p, vcovHC(plm.nav.p, method = "arellano")) # Heteroskedasticity consistent coefficients (Arellano) - allows to obtain inference based on robust standard errors
summary(fixef(plm.nav.p))


pool.nav.p <- plm(prem.disc.nav ~ second.order +  number_of_ap + pqs + pes_k + 
                    hhi_volume + volatility_ratio_ln + turnover + MER + age +  
                    + size + per_short_volume + d.corra + vix + tsx_return + daily.disclosure + allocation.methodology+covid + avg_volume, data=df, 
                  index =c("MC"), model="pooling")

summary(pool.nav.p)
coeftest(pool.nav.p, vcovHC(pool.nav.p, type = "HC3")) # Heteroskedasticity consistent coefficients, type 3

pool.nav$aliased
pool.nav$vcov #No collinearity
