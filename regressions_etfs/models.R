# Load required libraries
pacman::p_load(dplyr, janitor, tidyr, tidyverse, fuzzyjoin, stringr, zoo,
               data.table, fastDummies, corrr, arm, mclust, lmtest, leaps, 
               fmsb, plm, broom, knitr, performance, datawizard, lfe, lme4, 
               parameters, MuMIn, lattice, ggeffects, sandwich, tsibble)

# Function to read data
read_data <- function(file_path) {
  fread(file_path, encoding = "UTF-8")
}

# Function for data preprocessing and transformations
preprocess_data <- function(df) {
  df %>%
    mutate(
      size = log(exp(size) / 1000000),
      age = log(age),
      prem.disc.nav = prem.disc.nav * 10000,
      volatility_ratio_ln = volatility_ratio_ln * 100,
      per_short_volume = per_short_volume * 100,
      avg_volume = log(avg_volume),
      turnover = turnover * 100,
      vix = log(vix),
      tsx_return = tsx_return * 100,
      d.corra = d.corra * 100,
      pqs = pqs * 10000,
      pes_k = pes_k * 10000,
      prs_k_5m = prs_k_5m * 10000,
      ppi_k_5m = ppi_k_5m * 10000,
      year = as.character(year)
    ) %>%
    group_by(FundId, year) %>%
    mutate(across(where(is.numeric) & !c(number_of_ap, date, num_active_50, 
                                         n_aps_daily, active_aps, MER, size, 
                                         age, ADV, d.corra, vix, tsx_return, nrd.number),
                  ~ DescTools::Winsorize(.x, probs = c(0.0005, 0.9995), na.rm = TRUE, type = 1))) %>%
    ungroup() %>%
    group_by(year) %>%
    mutate(across(c(d.corra, vix, tsx_return),
                  ~ DescTools::Winsorize(.x, probs = c(0.0005, 0.9995), na.rm = TRUE, type = 1))) %>%
    ungroup()
}

# Function to rename and create new variables
rename_and_create_vars <- function(df) {
  df %>%
    rename(MC = "Morningstar Category", Category = "Global Broad Category Group") %>%
    group_by(FundId, year) %>%
    mutate(
      second.order = number_of_ap * prem.disc.nav,
      forward.prem.disc.nav = dplyr::lead(prem.disc.nav, 1),
      f.shares.bloomberg = dplyr::lead(shares.bloomberg, 1),
      f.DoD.Shares.Bloomberg = abs((dplyr::lead(shares.bloomberg, 1) / shares.bloomberg) - 1),
      buckets.num.aps = case_when(
        number_of_ap == 1 ~ "1",
        number_of_ap < 7 ~ "2-6",
        number_of_ap < 11 ~ "7-10",
        TRUE ~ "11+"
      )
    ) %>%
    ungroup() %>%
    filter(!allocation.methodology %in% "", !FundId %in% "", !is.na(date), !is.na(volatility_ratio_ln))
}

# Function to update allocation methodology
update_allocation_methodology <- function(df) {
  df %>%
    mutate(allocation.methodology = ifelse(allocation.methodology %in% 
                                             c("Enhanced, options or leverage (index or rules-based)", 
                                               "Index-tracking, replication", 
                                               "Rules-based, non-index tracking"), 
                                           "Passive", 
                                           allocation.methodology))
}



# Function to create COVID indicator
create_covid_indicator <- function(df) {
  covid_dates <- as.character(seq(as.Date("2020-02-20"), as.Date("2020-04-09"), by = "day"))
  df %>%
    mutate(covid = as.integer(as.character(date) %in% covid_dates))
}

# Function to filter out specific symbols
filter_symbols <- function(df) {
  df %>% filter(!sym %in% c('EAGB', 'EARK', 'TOCA', 'TOCM', 'ZINN'))
}

# Function to run models
run_model <- function(formula, data, index, model_type) {
  plm(formula, data = data, index = index, model = model_type)
}

# Main data processing pipeline
main_pipeline <- function(file_path) {
  read_data(file_path) %>%
    preprocess_data() %>%
    rename_and_create_vars() %>%
    update_allocation_methodology() %>%
    create_covid_indicator() %>%
    filter_symbols()
}

# Execute main pipeline
df <- main_pipeline(file_path)

# Apply Specific Condition to Model such as Geographies, IFMs, etc.
filter_data_for_model <- function(df, input_year) {
  df %>% 
    filter(year == input_year)#, 
           #grepl("Canadian|US|North American|Canada|Floating|Miscellaneous|Passive Inverse/Leveraged", MC, ignore.case = FALSE),
           #nrd.number %in% c(550, 1850, 2130, 3090, 3900, 4850, 17240, 28980))
}

# Model formulas
pqs_formula <- pqs ~ number_of_ap + hhi_volume + prem.disc.nav + volatility_ratio_ln + 
  turnover + MER + age + size + per_short_volume + d.corra + vix + 
  tsx_return + factor(daily.disclosure) + covid + allocation.methodology + avg_volume

pes_k_formula <- pes_k ~ number_of_ap + prem.disc.nav + hhi_volume + volatility_ratio_ln + 
  turnover + MER + age + size + per_short_volume + d.corra + vix + 
  tsx_return + daily.disclosure + covid + allocation.methodology + avg_volume

nav_formula <- forward.prem.disc.nav ~ number_of_ap + n_aps_daily + prem.disc.nav + pqs + pes_k + 
  hhi_volume + volatility_ratio_ln + turnover + MER + age + size + per_short_volume + 
  d.corra + vix + tsx_return + daily.disclosure + allocation.methodology + covid + avg_volume

nav_p_formula <- prem.disc.nav ~ number_of_ap + pqs + pes_k + hhi_volume + volatility_ratio_ln + 
  turnover + MER + age + size + per_short_volume + d.corra + vix + tsx_return + 
  daily.disclosure + allocation.methodology + covid + avg_volume

# Function to run and summarize model
run_and_summarize_model <- function(formula, data, index, model_type) {
  model <- run_model(formula, data, index, model_type)
  list(
    summary = summary(model),
    coeftest = coeftest(model, vcovHC(model, method = "arellano")),
    fixef = summary(fixef(model))
  )
}

# Run models
input_year <- 2022
df_filtered <- filter_data_for_model(df, input_year)

models <- list(
  pqs = run_and_summarize_model(pqs_formula, df_filtered, c("MC"), "within"),
  pes_k = run_and_summarize_model(pes_k_formula, df_filtered, c("MC"), "within"),
  nav = run_and_summarize_model(nav_formula, df_filtered, c("MC"), "within"),
  nav_p = run_and_summarize_model(nav_p_formula, df_filtered, c("MC"), "within")
)

# Print results
lapply(names(models), function(model_name) {
  cat("\nResults for", model_name, "model:\n")
  print(models[[model_name]]$summary)
  cat("\nCoefficient test:\n")
  print(models[[model_name]]$coeftest)
  cat("\nFixed effects:\n")
  print(models[[model_name]]$fixef)
})
