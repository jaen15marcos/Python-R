# -*- coding: utf-8 -*-
"""
Created on Tue Aug  1 14:54:48 2023

@author: MJaenCortes
"""

import pandas as pd
import numpy as np



raw_data = pd.read_excel("T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\Research and Analysis Advisory -Operational\\Corporate Finance\\Women on Boards\\Year 9 Grapevine @ Aug 1 (for stats - all changes made).xlsx", sheet_name="20234244881256" )
print(raw_data)
raw_data.columns = ['Participant No.','Submitted Date', 'Submitted Language', 'Q.1',
                    'Q.2', "Q.3","Q.4","Q.5.1", "Q.5.2", "Q.6.1","Q.6.2","Q.7.1","Q.7.2","Q.7.3",
                    "Q.7.4","Q.7.5","Q.8","Q.9.1","Q.9.2","Q.10.1", "Q.10.2",
                    "Q.10.3","Q.10.4","Q.10.5","Q.10.6","Q.11.1","Q.11.2",
                    "Q.11.3","Q.11.4","Q.11.5","Q.11.6","Q.12.1","Q.12.2","Q.13.1",
                    "Q.13.2","Q.14.1", "Q.14.2","Q.14.3","Q.14.4","Q.14.5", "Q.14.6",
                    "Q.14.7","Q.14.8","Q.14.9","Q.14.10","Q.15.1","Q.15.2","Q.16.1",
                    "Q.16.2","Q.16.3","Q.16.4","Q.16.5","Q.16.6","Q.16.7","Q.16.8",
                    "Q.17.1","Q.17.2", "Q.18.1","Q.18.2","Q.18.3","Q.18.4",
                    "Q.19.1","Q.19.2","Q.19.3","Q.19.4","Q.20.1","Q.20.2","Q.20.3","Q.20.4",
                    "Q.21.1","Q.21.2","Q.22.1","Q.22.2","Q.23.1","Q.23.2","Q.24.1",
                    "Q.24.2","Q.24.3","Q.24.4","Q.24.5","Q.25.1","Q.25.2","Q.26.1","Q.26.2",
                    "Q.26.3","Q.26.4","Q.27.1","Q.27.2","Q.27.3","Q.27.4","Q.28.1",
                    "Q.28.2","Q.28.3","Q.28.4","Q.28.5","Q.29.1","Q.29.2","Q.29.3",
                    "Q.29.4","Q.29.5","Q.30","Q.31", "Market Cap"]
"""
Executive Trends Calculations 
"""
#percentage of board seats held by women = Number of Women on the board / Total Number of board members
percentage_of_board_seats_held_by_women = sum(pd.to_numeric(raw_data["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(raw_data["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))
numerator_percentage_of_board_seats_held_by_women = sum(pd.to_numeric(raw_data["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_of_board_seats_held_by_women = sum(pd.to_numeric(raw_data["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))

#percentage of issuers had at least one woman on their board = Number of Women on the board > 0/ count(number of issuers)
percentage_of_issuers_had_at_least_one_woman_on_their_board = pd.to_numeric(raw_data["Q.19.1"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(raw_data["Q.19.1"], errors='coerce').fillna(0, downcast='infer') > 0].count()/ raw_data['Participant No.'].count()
numerator_percentage_of_issuers_had_at_least_one_woman_on_their_board = pd.to_numeric(raw_data["Q.19.1"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(raw_data["Q.19.1"], errors='coerce').fillna(0, downcast='infer') > 0].count()
denominator_percentage_of_issuers_had_at_least_one_woman_on_their_board = raw_data['Participant No.'].count()

#percentage of current chairs of board were women = Number of Chairs that are women/ count(number of issuers)
percentage_of_current_chairs_of_board_were_women= raw_data["Q.20.1"].value_counts().get(0)/ raw_data['Participant No.'].count()
numerator_percentage_of_current_chairs_of_board_were_women = raw_data["Q.20.1"].value_counts().get(0)
denominator_percentage_of_current_chairs_of_board_were_women = raw_data['Participant No.'].count()

#percentage of Board vacancies filled by women = Board vacancies filled by women/Board vacancies filled by women + board vacancies filled by men
percentage_of_board_vacancies_filled_by_women = sum(pd.to_numeric(raw_data["Q.26.3"], errors='coerce').fillna(0, downcast='infer'))/(sum(pd.to_numeric(raw_data["Q.26.2"], errors='coerce').fillna(0, downcast='infer'))+sum(pd.to_numeric(raw_data["Q.26.3"], errors='coerce').fillna(0, downcast='infer')))
numerator_percentage_of_board_vacancies_filled_by_women = sum(pd.to_numeric(raw_data["Q.26.3"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_of_board_vacancies_filled_by_women = (sum(pd.to_numeric(raw_data["Q.26.2"], errors='coerce').fillna(0, downcast='infer'))+sum(pd.to_numeric(raw_data["Q.26.3"], errors='coerce').fillna(0, downcast='infer')))

#percentage of issuers had at least 3 woman on their board = Number of Women on the board > 2/ count(number of issuers)
percentage_of_issuers_had_at_least_three_woman_on_their_board = pd.to_numeric(raw_data["Q.19.1"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(raw_data["Q.19.1"], errors='coerce').fillna(0, downcast='infer') > 2].count()/ raw_data['Participant No.'].count()
numerator_percentage_of_issuers_had_at_least_three_woman_on_their_board = pd.to_numeric(raw_data["Q.19.1"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(raw_data["Q.19.1"], errors='coerce').fillna(0, downcast='infer') > 2].count()
denominator_percentage_of_issuers_had_at_least_three_woman_on_their_board = raw_data['Participant No.'].count()
 
#percentage of board seats occupied by women for issuers with < $1 billion market capitalization = if market_cap < 1b then Number of Women on the board > 0/ if market_cap < 1b then Number of board members
less_than_1billion = raw_data.loc[pd.to_numeric(raw_data["Market Cap"], errors='coerce').fillna(0, downcast='infer')  < 1000000000]
percentage_of_board_seats_occupied_by_women_for_issuers_with_less_than_1billion_marketcap = sum(pd.to_numeric(less_than_1billion["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(less_than_1billion["Q.19.3"], errors='coerce').fillna(0, downcast='infer')) 
numerator_percentage_of_board_seats_occupied_by_women_for_issuers_with_less_than_1billion_marketcap = sum(pd.to_numeric(less_than_1billion["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_of_board_seats_occupied_by_women_for_issuers_with_less_than_1billion_marketcap = sum(pd.to_numeric(less_than_1billion["Q.19.3"], errors='coerce').fillna(0, downcast='infer')) 

#percentage of board seats occupied by women for issuers with $1-2 billion market capitalization
with1to2billion_marketcap = raw_data.loc[(pd.to_numeric(raw_data["Market Cap"], errors='coerce').fillna(0, downcast='infer') <= 2000000000) & (pd.to_numeric(raw_data["Market Cap" ], errors='coerce').fillna(0, downcast='infer') >= 1000000000)]
percentage_of_board_seats_occupied_by_women_for_issuers_with_1to2billion_marketcap = sum(pd.to_numeric(with1to2billion_marketcap["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(with1to2billion_marketcap["Q.19.3"], errors='coerce').fillna(0, downcast='infer')) 
numerator_percentage_of_board_seats_occupied_by_women_for_issuers_with_1to2billion_marketcap = sum(pd.to_numeric(with1to2billion_marketcap["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_of_board_seats_occupied_by_women_for_issuers_with_1to2billion_marketcap = sum(pd.to_numeric(with1to2billion_marketcap["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))

#percentage of board seats occupied by women for issuers with $2-10 billion market capitalization
with2to10billion_marketcap = raw_data.loc[(pd.to_numeric(raw_data["Market Cap"], errors='coerce').fillna(0, downcast='infer') <= 10000000000) & (pd.to_numeric(raw_data["Market Cap" ], errors='coerce').fillna(0, downcast='infer') >= 2000000000)]
percentage_of_board_seats_occupied_by_women_for_issuers_with_2to10billion_marketcap = sum(pd.to_numeric(with2to10billion_marketcap["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(with2to10billion_marketcap["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))
numerator_percentage_of_board_seats_occupied_by_women_for_issuers_with_2to10billion_marketcap =  sum(pd.to_numeric(with2to10billion_marketcap["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_of_board_seats_occupied_by_women_for_issuers_with_2to10billion_marketcap = sum(pd.to_numeric(with2to10billion_marketcap["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))

#percentage of board seats occupied by women for issuers with <$10 billion market capitalization
with10_or_morebillion_marketcap = raw_data.loc[(pd.to_numeric(raw_data["Market Cap"], errors='coerce').fillna(0, downcast='infer') > 10000000000)]
percentage_of_board_seats_occupied_by_women_for_issuers_with_10billion_marketcap = sum(pd.to_numeric(with10_or_morebillion_marketcap["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(with10_or_morebillion_marketcap["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))
numerator_percentage_of_board_seats_occupied_by_women_for_issuers_with_10billion_marketcap = sum(pd.to_numeric(with10_or_morebillion_marketcap["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_of_board_seats_occupied_by_women_for_issuers_with_10billion_marketcap = sum(pd.to_numeric(with10_or_morebillion_marketcap["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))

#percentage of Issuers with at least one woman in an executive officer position = issuers with at least one women/number of issuers that disclosed
both = raw_data.loc[(raw_data["Q.23.1"] == "Disclosed both the number and percentage of women in executive officer positions")]
both_women = pd.to_numeric(both["Q.24.1"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(both["Q.24.1"], errors='coerce').fillna(0, downcast='infer') > 0].count()
number = raw_data.loc[(raw_data["Q.23.1"] == "Disclosed the number of women in executive officer positions")]
number_women = pd.to_numeric(number["Q.24.1"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(number["Q.24.1"], errors='coerce').fillna(0, downcast='infer') > 0].count()
percentage = raw_data.loc[(raw_data["Q.23.1"] == "Disclosed the percentage of women in executive officer positions")]
percentage_women =  pd.to_numeric(percentage["Q.24.2"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(percentage["Q.24.2"], errors='coerce').fillna(0, downcast='infer') > 0].count()
percentage_of_Issuers_with_at_least_one_woman_exec = (both_women + number_women + percentage_women) /(raw_data.shape[0] - raw_data.loc[raw_data["Q.23.1"] == "Neither disclosed"].shape[0])
numerator_percentage_of_Issuers_with_at_least_one_woman_exec = (both_women + number_women + percentage_women)
denominator_percentage_of_Issuers_with_at_least_one_woman_exec = (raw_data.shape[0] - raw_data.loc[raw_data["Q.23.1"] == "Neither disclosed"].shape[0])

#percentage of Issuers with a woman CEO = issuers with women ceo/ count(issuers)
percentage_of_issuers_with_woman_ceo = raw_data.loc[(raw_data["Q.21.1"] == "Y")].shape[0]/raw_data.shape[0]
numerator_percentage_of_issuers_with_woman_ceo = raw_data.loc[(raw_data["Q.21.1"] == "Y")].shape[0]
denominator_percentage_of_issuers_with_woman_ceo = raw_data.shape[0]

#percentage of Issuers with a woman CFO = issuers with women CFO/ count(issuers)
percentage_of_issuers_with_woman_cfo = raw_data.loc[(raw_data["Q.22.1"] == "Y")].shape[0]/raw_data.shape[0]
numerator_percentage_of_issuers_with_woman_cfo = raw_data.loc[(raw_data["Q.22.1"] == "Y")].shape[0]
denominator_percentage_of_issuers_with_woman_cfo = raw_data.shape[0]

#Issuers that adopted a policy relating to the representation of women on their board = Issuers with written policy (clear and unclear)/all issuers
percentage_of_issuer_policy_board_womeen = raw_data.loc[(raw_data["Q.12.1"] == "Issuer has a written policy") | (raw_data["Q.12.1"] == "Policy exists but unwritten or unclear if written")].shape[0]/raw_data.shape[0]
percentage_of_without_issuer_policy_board_womeen = 1 - percentage_of_issuer_policy_board_womeen
numerator_percentage_of_issuer_policy_board_womeen = raw_data.loc[(raw_data["Q.12.1"] == "Issuer has a written policy") | (raw_data["Q.12.1"] == "Policy exists but unwritten or unclear if written")].shape[0]
denominator_percentage_of_issuer_policy_board_womeen = raw_data.shape[0]
numerator_percentage_of_without_issuer_policy_board_womeen = raw_data.shape[0] - raw_data.loc[(raw_data["Q.12.1"] == "Issuer has a written policy") | (raw_data["Q.12.1"] == "Policy exists but unwritten or unclear if written")].shape[0]
denominator_percentage_of_without_issuer_policy_board_womeen = raw_data.shape[0]
 
#Issuers that adopted targets for the representation of women on their board = Issuers that adopted targets for the representation of women on their board/all issuers
percentage_of_issuer_targets_board = raw_data.loc[(raw_data["Q.13.1"] == "Women on board targets adopted")].shape[0]/raw_data.shape[0]
numerator_percentage_of_issuer_targets_board = raw_data.loc[(raw_data["Q.13.1"] == "Women on board targets adopted")].shape[0]
denominator_percentage_of_issuer_targets_board = raw_data.shape[0]
percentage_of_issuer_without_targets_board = 1 - percentage_of_issuer_targets_board
numerator_percentage_of_issuer_without_targets_board = raw_data.shape[0] - raw_data.loc[(raw_data["Q.13.1"] == "Women on board targets adopted")].shape[0] 
denominator_percentage_of_issuer_without_targets_board = raw_data.shape[0]

#Issuers that adopted targets for the representation of women in executive officer positions 
percentage_of_issuer_targets_exec = raw_data.loc[(raw_data["Q.15.1"] == "Women in executive officer positions target adopted")].shape[0]/raw_data.shape[0]
numerator_percentage_of_issuer_targets_exec = raw_data.loc[(raw_data["Q.15.1"] == "Women in executive officer positions target adopted")].shape[0]
denominator_percentage_of_issuer_targets_exec = raw_data.shape[0]

#Issuers that adopted director term limits and breakdown
percentage_of_issuer_terms_limit_director = raw_data.loc[(raw_data["Q.9.1"] == "Director term limits (alone or with other mechanisms)")].shape[0]/raw_data.shape[0]
numerator_percentage_of_issuer_terms_limit_director = raw_data.loc[(raw_data["Q.9.1"] == "Director term limits (alone or with other mechanisms)")].shape[0]
denominator_percentage_of_issuer_terms_limit_director = raw_data.shape[0]

df3 = raw_data.loc[(raw_data["Q.9.1"] == "Director term limits (alone or with other mechanisms)")]
percentage_agentenure_limit = df3.loc[(df3["Q.10.1"] == "Term limit related to age") & (df3["Q.10.2"] == "Term limit related to tenure")].shape[0]/df3.shape[0]
numerator_percentage_agentenure_limit = df3.loc[(df3["Q.10.1"] == "Term limit related to age") & (df3["Q.10.2"] == "Term limit related to tenure")].shape[0]
denominator_percentage_agentenure_limit = df3.shape[0]
percentage_age_limit = df3.loc[(df3["Q.10.1"] == "Term limit related to age") & (df3["Q.10.2"] != "Term limit related to tenure")].shape[0]/df3.shape[0]
numerator_percentage_age_limit = df3.loc[(df3["Q.10.1"] == "Term limit related to age") & (df3["Q.10.2"] != "Term limit related to tenure")].shape[0]
denominator_percentage_age_limit = df3.shape[0]
percentage_tenure = df3.loc[(df3["Q.10.2"] == "Term limit related to tenure") & (df3["Q.10.1"] != "Term limit related to age")].shape[0]/df3.shape[0]
numerator_percentage_tenure = df3.loc[(df3["Q.10.2"] == "Term limit related to tenure") & (df3["Q.10.1"] != "Term limit related to age")].shape[0]
denominator_percentage_tenure = df3.shape[0]
percentage_women_on_board_with_term_limits = sum(pd.to_numeric(df3["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(df3["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))
numerator_percentage_women_on_board_with_term_limits = sum(pd.to_numeric(df3["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_women_on_board_with_term_limits = sum(pd.to_numeric(df3["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))

df_no_term_limits = raw_data.loc[(raw_data["Q.9.1"] != "Director term limits (alone or with other mechanisms)")]

percentage_women_on_board_without_term_limits = sum(pd.to_numeric(df_no_term_limits["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(df_no_term_limits["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))
numerator_percentage_women_on_board_without_term_limits = sum(pd.to_numeric(df_no_term_limits["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_women_on_board_without_term_limits = sum(pd.to_numeric(df_no_term_limits["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))

number_of_issuers = raw_data.shape[0]
numerator_number_of_issuers = raw_data.shape[0]
denominator_number_of_issuers = raw_data.shape[0]
"""
By Industry
"""
mining = raw_data.loc[raw_data["Q.5.1"] == "Mining"]
oilngas = raw_data.loc[raw_data["Q.5.1"] == "Oil & Gas"]
financial_services = raw_data.loc[raw_data["Q.5.1"] == "Financial Services"]
technology = raw_data.loc[raw_data["Q.5.1"] == "Technology"]
real_estate = raw_data.loc[raw_data["Q.5.1"] == "Real estate"]
retail = raw_data.loc[raw_data["Q.5.1"] == "Retail"]
manufacturing = raw_data.loc[raw_data["Q.5.1"] == "Manufacturing"]
utilities = raw_data.loc[raw_data["Q.5.1"] == "Utilities"]
biotechnology = raw_data.loc[raw_data["Q.5.1"] == "Biotechnology"]
other = raw_data.loc[raw_data["Q.5.1"].str.contains('Other')]
l = [mining, oilngas, financial_services, technology, real_estate, retail, manufacturing, utilities, biotechnology, other]
#percentage of issuers with at least one woman in an executive officer position
industry_percentage_of_Issuers_with_at_least_one_woman_exec = []
industry_percentage_of_Issuers_with_at_least_one_woman_board = []
names_industry = []
number_of_issuers = []
for i in range(len(l)):
    industry_both = l[i].loc[(l[i]["Q.23.1"] == "Disclosed both the number and percentage of women in executive officer positions")]
    industry_both_women = pd.to_numeric(industry_both["Q.24.1"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(industry_both["Q.24.1"], errors='coerce').fillna(0, downcast='infer') > 0].count()
    industry_number =  l[i].loc[( l[i]["Q.23.1"] == "Disclosed the number of women in executive officer positions")]
    industry_number_women = pd.to_numeric(industry_number["Q.24.1"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(industry_number["Q.24.1"], errors='coerce').fillna(0, downcast='infer') > 0].count()
    industry_percentage = l[i].loc[(l[i]["Q.23.1"] == "Disclosed the percentage of women in executive officer positions")]
    industry_percentage_women =  pd.to_numeric(industry_percentage["Q.24.2"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(industry_percentage["Q.24.2"], errors='coerce').fillna(0, downcast='infer') > 0].count()
    industry_percentage_of_Issuers_with_at_least_one_woman_exec.append((industry_both_women + industry_number_women + industry_percentage_women) /(l[i].shape[0] - l[i].loc[l[i]["Q.23.1"] == "Neither disclosed"].shape[0]))
    industry_percentage_of_Issuers_with_at_least_one_woman_board.append(pd.to_numeric(l[i]["Q.19.1"], errors='coerce').fillna(0, downcast='infer')[pd.to_numeric(l[i]["Q.19.1"], errors='coerce').fillna(0, downcast='infer') > 0].count()/ l[i]['Participant No.'].count())
    names_industry.append(l[i]["Q.5.1"].values[1])
    number_of_issuers.append(l[i].shape[0])
    
#% of issuers adopted other mechanisms of board renewal (but not director term limits) 
percentage_of_issuer_adopted_other_mechanisms_of_board_renawal = raw_data.loc[(raw_data["Q.9.1"] == "Other mechanisms for board renewal only")].shape[0]/raw_data.shape[0]
numerator_percentage_of_issuer_adopted_other_mechanisms_of_board_renawal = raw_data.loc[(raw_data["Q.9.1"] == "Other mechanisms for board renewal only")].shape[0]
denominator_percentage_of_issuer_adopted_other_mechanisms_of_board_renawal = raw_data.shape[0]

# % of issuers did not adopt director term limits or other mechanisms of board renewal ;
percentage_of_issuers_did_not_adopt_director_term_limits =  raw_data.loc[(raw_data["Q.9.1"] == "No director term limits or other mechanisms")].shape[0]/raw_data.shape[0]
numerator_percentage_of_issuers_did_not_adopt_director_term_limits = raw_data.loc[(raw_data["Q.9.1"] == "No director term limits or other mechanisms")].shape[0]
denominator_percentage_of_issuers_did_not_adopt_director_term_limits = raw_data.shape[0]

#breakdown of women on boards with targets and without 
percentage_of_issuers_with_targets_board_seats_women = sum(pd.to_numeric(raw_data.loc[(raw_data["Q.13.1"] == "Women on board targets adopted")]["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(raw_data.loc[(raw_data["Q.13.1"] == "Women on board targets adopted")]["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))
numerator_percentage_of_issuers_with_targets_board_seats_women = sum(pd.to_numeric(raw_data.loc[(raw_data["Q.13.1"] == "Women on board targets adopted")]["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_of_issuers_with_targets_board_seats_women = sum(pd.to_numeric(raw_data.loc[(raw_data["Q.13.1"] == "Women on board targets adopted")]["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))

percentage_of_issuers_without_targets_board_seats_women = sum(pd.to_numeric(raw_data.loc[(raw_data["Q.13.1"] == "Issuer elected to not adopt targets")]["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(raw_data.loc[(raw_data["Q.13.1"] == "Issuer elected to not adopt targets")]["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))
numerator_percentage_of_issuers_without_targets_board_seats_women = sum(pd.to_numeric(raw_data.loc[(raw_data["Q.13.1"] == "Issuer elected to not adopt targets")]["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_of_issuers_without_targets_board_seats_women = sum(pd.to_numeric(raw_data.loc[(raw_data["Q.13.1"] == "Issuer elected to not adopt targets")]["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))

#breakdown of women on boards with policies and without 
df = raw_data.loc[(raw_data["Q.12.1"] == "Issuer has a written policy") | (raw_data["Q.12.1"] == "Policy exists but unwritten or unclear if written")]
percentage_of_issuers_with_policy_board_seats_women = sum(pd.to_numeric(df["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(df["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))
numerator_percentage_of_issuers_with_policy_board_seats_women = sum(pd.to_numeric(df["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_of_issuers_with_policy_board_seats_women = sum(pd.to_numeric(df["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))

df2 = raw_data.loc[(raw_data["Q.12.1"] == "Issuer elected to not adopt a policy")]
percentage_of_issuers_without_policy_board_seats_women =  sum(pd.to_numeric(df2["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))/sum(pd.to_numeric(df2["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))
numerator_percentage_of_issuers_without_policy_board_seats_women = sum(pd.to_numeric(df2["Q.19.1"], errors='coerce').fillna(0, downcast='infer'))
denominator_percentage_of_issuers_without_policy_board_seats_women = sum(pd.to_numeric(df2["Q.19.3"], errors='coerce').fillna(0, downcast='infer'))

#filled vacated board seats during the year 
denominator_filled_vacated_board_seats = sum(pd.to_numeric(raw_data["Q.26.1"], errors='coerce').fillna(0, downcast='infer'))
numerator_filled_vacated_board_seats = (sum(pd.to_numeric(raw_data["Q.26.2"], errors='coerce').fillna(0, downcast='infer'))+sum(pd.to_numeric(raw_data["Q.26.3"], errors='coerce').fillna(0, downcast='infer')))
filled_vacated_board_seats = (sum(pd.to_numeric(raw_data["Q.26.2"], errors='coerce').fillna(0, downcast='infer'))+sum(pd.to_numeric(raw_data["Q.26.3"], errors='coerce').fillna(0, downcast='infer')))/sum(pd.to_numeric(raw_data["Q.26.1"], errors='coerce').fillna(0, downcast='infer'))



list_of_values_output = [percentage_of_board_seats_held_by_women, percentage_of_issuers_had_at_least_one_woman_on_their_board,  
                         percentage_of_current_chairs_of_board_were_women,
                         percentage_of_board_vacancies_filled_by_women,
                         percentage_of_issuers_had_at_least_three_woman_on_their_board,
                         percentage_of_board_seats_occupied_by_women_for_issuers_with_less_than_1billion_marketcap, 
                         percentage_of_board_seats_occupied_by_women_for_issuers_with_1to2billion_marketcap,
                         percentage_of_board_seats_occupied_by_women_for_issuers_with_2to10billion_marketcap,
                         percentage_of_board_seats_occupied_by_women_for_issuers_with_10billion_marketcap,
                         percentage_of_Issuers_with_at_least_one_woman_exec,
                         percentage_of_issuers_with_woman_ceo,
                         percentage_of_issuers_with_woman_cfo,
                         percentage_of_issuer_policy_board_womeen,
                         percentage_of_without_issuer_policy_board_womeen,
                         percentage_of_issuer_targets_board,
                         percentage_of_issuer_without_targets_board,
                         percentage_of_issuer_targets_exec,
                         percentage_of_issuer_terms_limit_director,
                         percentage_agentenure_limit,
                         percentage_age_limit,
                         percentage_tenure,
                         percentage_of_issuer_adopted_other_mechanisms_of_board_renawal,
                         percentage_of_issuers_did_not_adopt_director_term_limits,
                         percentage_of_issuers_with_targets_board_seats_women,
                         percentage_of_issuers_without_targets_board_seats_women,
                         percentage_of_issuers_with_policy_board_seats_women,
                         percentage_of_issuers_without_policy_board_seats_women,
                         number_of_issuers,
                         percentage_women_on_board_with_term_limits,
                         percentage_women_on_board_without_term_limits, filled_vacated_board_seats]

list_of_names = ["Total board seats occupied by women", "Issuers with at least one woman on their board",
                 "Chairs of the board who are women",
                 "Board vacancies filled by women",
                 "Issuers with three or more women on their board",
                 "Board seats occupied by women for issuers with < $1 billion market capitalization",
                 "Board seats occupied by women for issuers with $1-2 billion market capitalization",
                 "Board seats occupied by women for issuers with $2-10 billion market capitalization",
                 "Board seats occupied by women for issuers with over $10 billion market capitalization",
                 "Issuers with at least one woman in an executive officer position",
                 "Issuers with a woman CEO", "Issuers with a woman CFO", 
                 "Issuers that adopted a policy relating to the representation of women on their board",
                 "Issuers that did not adopted a policy relating to the representation of women on their board",
                 "Issuers that adopted targets for the representation of women on their board",
                 "Issuers that did not adopted targets for the representation of women on their board",
                 "Issuers that adopted targets for the representation of women in executive officer positions",
                 "Issuers that adopted director term limits","Issuers that adopted director term limits with regards to age and tenure limits",
                 "Issuers that adopted director term limits with regards to age limits",
                 "Issuers that adopted director term limits with regards to tenure limits",
                 "Issuers that adopted other mechanisms of board renewal (but not director term limits) ",
                 "Issuers that did not adopted director term limits or other mechanisms of board renewal", "Board seats held by women for Issuers with targets for women board representation",
                 "Board seats held by women for Issuers without targets for women board representation",
                 "Board seats held by women for Issuers with policy for women board representation", "Board seats held by women for Issuers without policy for women board representation",
                 "Number of Issuers",
                 "Total board seats occupied by women with Issuers that adopted term limits",
                 "Total board seats occupied by women with Issuers without term limits", "Filled vacated board seats"]
# initialize data of lists.
year = ["Year 9"]*len(list_of_names)
data_points = {'Year': year,
        'Key Trend': list_of_names,
        'Value': list_of_values_output}

# Create DataFrame
output_1 = pd.DataFrame(data_points)

data_points_trends_1 = {'Year': ["Year 9"]*len(industry_percentage_of_Issuers_with_at_least_one_woman_board),
        'Key Trend': ["percentage of issuers with at least one woman on their board"]*len(names_industry),
        'Industry': names_industry,
        'Values': industry_percentage_of_Issuers_with_at_least_one_woman_board}

data_points_trends_2 = {'Year': ["Year 9"]*len(industry_percentage_of_Issuers_with_at_least_one_woman_exec),
        'Key Trend': ["percentage of issuers with at least one woman in an executive officer position"]*len(names_industry),
        'Industry': names_industry,
        'Values': industry_percentage_of_Issuers_with_at_least_one_woman_exec}

data_points_trends_2 = pd.DataFrame(data_points_trends_2)
data_points_trends_1 = pd.DataFrame(data_points_trends_1)


output_industry = pd.concat([data_points_trends_1, data_points_trends_2]).reset_index(drop=True)


list_of_values_numerator_output = [numerator_percentage_of_board_seats_held_by_women, numerator_percentage_of_issuers_had_at_least_one_woman_on_their_board,  
                         numerator_percentage_of_current_chairs_of_board_were_women,
                         numerator_percentage_of_board_vacancies_filled_by_women,
                         numerator_percentage_of_issuers_had_at_least_three_woman_on_their_board,
                         numerator_percentage_of_board_seats_occupied_by_women_for_issuers_with_less_than_1billion_marketcap, 
                         numerator_percentage_of_board_seats_occupied_by_women_for_issuers_with_1to2billion_marketcap,
                         numerator_percentage_of_board_seats_occupied_by_women_for_issuers_with_2to10billion_marketcap,
                         numerator_percentage_of_board_seats_occupied_by_women_for_issuers_with_10billion_marketcap,
                         numerator_percentage_of_Issuers_with_at_least_one_woman_exec,
                         numerator_percentage_of_issuers_with_woman_ceo,
                         numerator_percentage_of_issuers_with_woman_cfo,
                         numerator_percentage_of_issuer_policy_board_womeen,
                         numerator_percentage_of_without_issuer_policy_board_womeen,
                         numerator_percentage_of_issuer_targets_board,
                         numerator_percentage_of_issuer_without_targets_board,
                         numerator_percentage_of_issuer_targets_exec,
                         numerator_percentage_of_issuer_terms_limit_director,
                         numerator_percentage_agentenure_limit,
                         numerator_percentage_age_limit,
                         numerator_percentage_tenure,
                         numerator_percentage_of_issuer_adopted_other_mechanisms_of_board_renawal,
                         numerator_percentage_of_issuers_did_not_adopt_director_term_limits,
                         numerator_percentage_of_issuers_with_targets_board_seats_women,
                         numerator_percentage_of_issuers_without_targets_board_seats_women,
                         numerator_percentage_of_issuers_with_policy_board_seats_women,
                         numerator_percentage_of_issuers_without_policy_board_seats_women,
                         numerator_number_of_issuers,
                         numerator_percentage_women_on_board_with_term_limits,
                         numerator_percentage_women_on_board_without_term_limits, numerator_filled_vacated_board_seats]

list_of_values_denominator_output = [denominator_percentage_of_board_seats_held_by_women, denominator_percentage_of_issuers_had_at_least_one_woman_on_their_board,  
                         denominator_percentage_of_current_chairs_of_board_were_women,
                         denominator_percentage_of_board_vacancies_filled_by_women,
                         denominator_percentage_of_issuers_had_at_least_three_woman_on_their_board,
                         denominator_percentage_of_board_seats_occupied_by_women_for_issuers_with_less_than_1billion_marketcap, 
                         denominator_percentage_of_board_seats_occupied_by_women_for_issuers_with_1to2billion_marketcap,
                         denominator_percentage_of_board_seats_occupied_by_women_for_issuers_with_2to10billion_marketcap,
                         denominator_percentage_of_board_seats_occupied_by_women_for_issuers_with_10billion_marketcap,
                         denominator_percentage_of_Issuers_with_at_least_one_woman_exec,
                         denominator_percentage_of_issuers_with_woman_ceo,
                         denominator_percentage_of_issuers_with_woman_cfo,
                         denominator_percentage_of_issuer_policy_board_womeen,
                         denominator_percentage_of_without_issuer_policy_board_womeen,
                         denominator_percentage_of_issuer_targets_board,
                         denominator_percentage_of_issuer_without_targets_board,
                         denominator_percentage_of_issuer_targets_exec,
                         denominator_percentage_of_issuer_terms_limit_director,
                         denominator_percentage_agentenure_limit,
                         denominator_percentage_age_limit,
                         denominator_percentage_tenure,
                         denominator_percentage_of_issuer_adopted_other_mechanisms_of_board_renawal,
                         denominator_percentage_of_issuers_did_not_adopt_director_term_limits,
                         denominator_percentage_of_issuers_with_targets_board_seats_women,
                         denominator_percentage_of_issuers_without_targets_board_seats_women,
                         denominator_percentage_of_issuers_with_policy_board_seats_women,
                         denominator_percentage_of_issuers_without_policy_board_seats_women,
                         denominator_number_of_issuers,
                         denominator_percentage_women_on_board_with_term_limits,
                         denominator_percentage_women_on_board_without_term_limits, denominator_filled_vacated_board_seats]
df_numerator_denominator = {'numerator': list_of_values_numerator_output,
                            'denominator': list_of_values_denominator_output}
df_numerator_denominator = pd.DataFrame(df_numerator_denominator)

#output_1.to_csv('C:\\Users\\MjaenCortes\\Desktop\\key_metrics.csv') , df_numerator_denominator.to_csv('C:\\Users\\MjaenCortes\\Desktop\\key_metrics_numerator_denominator.csv'), output_industry.to_csv('C:\\Users\\MjaenCortes\\Desktop\\key_metrics_industry.csv')
