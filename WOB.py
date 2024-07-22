# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 12:18:39 2024

@author: MJaenCortes
"""
import pandas as pd
import re


def load_and_preprocess_data(file_path, sheet_name):
    # Load the raw data
    raw_data = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Function to process each column name
    def process_column_names(columns):
        new_columns = []
        prev_num = None
        prev_subnum = 0
        
        for col in columns:
            # Check if the column starts with a number
            match = re.match(r'^(\d+)(\.\d+)?', col)
            if match:
                num = match.group(1)
                if num != prev_num:
                    prev_subnum = 0
                
                if match.group(2):  # If there's a subnum like .1, .2, etc.
                    subnum = match.group(2)[1:]  # Remove the leading dot
                    new_col = f'Q.{num}.{subnum}'
                    prev_subnum = int(subnum)
                else:
                    prev_subnum += 1
                    new_col = f'Q.{num}.{prev_subnum}'
                
                prev_num = num
            else:
                # Keep the original name for columns without leading numbers
                new_col = col
            
            new_columns.append(new_col)
        
        return new_columns

    # Apply the processing to all column names
    new_columns = process_column_names(raw_data.columns)
    
    # Set the new column names
    raw_data.columns = new_columns

    return raw_data

def calculate_percentage(numerator, denominator):
    return numerator / denominator if denominator != 0 else 0


def calculate_board_stats(df):
    stats = {}
    
    stats['percentage_of_board_seats_held_by_women'] = calculate_percentage(
        sum(pd.to_numeric(df["Q.19.1"], errors='coerce').fillna(0)),
        sum(pd.to_numeric(df["Q.19.3"], errors='coerce').fillna(0))
    )
    
    stats['percentage_of_issuers_had_at_least_one_woman_on_their_board'] = calculate_percentage(
        (pd.to_numeric(df["Q.19.1"], errors='coerce').fillna(0) > 0).sum(),
        df['Participant No.'].count()
    )
    
    stats['percentage_of_current_chairs_of_board_were_women'] = calculate_percentage(
        df["Q.20.1"].value_counts().get(0, 0),
        df['Participant No.'].count()
    )
    
    stats['percentage_of_board_vacancies_filled_by_women '] = calculate_percentage(
        sum(pd.to_numeric(df["Q.26.3"], errors='coerce').fillna(0, downcast='infer')),
        (sum(pd.to_numeric(df["Q.26.2"], errors='coerce').fillna(0, downcast='infer')) +
         sum(pd.to_numeric(df["Q.26.3"], errors='coerce').fillna(0, downcast='infer')))
    )
    
    stats['percentage_of_issuers_had_at_least_three_woman_on_their_board '] = calculate_percentage(
        (pd.to_numeric(df["Q.19.1"], errors='coerce').fillna(0) > 2).sum(),
        df['Participant No.'].count()
    )
    
    less_than_1billion = df.loc[pd.to_numeric(df['Market Cap'], errors='coerce').fillna(0, downcast='infer')  < 1000000000]
    stats['percentage_of_board_seats_occupied_by_women_for_issuers_with_less_than_1billion_marketcap'] = calculate_percentage(
        pd.to_numeric(less_than_1billion["Q.19.1"], errors='coerce').fillna(0, downcast='infer').sum(),
        pd.to_numeric(less_than_1billion["Q.19.3"], errors='coerce').fillna(0, downcast='infer').sum()
    )
    
    with1to2billion_marketcap = df.loc[(pd.to_numeric(df['Market Cap'], errors='coerce').fillna(0, downcast='infer') <= 2000000000) 
                                       & (pd.to_numeric(df['Market Cap'], errors='coerce').fillna(0, downcast='infer') >= 1000000000)]
    stats['percentage_of_board_seats_occupied_by_women_for_issuers_with_1to2billion_marketcap'] = calculate_percentage(
        pd.to_numeric(with1to2billion_marketcap["Q.19.1"], errors='coerce').fillna(0, downcast='infer').sum(),
        pd.to_numeric(with1to2billion_marketcap["Q.19.3"], errors='coerce').fillna(0, downcast='infer').sum()
    )
    
    with2to10billion_marketcap = df.loc[(pd.to_numeric(df['Market Cap'], errors='coerce').fillna(0, downcast='infer') <= 10000000000) 
                                       & (pd.to_numeric(df['Market Cap'], errors='coerce').fillna(0, downcast='infer') >= 2000000000)]
    stats['percentage_of_board_seats_occupied_by_women_for_issuers_with_2to10billion_marketcap'] = calculate_percentage(
        pd.to_numeric(with2to10billion_marketcap["Q.19.1"], errors='coerce').fillna(0, downcast='infer').sum(),
        pd.to_numeric(with2to10billion_marketcap["Q.19.3"], errors='coerce').fillna(0, downcast='infer').sum()
    )
    
    with10_or_morebillion_marketcap = df.loc[(pd.to_numeric(df['Market Cap'], errors='coerce').fillna(0, downcast='infer') > 10000000000)]
    stats['percentage_of_board_seats_occupied_by_women_for_issuers_with10_or_morebillion_marketcap'] = calculate_percentage(
        pd.to_numeric(with10_or_morebillion_marketcap["Q.19.1"], errors='coerce').fillna(0, downcast='infer').sum(),
        pd.to_numeric(with10_or_morebillion_marketcap["Q.19.3"], errors='coerce').fillna(0, downcast='infer').sum()
    )

    return stats

def calculate_executive_stats(df):
    stats = {}
    
    stats['percentage_of_Issuers_with_at_least_one_woman_exec'] = calculate_percentage(
        (pd.to_numeric(df.loc[(df["Q.23.1"].isin(["Disclosed both the number and percentage of women in executive officer positions", 
                                                "Disclosed the number of women in executive officer positions"]))]["Q.24.1"], errors='coerce').fillna(0) > 0).sum() + 
        (pd.to_numeric(df.loc[(df["Q.23.1"] == "Disclosed the percentage of women in executive officer positions")]["Q.24.2"], errors='coerce').fillna(0) > 0).sum(),
        df.shape[0] - df.loc[df["Q.23.1"] == "Neither disclosed"].shape[0]
    )
    
    stats['percentage_of_issuers_with_woman_ceo'] = calculate_percentage(
        df.loc[df["Q.21.1"] == "Y"].shape[0],
        df.shape[0]
    )
    
    stats['percentage_of_issuers_with_woman_cfo'] = calculate_percentage(
        df.loc[df["Q.22.1"] == "Y"].shape[0],
        df.shape[0]
    )
        
    return stats

def calculate_policy_stats(df):
    stats = {}
    
    stats['percentage_of_issuer_policy_board_women'] = calculate_percentage(
        df.loc[(df["Q.12.1"].isin(["Issuer has a written policy", "Policy exists but unwritten or unclear if written"]))].shape[0],
        df.shape[0]
    )
    
    stats['percentage_of_without_issuer_policy_board_women'] = calculate_percentage(
        df.loc[(-df["Q.12.1"].isin(["Issuer has a written policy", "Policy exists but unwritten or unclear if written"]))].shape[0],
        df.shape[0]
    )
    
    stats['percentage_of_issuer_targets_board'] = calculate_percentage(
        df.loc[(df["Q.13.1"] == "Women on board targets adopted")].shape[0],
        df.shape[0]
    )
    
    stats['percentage_of_issuer_without_targets_board'] = calculate_percentage(
        df.loc[(df["Q.13.1"] != "Women on board targets adopted")].shape[0],
        df.shape[0]
    )
    
    
    stats['percentage_of_issuer_targets_exec'] = calculate_percentage(
        df.loc[(df["Q.15.1"] == "Women in executive officer positions target adopted")].shape[0],
        df.shape[0]
    )
    
    stats['percentage_of_issuer_terms_limit_director'] = calculate_percentage(
        df.loc[(df["Q.9.1"] == "Director term limits (alone or with other mechanisms)")].shape[0],
        df.shape[0]
    )
    
    stats['percentage_agentenure_limit'] = calculate_percentage(
        df.loc[(df["Q.9.1"] == "Director term limits (alone or with other mechanisms)") & 
               (df["Q.10.1"] == "Term limit related to age") & 
               (df["Q.10.2"] == "Term limit related to tenure")].shape[0],
        df.loc[(df["Q.9.1"] == "Director term limits (alone or with other mechanisms)")].shape[0]
    )
    
    stats['percentage_age_limit'] = calculate_percentage(
        df.loc[(df["Q.9.1"] == "Director term limits (alone or with other mechanisms)") & 
               (df["Q.10.1"] == "Term limit related to age") & (df["Q.10.2"] != "Term limit related to tenure")].shape[0],
        df.loc[(df["Q.9.1"] == "Director term limits (alone or with other mechanisms)")].shape[0]
    )
    
    stats['percentage_tenure'] = calculate_percentage(
        df.loc[(df["Q.9.1"] == "Director term limits (alone or with other mechanisms)") & 
               (df["Q.10.1"] != "Term limit related to age") & (df["Q.10.2"] == "Term limit related to tenure")].shape[0],
        df.loc[(df["Q.9.1"] == "Director term limits (alone or with other mechanisms)")].shape[0]
    )
    
    stats['percentage_women_on_board_with_term_limits'] = calculate_percentage(
        pd.to_numeric(df.loc[df["Q.9.1"] == "Director term limits (alone or with other mechanisms)"]["Q.19.1"],  errors='coerce').fillna(0, downcast='infer').sum(),
        pd.to_numeric(df.loc[df["Q.9.1"] == "Director term limits (alone or with other mechanisms)"]["Q.19.3"],  errors='coerce').fillna(0, downcast='infer').sum()
    )
    
    stats['percentage_women_on_board_without_term_limits'] = calculate_percentage(
        pd.to_numeric(df.loc[df["Q.9.1"] != "Director term limits (alone or with other mechanisms)"]["Q.19.1"],  errors='coerce').fillna(0, downcast='infer').sum(),
        pd.to_numeric(df.loc[df["Q.9.1"] != "Director term limits (alone or with other mechanisms)"]["Q.19.3"],  errors='coerce').fillna(0, downcast='infer').sum()
    )
    
    stats['percentage_of_issuer_adopted_other_mechanisms_of_board_renawal'] = calculate_percentage(
        df.loc[(df["Q.9.1"] == "Other mechanisms for board renewal only")].shape[0],
        df.shape[0]
    )
    
    stats['percentage_of_issuers_did_not_adopt_director_term_limits'] = calculate_percentage(
        df.loc[(df["Q.9.1"] == "No director term limits or other mechanisms")].shape[0],
        df.shape[0]
    )
    
    stats['percentage_of_issuers_with_targets_board_seats_women'] = calculate_percentage(
        pd.to_numeric(df.loc[(df["Q.13.1"] == "Women on board targets adopted")]["Q.19.1"], errors='coerce').fillna(0, downcast='infer').sum(),
        pd.to_numeric(df.loc[(df["Q.13.1"] == "Women on board targets adopted")]["Q.19.3"], errors='coerce').fillna(0, downcast='infer').sum()
    )
    
    stats['percentage_of_issuers_without_targets_board_seats_women'] = calculate_percentage(
        pd.to_numeric(df.loc[(df["Q.13.1"] == "Issuer elected to not adopt targets")]["Q.19.1"], errors='coerce').fillna(0, downcast='infer').sum(),
        pd.to_numeric(df.loc[(df["Q.13.1"] == "Issuer elected to not adopt targets")]["Q.19.3"], errors='coerce').fillna(0, downcast='infer').sum()
    )
    
    stats['percentage_of_issuers_with_policy_board_seats_women'] = calculate_percentage(
        pd.to_numeric(df.loc[(df["Q.12.1"].isin(["Issuer has a written policy", "Policy exists but unwritten or unclear if written"]))]["Q.19.1"], errors='coerce').fillna(0, downcast='infer').sum(),
        pd.to_numeric(df.loc[(df["Q.12.1"].isin(["Issuer has a written policy", "Policy exists but unwritten or unclear if written"]))]["Q.19.3"], errors='coerce').fillna(0, downcast='infer').sum()
    )
   
    
    stats['filled_vacated_board_seats'] = calculate_percentage(
        pd.to_numeric(df["Q.26.2"], errors='coerce').fillna(0, downcast='infer').sum()+ 
        pd.to_numeric(df["Q.26.3"], errors='coerce').fillna(0, downcast='infer').sum(),
        pd.to_numeric(df["Q.26.1"], errors='coerce').fillna(0, downcast='infer').sum()
    )
    
    return stats


def calculate_industry_stats(df):
    industries = ['Mining', 'Oil & Gas', 'Financial Services', 'Technology', 'Real estate', 
                  'Retail', 'Manufacturing', 'Utilities', 'Biotechnology', 'Other']
    
    stats = {industry: {} for industry in industries}
    
    for industry in industries:
        industry_df = df[df["Q.5.1"] == industry]
        
        stats[industry]['percentage_of_Issuers_with_at_least_one_woman_exec'] = calculate_percentage(
            (pd.to_numeric(industry_df.loc[(industry_df["Q.23.1"].isin(["Disclosed both the number and percentage of women in executive officer positions", 
                                                    "Disclosed the number of women in executive officer positions"]))]["Q.24.1"], errors='coerce').fillna(0) > 0).sum() + 
            (pd.to_numeric(industry_df.loc[(industry_df["Q.23.1"] == "Disclosed the percentage of women in executive officer positions")]["Q.24.2"], errors='coerce').fillna(0) > 0).sum(),
            industry_df.shape[0] - industry_df.loc[industry_df["Q.23.1"] == "Neither disclosed"].shape[0]
        )    
        
        stats[industry]['percentage_of_Issuers_with_at_least_one_woman_board'] = calculate_percentage(
            (pd.to_numeric(industry_df["Q.19.1"], errors='coerce').fillna(0) > 0).sum(),
            industry_df['Participant No.'].count()
        )
            
        stats[industry]['Industries in Sample'] = calculate_percentage(
            industry_df['Participant No.'].count(),
            df['Participant No.'].count()
            
        )
        
    return stats

def main(file_path, sheet_name):
    df = load_and_preprocess_data(file_path, sheet_name)
    
    results = {}
    results.update(calculate_board_stats(df))
    results.update(calculate_executive_stats(df))
    results.update(calculate_policy_stats(df))
    results['industry_stats'] = calculate_industry_stats(df)
    
    return results

if __name__ == "__main__":
    file_path = "T:\\Strategy_Operations\\Economics Analysis\\Data Sources folder mirrored on OSCER\Research and Analysis Advisory -Operational\\Corporate Finance\\Women on Boards\\Year 8 Export (FINAL) - FA - Edited.xlsx"
    sheet_name = "2022473846982"
    results = main(file_path, sheet_name)
    
    # Print or process the results as needed
    for key, value in results.items():
        print(f"{key}: {value}")
