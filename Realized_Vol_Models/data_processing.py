#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 17:14:52 2024

@author: lourdescortes
"""

#Ticker Level Information



#Market Controls
import pandas as pd
import data_collection
from functools import reduce



def merge_ticker_data(data_dict):
    # Merge refinitiv_identifiers with sp500_pricing_data
    sp500_pricing_data = pd.merge(data_dict['sp500_pricing_data'],
                                  data_dict['refinitiv_identifiers'][["Instrument", "Constituent Name"]],
                                  on='Instrument', how='outer')
    
    # Update data_dict
    data_dict = {k: v for k, v in data_dict.items() if k not in ['refinitiv_identifiers', 'sp500_pricing_data']}
    data_dict['sp500_pricing_data'] = sp500_pricing_data

    def process_df(df):
        if 'Instrument' not in df.columns and 'Instrument' in df.index.names:
            df = df.reset_index()
        date_cols = [col for col in df.columns if col.startswith('Date')]
        if date_cols:
            df[date_cols] = df[date_cols].apply(pd.to_datetime).apply(lambda x: x.dt.normalize())
            df['Date'] = df[date_cols[0]]
            df = df.drop(columns=[col for col in date_cols if col != 'Date'])
        df = df.drop(columns=[col for col in df.columns if col.startswith('V1') or col.startswith('Unnamed: 0')], errors='ignore')
        return df

    # Filter and process DataFrames
    ticker_data = {k: process_df(v) for k, v in data_dict.items() 
                   if isinstance(v, pd.DataFrame) and 'Instrument' in v.columns}

    if not ticker_data:
        print("No valid DataFrames to merge.")
        return None

    # Merge DataFrames
    merged_df = reduce(lambda left, right: pd.merge(left, right, on=['Instrument', 'Date'], how='outer'), ticker_data.values())
    merged_df = merged_df.sort_values(['Instrument', 'Date'])

    # Fill NA for string columns
    string_cols = merged_df.select_dtypes(include=['object']).columns
    for col in string_cols:
        merged_df[col] = merged_df.groupby('Instrument')[col].transform(lambda x: x.fillna(method='ffill').fillna(method='bfill'))

    return merged_df

refinitiv_data = merge_ticker_data(collected_data)

"""
def merge_all_dataframes(data_dict):
    # Filter out non-DataFrame objects and ensure each DataFrame has a 'Date' column
    dfs = [df.reset_index() if 'Date' not in df.columns else df for key, df in data_dict.items() 
           if isinstance(df, pd.DataFrame) and ('Date' in df.columns or 'Date' in df.index.names)]
    
    # Ensure 'Date' is datetime type in all DataFrames
    for df in dfs:
        df['Date'] = pd.to_datetime(df['Date'])
    
    # Merge all DataFrames
    merged_df = reduce(lambda left, right: pd.merge(left, right, on='Date', how='outer'), dfs)
    
    # Sort by Date
    merged_df = merged_df.sort_values('Date')
    
    return merged_df

# Merge all DataFrames in collected_data
merged_data = merge_all_dataframes(collected_data)

# Display info about the merged DataFrame
print(merged_data.info())

# Display the first few rows of the merged DataFrame
print(merged_data.head())
#Pricing Data
"""
