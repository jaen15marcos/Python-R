import requests
import pandas as pd
import io
import gzip
import tempfile
from io import StringIO
from bs4 import BeautifulSoup
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Define all functions

def fetch_and_process_treasury_data(start_year, end_year):
    def fetch_treasury_data(year):
        url = f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/TextView?type=daily_treasury_yield_curve&field_tdr_date_value={year}"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find("table")
        if table:
            return pd.read_html(StringIO(table.prettify()))[0]
        return None

    all_data = []
    for year in range(start_year, end_year + 1):
        print(f"Fetching data for year {year}...")
        rates = fetch_treasury_data(year)
        if rates is not None:
            rates['Year'] = year  # Add a column for the year
            all_data.append(rates)
        else:
            print(f"No data found for year {year}")

    if all_data:
        rates_df = pd.concat(all_data, ignore_index=True)
        
        # Convert 'Date' column to datetime
        rates_df['Date'] = pd.to_datetime(rates_df['Date'])
        
        # Sort the DataFrame by date
        rates_df = rates_df.sort_values('Date')
        
        # Reset the index
        rates_df = rates_df.reset_index(drop=True)
        
        # Delete cols where all elements are NaN
        rates_df.dropna(axis=1, how='all', inplace=True)
        
        print("Rates DataFrame:")
        print(rates_df.head())
        
        return rates_df
    else:
        print("No data was collected.")
        return None

def process_fred_data(url, start_date='2014-01-01', end_date='2024-07-01'):
    """
    Fetches data from a FRED URL, processes it, and returns a daily resampled DataFrame.
    
    Parameters:
    url (str): The URL of the FRED CSV file.
    start_date (str): The start date for filtering (default: '2014-01-01').
    end_date (str): The end date for filtering (default: '2024-07-01').
    
    Returns:
    pandas.DataFrame: Processed and resampled DataFrame.
    """
    try:
        # Download the CSV file
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad responses
        
        # Read the CSV data directly from the response content
        df = pd.read_csv(StringIO(response.text))
        
        # Rename column names for consistency
        df = df.rename(columns={df.columns[0]: 'Date'})
        
        # Convert 'Date' column to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Set Date as the index
        df.set_index('Date', inplace=True)
        
        # Resample to daily frequency and forward fill the values
        df = df.resample('D').ffill()
        
        # Reset the index to make Date a column again
        df.reset_index(inplace=True)
        
        # Filter for the specified date range
        df = df[(df['Date'] >= start_date) & (df['Date'] <= end_date)]
        
        return df
    
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except pd.errors.EmptyDataError:
        print("The CSV file is empty.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def read_github_gz_csv(repo_owner, repo_name, file_path, branch='main'):
    """
    Read a gzipped CSV file from a GitHub repository and return it as a pandas DataFrame.
    
    Parameters:
    repo_owner (str): The owner of the GitHub repository
    repo_name (str): The name of the GitHub repository
    file_path (str): The path to the file within the repository
    branch (str): The branch to use (default is 'main')
    
    Returns:
    pandas.DataFrame: The data from the CSV file
    """
    # Construct the URL for the raw file
    url = f"https://github.com/{repo_owner}/{repo_name}/raw/{branch}/{file_path}"
    
    try:
        # Send a GET request to the URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad responses
        
        # Decompress the gzipped content
        with gzip.open(io.BytesIO(response.content), 'rt') as f:
            # Read the CSV data into a pandas DataFrame
            df = pd.read_csv(f)
        
        return df
    
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")
        return None
    except pd.errors.EmptyDataError:
        print("The CSV file is empty.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def fetch_economic_calendar(start_year, end_year, end_month=None):
    url = 'https://economic-calendar.tradingview.com/events'
    headers = {
        'Origin': 'https://in.tradingview.com'
    }

    all_data = []

    for year in range(start_year, end_year + 1):
        start_date = pd.Timestamp(year, 1, 1)

        if year == end_year and end_month:
            end_date = pd.Timestamp(year, end_month, 1) + pd.offsets.MonthEnd(1)
        else:
            end_date = pd.Timestamp(year, 12, 31)

        payload = {
            'from': start_date.isoformat() + '.000Z',
            'to': end_date.isoformat() + '.000Z',
            'countries': ','.join(['US', 'IN']),
            'minImportance': 1 #only high importance events
        }

        response = requests.get(url, headers=headers, params=payload)
        if response.status_code == 200:
            data = response.json()
            if 'result' in data:
                df = pd.DataFrame(data['result'])
                all_data.append(df)
                print(f"Data fetched for year {year}")
                time.sleep(2)
            else:
                print(f"No results found for year {year}")
        else:
            print(f"Failed to fetch data for year {year}. Status code: {response.status_code}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()

def read_online_csv(file_path):
    """
    Read a CSV file from a website and return it as a pandas DataFrame.
    
    Parameters:
    file_path (str): The path to the file within the repository

    Returns:
    pandas.DataFrame: The data from the CSV file
    """
    df = pd.read_csv(file_path)
    
    #Covid DF
    if 'iso_code' in  df.columns.to_list():
        # Filter for USA data
        df = df.loc[df['iso_code']=='USA']

        # Get Desired Columns
        df = df[['date', 'total_cases_per_million','new_cases_per_million','total_deaths_per_million',
               'new_deaths_per_million']]
        # Rename column names for consistency
        df = df.rename(columns={df.columns[0]: 'Date'})
        
        # Convert 'Date' column to datetime
        df['Date'] = pd.to_datetime(df['Date'])
        
        return df

def scrape_wikipedia_table(url: str, table_index: int = 0):
    """
    Scrape a table from a Wikipedia page.

    Parameters:
    url (str): The URL of the Wikipedia page.
    table_index (int): The index of the table to scrape (default is 0, i.e., the first table).

    Returns:
    pd.DataFrame: The scraped table as a pandas DataFrame, or None if the scraping fails.
    """
    try:
        # Fetch the page content
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for bad responses

        # Read all tables from the page
        tables = pd.read_html(response.text)

        # Check if the requested table index exists
        if table_index < len(tables):
            return tables[table_index]
        else:
            print(f"Table index {table_index} is out of range. The page contains {len(tables)} tables.")
            return None

    except requests.RequestException as e:
        print(f"Error fetching the page: {e}")
    except ValueError as e:
        print(f"Error parsing HTML tables: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    return None

def collect_data():
    try:
        logger.info("Starting data collection...")

        # Treasury rates
        rates_df = fetch_and_process_treasury_data(start_year=2014, end_year=2024)

        # FRED data
        rgdp = process_fred_data('https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=GDP&scale=left&cosd=1947-01-01&coed=2024-01-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2024-07-17&revision_date=2024-07-17&nd=1947-01-01')
        vix = process_fred_data('https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv')
        gdpnow = process_fred_data('https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=GDPNOW&scale=left&cosd=2011-07-01&coed=2024-04-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Quarterly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2024-07-18&revision_date=2024-07-18&nd=2011-07-01')
        sp500_returns = process_fred_data('https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=SP500&scale=left&cosd=2014-07-18&coed=2024-07-17&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Daily%2C%20Close&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2024-07-18&revision_date=2024-07-18&nd=2014-07-18')
        cpi = process_fred_data('https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1318&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id=MEDCPIM158SFRBCLE&scale=left&cosd=1983-01-01&coed=2024-06-01&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Monthly&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2024-07-18&revision_date=2024-07-18&nd=1983-01-01')

        # GitHub data
        repo_owner, repo_name = "jaen15marcos", "Python-R"
        sp500_mkt_data = read_github_gz_csv(repo_owner, repo_name, "Realized_Vol_Models/sp500_historical_market_data.gz")
        sp500_pricing_data = read_github_gz_csv(repo_owner, repo_name, "Realized_Vol_Models/sp500_historical_prices.gz")
        sp500_earning_dates_data = read_github_gz_csv(repo_owner, repo_name, "Realized_Vol_Models/earning_dates.gz")
        refinitiv_identifiers = read_github_gz_csv(repo_owner, repo_name, "Realized_Vol_Models/rics")

        # Economic calendar
        start_year, end_year, end_month = 2014, 2024, 7
        economic_calendar = fetch_economic_calendar(start_year, end_year, end_month)

        # COVID data
        covid = read_online_csv('https://covid.ourworldindata.org/data/owid-covid-data.csv')

        # S&P 500 constituents
        sp500_constituents = scrape_wikipedia_table("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")

        logger.info("Data collection completed successfully.")

        return {
            'rates': rates_df,
            'rgdp': rgdp,
            'vix': vix,
            'gdpnow': gdpnow,
            'sp500_returns': sp500_returns,
            'cpi': cpi,
            'sp500_mkt_data': sp500_mkt_data,
            'sp500_pricing_data': sp500_pricing_data,
            'sp500_earning_dates_data': sp500_earning_dates_data,
            'refinitiv_identifiers': refinitiv_identifiers,
            'economic_calendar': economic_calendar,
            'covid': covid,
            'sp500_constituents': sp500_constituents
        }

    except Exception as e:
        logger.error(f"An error occurred during data collection: {str(e)}")
        raise

if __name__ == "__main__":
    collected_data = collect_data()
    
