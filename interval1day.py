import pytz
import requests
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv
# import re
from datetime import datetime

load_dotenv()
token = os.getenv("TOKEN")
utc_timezone = pytz.utc
wib_timezone = pytz.timezone('Asia/Jakarta')
from_date_string = '2024-08-26T00:00:00'
to_date_string = '2024-09-11T00:00:00'

def format_date(date):
    init_datetime = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S') # convert string to datetime
    wib_datetime = wib_timezone.localize(init_datetime)
    utc_datetime = wib_datetime.astimezone(utc_timezone)
    formatted_date = utc_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') # convert datetime to string
    return formatted_date

def fetch_count():
    from_date_formatted = format_date(from_date_string)
    to_date_formatted = format_date(to_date_string)
    url = f"https://n01.scf488.dynatrace-managed.com/e/97937fef-013b-4e90-acc8-8267cc898592/api/v2/metrics/query?metricSelector=(builtin:service.keyRequest.count.total:splitBy(\"dt.entity.service_method\"):sum:names:sort(dimension(\"dt.entity.service_method.name\",ascending)):limit(700)):limit(700):names&from={from_date_formatted}&to={to_date_formatted}&resolution=1d&mzSelector=mzId(-413968960818628324)"
    payload = {}
    headers = {
            "Authorization": f"Api-Token {token}",
            "accept": "text/csv, application/json; q=0.1",
        }
    
    response = requests.request("GET", url, headers=headers, data=payload, verify=False)
    if response.status_code == 200:
        data = StringIO(response.text)
        print("Success fetching data")
        return pd.read_csv(data)
    else:
        print(f"Failed to retrieve data from {url}: {response.status_code}")
        return pd.DataFrame()

def fetch_data(percentile):
    from_date_formatted = format_date(from_date_string)
    to_date_formatted = format_date(to_date_string)
    url = f"https://n01.scf488.dynatrace-managed.com/e/97937fef-013b-4e90-acc8-8267cc898592/api/v2/metrics/query?metricSelector=(builtin:service.keyRequest.response.time:splitBy(\"dt.entity.service_method\"):percentile({percentile}):names:sort(dimension(\"dt.entity.service_method.name\",ascending)):limit(700)):limit(700):names&from={from_date_formatted}&to={to_date_formatted}&resolution=1d&mzSelector=mzId(-413968960818628324)"
    payload = {}
    headers = {
            "Authorization": f"Api-Token {token}",
            "accept": "text/csv, application/json; q=0.1",
        }
    
    response = requests.request("GET", url, headers=headers, data=payload, verify=False)
    if response.status_code == 200:
        data = StringIO(response.text)
        print("Success fetching data")
        return pd.read_csv(data)
    else:
        print(f"Failed to retrieve data from {url}: {response.status_code}")
        return pd.DataFrame()
    
def convert_csv(csv_name, excel_name):
    df_csv = pd.read_csv(csv_name, sep=',', header=0)
    with pd.ExcelWriter(excel_name, engine='xlsxwriter') as writer:
        df_csv.to_excel(writer, index=False, sheet_name='Sheet1', startrow=1)
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        worksheet.write('A1', f"Date Range: {from_date_string.replace('T', ' ')} - {to_date_string.replace('T', ' ')}")
        
        green_format = workbook.add_format({'bg_color': '#66ff66'})
        yellow_format = workbook.add_format({'bg_color': '#ffff66'})
        red_format = workbook.add_format({'bg_color': '#ff6666'})
        column = ['C', 'D', 'E']
        for col in column:
            worksheet.conditional_format(f'{col}3:{col}{len(df_csv)+2}', 
                                         {'type': 'cell', 'criteria': 'between', 'minimum': 0, 'maximum': 999, 'format': green_format}) # column excel c start from row 3 until length of dataframe + 2 cause header and title eg: if u have 100 rows then start from 3 until 102
            worksheet.conditional_format(f'{col}3:{col}{len(df_csv)+2}', 
                                         {'type': 'cell', 'criteria': 'between', 'minimum': 1000, 'maximum': 1999, 'format': yellow_format})
            worksheet.conditional_format(f'{col}3:{col}{len(df_csv)+2}', 
                                         {'type': 'cell', 'criteria': 'greater than', 'value': 1999, 'format': red_format})

        wrap_format = workbook.add_format({'align': 'center', 'border': 1})
        worksheet.set_column(0, 1, 50, wrap_format) # For First Column
        for col_idx in range(2, len(df_csv.columns)):
            worksheet.set_column(col_idx, col_idx, 20, wrap_format) # For Rest of Column
            
def remove_csv(csv_name):
    try:
        os.remove(csv_name)
        print(f"{csv_name} has been removed.")
    except FileNotFoundError:
        print(f"{csv_name} not found.")
    except Exception as e:
        print(f"Error occurred while trying to remove {csv_name}: {e}")
    
def process_df(df, flags):
    df['api_name']=df['dt.entity.service_method.name']
    df = df.drop('metricId', axis=1)
    df = df.drop('dt.entity.service_method', axis=1)
    df = df.drop('dt.entity.service_method.name', axis=1)
    
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time']) # Parse string date to datetime
        df['time'] = df['time'].dt.tz_localize(utc_timezone).dt.tz_convert(wib_timezone) # convert utc to wib
        df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    if flags is True:
        df['value'] = round((df['value'] / 1000),2)
        grouped_df = df.groupby(['api_name', 'time']).agg(
            Percentile=('value', 'mean')
        ).reset_index()
        return grouped_df
    else:
        grouped_df = df.groupby(['api_name', 'time']).agg(
            total_request=('value', 'sum')
        ).reset_index()
        return grouped_df

def export(df, csv_name, xlsx_name):
    df.to_csv(csv_name, index=False)
    convert_csv(csv_name, xlsx_name)
    remove_csv(csv_name)

def main():
    df_percentile_50 = fetch_data(50.0)
    df_percentile_75 = fetch_data(75.0)
    df_percentile_95 = fetch_data(95.0)
    df_percentile_count = fetch_count()

    df_50 = process_df(df_percentile_50, True)
    df_75 = process_df(df_percentile_75, True)
    df_95 = process_df(df_percentile_95, True)
    df_count = process_df(df_percentile_count, False)
    
    df_merged = pd.merge(df_50, df_75, on=['time', 'api_name'], how='outer', suffixes=(' 50', ' 75'))
    df_merged = pd.merge(df_merged, df_95, on=['time', 'api_name'], how='outer')
    df_merged = pd.merge(df_merged, df_count, on=['time', 'api_name'], how='left')
    
    df_merged.rename(columns={'Percentile': 'Percentile 95'}, inplace=True)
    df_merged.rename(columns={'api_name': 'API Name'}, inplace=True)
    df_merged.rename(columns={'total_request': 'Total Request'}, inplace=True)
    df_merged.rename(columns={'time': 'Timestamp'}, inplace=True)
    df_merged = df_merged[['Timestamp', 'API Name', 'Percentile 50', 'Percentile 75', 'Percentile 95', 'Total Request']]
    
    export(df_merged,"output_percentile_merge1D.csv","output_percentile_merge1D.xlsx")
  
if __name__ == "__main__":
    print("Running.....")
    main()