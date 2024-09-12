import pytz
import requests
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
token = os.getenv("TOKEN")
utc_timezone = pytz.utc
wib_timezone = pytz.timezone('Asia/Jakarta')
from_date_string = '2024-08-26T00:00:00'
to_date_string = '2024-09-11T00:00:00'

def format_date(date):
    init_datetime = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S') # convert string to datetime
    utc_datetime = wib_timezone.localize(init_datetime).astimezone(utc_timezone)
    return utc_datetime.strftime('%Y-%m-%dT%H:%M:%SZ') # convert datetime to string

def request(url):
    headers = {
            "Authorization": f"Api-Token {token}",
            "accept": "text/csv, application/json; q=0.1",
        }
    try:
        response = requests.get(url, headers=headers, verify=False)
        response.raise_for_status()
        print("Success fetching data")
        return pd.read_csv(StringIO(response.text))
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    
    return pd.DataFrame()

def fetch_count():
    from_date_formatted = format_date(from_date_string)
    to_date_formatted = format_date(to_date_string)
    url = f"https://n01.scf488.dynatrace-managed.com/e/97937fef-013b-4e90-acc8-8267cc898592/api/v2/metrics/query?metricSelector=(builtin:service.keyRequest.count.total:splitBy(\"dt.entity.service_method\"):sum:names:sort(dimension(\"dt.entity.service_method.name\",ascending)):limit(700)):limit(700):names&from={from_date_formatted}&to={to_date_formatted}&resolution=1d&mzSelector=mzId(-413968960818628324)"
    df = request(url)
    return df
    
def fetch_error():
    from_date_formatted = format_date(from_date_string)
    to_date_formatted = format_date(to_date_string)
    url = f"https://n01.scf488.dynatrace-managed.com/e/97937fef-013b-4e90-acc8-8267cc898592/api/v2/metrics/query?metricSelector=(builtin:service.keyRequest.errors.server.rate:splitBy(\"dt.entity.service_method\"):sum:names:sort(dimension(\"dt.entity.service_method.name\",ascending)):limit(700)):limit(700):names&from={from_date_formatted}&to={to_date_formatted}&resolution=1d&mzSelector=mzId(-413968960818628324)"
    df = request(url)
    return df

def fetch_data(percentile):
    from_date_formatted = format_date(from_date_string)
    to_date_formatted = format_date(to_date_string)
    url = f"https://n01.scf488.dynatrace-managed.com/e/97937fef-013b-4e90-acc8-8267cc898592/api/v2/metrics/query?metricSelector=(builtin:service.keyRequest.response.time:splitBy(\"dt.entity.service_method\"):percentile({percentile}):names:sort(dimension(\"dt.entity.service_method.name\",ascending)):limit(700)):limit(700):names&from={from_date_formatted}&to={to_date_formatted}&resolution=1d&mzSelector=mzId(-413968960818628324)"
    df = request(url)
    return df
    
def convert_csv(csv_name, excel_name):
    df_csv = pd.read_csv(csv_name, sep=',', header=0)
    from_date_formatted = datetime.strptime(from_date_string, '%Y-%m-%dT%H:%M:%S').date() # change format to date %Y-%m-%d
    to_date_formatted = datetime.strptime(to_date_string, '%Y-%m-%dT%H:%M:%S').date()
    with pd.ExcelWriter(excel_name, engine='xlsxwriter') as writer:
        df_csv.to_excel(writer, index=False, sheet_name='Sheet1', startrow=1)
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        worksheet.write('A1', f"Date Range: {from_date_formatted} - {to_date_formatted}")
        
        formats = {
            'green': workbook.add_format({'bg_color': '#66ff66'}),
            'yellow': workbook.add_format({'bg_color': '#ffff66'}),
            'red': workbook.add_format({'bg_color': '#ff6666'}),
            'wrap': workbook.add_format({'align': 'center', 'border': 1})
        }
        
        column = ['C', 'D', 'E']
        for col in column:
            worksheet.conditional_format(f'{col}3:{col}{len(df_csv)+2}', {'type': 'cell', 'criteria': 'between', 'minimum': 0, 'maximum': 999, 'format': formats['green']}) # column excel c start from row 3 until length of dataframe + 2 cause header and title eg: if u have 100 rows then start from 3 until 102
            worksheet.conditional_format(f'{col}3:{col}{len(df_csv)+2}', {'type': 'cell', 'criteria': 'between', 'minimum': 1000, 'maximum': 1999, 'format': formats['yellow']})
            worksheet.conditional_format(f'{col}3:{col}{len(df_csv)+2}', {'type': 'cell', 'criteria': 'greater than', 'value': 1999, 'format': formats['red']})

        worksheet.set_column(0, 1, 50, formats['wrap']) # For First Column
        worksheet.set_column(2, len(df_csv.columns) - 1, 20, formats['wrap']) # For Rest of Column
            
def remove_csv(csv_name):
    try:
        os.remove(csv_name)
        print(f"{csv_name} has been removed.")
    except FileNotFoundError:
        print(f"{csv_name} not found.")
    except Exception as e:
        print(f"Error occurred while trying to remove {csv_name}: {e}")
    
def process_df(df, method):
    df['api_name']=df['dt.entity.service_method.name']
    df = df.drop(['metricId', 'dt.entity.service_method', 'dt.entity.service_method.name'], axis=1)
    
    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time']).dt.tz_localize(utc_timezone).dt.tz_convert(wib_timezone) # convert utc to wib
        df['time'] = df['time'].dt.strftime('%Y-%m-%d')
    
    if method == 'percentile':
        df['value'] = round((df['value'] / 1000), 2)
        agg_func = 'mean'
        result_column = 'Percentile'
    elif method == 'total_request':
        agg_func = 'sum'
        result_column = 'Total Request'
    elif method == 'error_rate':
        agg_func = 'mean'
        result_column = 'Error Rate'
    grouped_df = df.groupby(['api_name', 'time']).agg(
        **{result_column:('value', agg_func)}
    ).reset_index() # using ** for dynamic keyword
    return grouped_df

def export(df, csv_name, xlsx_name):
    df.to_csv(csv_name, index=False)
    convert_csv(csv_name, xlsx_name)
    remove_csv(csv_name)

def main():
    percentiles = [50.0, 70.0, 90.0]
    percentile_dfs = {}
    
    for p in percentiles:
        percentile_dfs[p] = process_df(fetch_data(p), 'percentile')

    df_count_percent = process_df(fetch_count(), 'total_request')
    df_count_err = process_df(fetch_error(), 'error_rate')
    
    df_merged = percentile_dfs[50.0]
    df_merged = pd.merge(df_merged, percentile_dfs[70.0], on=['time', 'api_name'], how='outer', suffixes=(' 50', ' 70'))
    df_merged = pd.merge(df_merged, percentile_dfs[90.0], on=['time', 'api_name'], how='outer')
    df_merged = pd.merge(df_merged, df_count_percent, on=['time', 'api_name'], how='left')
    df_merged = pd.merge(df_merged, df_count_err, on=['time', 'api_name'], how='left')
    
    df_merged.rename(columns={
        'Percentile': 'Percentile 90', 
        'api_name': 'API Name', 
        'time': 'Timestamp'
    }, inplace=True)
    df_merged['Error Rate'] = round(df_merged['Error Rate'], 2)
    df_merged['Error Rate'] = df_merged['Error Rate'].apply(lambda x: f"{int(x)} %" if isinstance(x, (float, int)) and x.is_integer() else (f"{x:.2f} %" if pd.notna(x) else x))
    df_merged = df_merged[['Timestamp', 'API Name', 'Percentile 50', 'Percentile 70', 'Percentile 90', 'Error Rate','Total Request']]
    
    export(df_merged,"output_percentile_merge_interval_1day.csv","output_percentile_merge_interval_1day.xlsx")
  
if __name__ == "__main__":
    print("Running.....")
    main()