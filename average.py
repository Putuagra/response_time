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
from_date_string = '2024-09-08T00:00:00'
to_date_string = '2024-09-09T00:00:00'

def categorize(value):
    if 1000 < value < 2000:
        return 'yellow'
    elif value < 1000:
        return 'green'
    elif value > 2000:
        return 'red'
    else:
        return
    
def convert_csv(csv_name, excel_name):
    df_csv = pd.read_csv(csv_name, sep=',', header=0)
    with pd.ExcelWriter(excel_name, engine='xlsxwriter') as writer:
        df_csv.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']
        
        color = ['66ff66','#ffff66', '#ff6666']
        for col_idx in range(1, 4):
            header_format = workbook.add_format({'bg_color': color[(col_idx - 1) % len(color)], 'align': 'center'})
            worksheet.write(0, col_idx, df_csv.columns[col_idx], header_format)

        wrap_format = workbook.add_format({'align': 'center'})
        worksheet.set_column(0, 0, 40, wrap_format)
        for col_idx in range(1, len(df_csv.columns)):
            worksheet.set_column(col_idx, col_idx, 10, wrap_format)
            
def remove_csv(csv_name):
    try:
        os.remove(csv_name)
        print(f"{csv_name} has been removed.")
    except FileNotFoundError:
        print(f"{csv_name} not found.")
    except Exception as e:
        print(f"Error occurred while trying to remove {csv_name}: {e}")
        
def format_date(date):
    init_datetime = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S')
    wib_datetime = wib_timezone.localize(init_datetime)
    utc_datetime = wib_datetime.astimezone(utc_timezone)
    formatted_date = utc_datetime.strftime('%Y-%m-%dT%H:%M:%SZ')
    return formatted_date

def fetch_data():
    from_date_formatted = format_date(from_date_string)
    to_date_formatted = format_date(to_date_string)
    url = f"https://n01.scf488.dynatrace-managed.com/e/97937fef-013b-4e90-acc8-8267cc898592/api/v2/metrics/query?metricSelector=(builtin:service.keyRequest.response.time:splitBy(\"dt.entity.service_method\"):avg:names:sort(dimension(\"dt.entity.service_method.name\",ascending)):limit(700)):limit(700):names&from={from_date_formatted}&to={to_date_formatted}&resolution=1m"
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

def process_df(df):
    df['api_name']=df['dt.entity.service_method.name']
    # df['id'] = df['metricId'].apply(extract_percentile)
    # df = df.drop('metricId', axis=1)
    # df = df.drop('dt.entity.service_method', axis=1)
    df['value'] = round((df['value'] / 1000),2)

    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])
        df['time'] = df['time'].dt.tz_localize(utc_timezone).dt.tz_convert(wib_timezone)
        df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df['category'] = df['value'].apply(categorize)

    grouped_df = df.groupby('api_name')['category'].value_counts().unstack(fill_value=0)
    ordered_df = grouped_df[['green', 'yellow', 'red']]
    return ordered_df

def export(df, csv_name, xlsx_name):
    df.to_csv(csv_name)
    convert_csv(csv_name, xlsx_name)
    remove_csv(csv_name)
        
def main():
    df = fetch_data()
    df_avg = process_df(df)
    export(df_avg,"output.csv","output.xlsx")
    
if __name__ == "__main__":
    print("Running.....")
    main()