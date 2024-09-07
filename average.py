import pytz
import requests
import pandas as pd
from io import StringIO
import os
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("TOKEN")

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
        
        color = ['1bf505','#eaf505', '#f50505']
        for col_idx in range(1, 4):
            header_format = workbook.add_format({'bg_color': color[(col_idx - 1) % len(color)]})
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
        
def main():
    df = []
    url = "https://n01.scf488.dynatrace-managed.com/e/97937fef-013b-4e90-acc8-8267cc898592/api/v2/metrics/query?metricSelector=(builtin:service.keyRequest.response.time:splitBy(\"dt.entity.service_method\"):percentile(50):names:sort(dimension(\"dt.entity.service_method.name\",ascending)):limit(700)):limit(700):names&from=-1d/d&to=now/d&resolution=1m&mzSelector=mzId(-413968960818628324)"
    # url_percentile = "https://n01.scf488.dynatrace-managed.com/e/97937fef-013b-4e90-acc8-8267cc898592/api/v2/metrics/query?metricSelector=(builtin:service.keyRequest.response.time:splitBy(\"dt.entity.service_method\"):percentile(50.0):names:sort(dimension(\"dt.entity.service_method.name\",ascending)):limit(700)):limit(700):names,(builtin:service.keyRequest.response.time:splitBy(\"dt.entity.service_method\"):percentile(75.0):names:sort(dimension(\"dt.entity.service_method.name\",ascending)):limit(700)):limit(700):names,(builtin:service.keyRequest.response.time:splitBy(\"dt.entity.service_method\"):percentile(95.0):names:sort(dimension(\"dt.entity.service_method.name\",ascending)):limit(700)):limit(700):names&from=-1d/d&to=now/d&resolution=1m&mzSelector=mzId(-413968960818628324)"

    payload = {}
    headers = {
            "Authorization": f"Api-Token {token}",
            "accept": "text/csv, application/json; q=0.1",
        }

    response = requests.request("GET", url, headers=headers, data=payload, verify=False)
    # response_percentile = requests.request("GET", url_percentile, headers=headers, data=payload, verify=False)
    if response.status_code == 200:
        data = StringIO(response.text)
        df = pd.read_csv(data)
    else:
        print(f"Failed to retrieve data: {response.status_code}")

    df['api_name']=df['dt.entity.service_method.name']
    # df = df.drop('metricId', axis=1)
    df = df.drop('dt.entity.service_method', axis=1)
    df['value'] = round((df['value'] / 1000),2)

    if 'time' in df.columns:
        df['time'] = pd.to_datetime(df['time'])

        utc_timezone = pytz.utc
        wib_timezone = pytz.timezone('Asia/Jakarta')

        df['time'] = df['time'].dt.tz_localize(utc_timezone).dt.tz_convert(wib_timezone)

        df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    df['category'] = df['value'].apply(categorize)

    grouped_df = df.groupby('api_name')['category'].value_counts().unstack(fill_value=0)
    ordered_df = grouped_df[['green', 'yellow', 'red']]

    column = [
        "api_name",
        "time",
        "value",
    ]

    df.to_csv("output.csv", index=False, columns=column)
    ordered_df.to_csv("grouped_output.csv")
    convert_csv("grouped_output.csv", "output.xlsx")
    remove_csv("grouped_output.csv")
    
if __name__ == "__main__":
    print("Running.....")
    main()