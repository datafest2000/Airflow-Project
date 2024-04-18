from datetime import datetime, timedelta
import pandas as pd
import json




def transform_data():
    with open('./opt/airflow/raw/extract2.json', 'r') as json_file:
        response = json.load(json_file)

        print(response)

        rates = []
        timestamp = response['timestamp']

        for rate in response['to']:
            rates.append((timestamp, 'USD', rate['quotecurrency'], rate['mid']))

        data = pd.DataFrame(rates, columns=['timestamp', 'currency_from', 'currency_to', 'rate'])

        data['timestamp'] = data['timestamp'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))

        data.to_csv('./opt/airflow/raw/data.csv', index=False)




   