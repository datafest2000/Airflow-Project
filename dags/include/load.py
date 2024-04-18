import pandas as pd



def load_data(engine, table):

    df = pd.read_csv('./opt/airflow/raw/data.csv')

    df.to_sql(table, engine, if_exists= 'append', index=False)
