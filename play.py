from airflow.hooks.http_hook import HttpHook

result = HttpHook(http_conn_id='test', method='GET').run(endpoint="https://europe-west1-gdd-airflow-training.cloudfunctions.net/airflow-training-transform-valutas?date=2018-10-10&from=GBP&to=USD")

print(str(result))
