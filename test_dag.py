import urllib
import urllib.parse
import warnings
from datetime import datetime, timedelta

import requests
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from requests.adapters import HTTPAdapter

warnings.filterwarnings("ignore")


def telegram(text):
    try:
        msg = urllib.parse.quote(str(text))
        bot_token = str(Variable.get("etd-not_bot_token"))
        channel_id = str(Variable.get("etd-not_channel_id"))

        adapter = HTTPAdapter()
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        http.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={channel_id}&text={msg}",
            verify=False,
            timeout=10,
        )
    except Exception as err:
        print(f"airflow_test_dag: Ошибка при отправке в telegram -  {err}")


def is_scheduled_run_type(**context):
    return context["dag_run"].run_type == "scheduled"


with DAG(
    dag_id="airflow_test_dag",
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 3, 3, 0),
    catchup=False,
) as dag:
    is_scheduled_run_type_task = ShortCircuitOperator(
        task_id="is_scheduled_run_type",
        python_callable=is_scheduled_run_type,
        ignore_downstream_trigger_rules=False,
    )

    test_task_1 = PythonOperator(
        task_id="telegram_send_1",
        python_callable=telegram,
        op_kwargs={
            "text": "airflow_test_dag: тестовая отправка в телеграм 1",
        },
        provide_context=True,
    )

    test_task_2 = PythonOperator(
        task_id="telegram_send_2",
        python_callable=telegram,
        op_kwargs={
            "text": "airflow_test_dag: тестовая отправка в телеграм 2",
        },
        provide_context=True,
    )

    trigger_airflow_test_dag_2 = TriggerDagRunOperator(
        task_id="trigger_airflow_test_dag_2",
        trigger_dag_id="airflow_test_dag_2",
    )

    (
        test_task_1
        >> test_task_2
        >> is_scheduled_run_type_task
        >> trigger_airflow_test_dag_2
    )

