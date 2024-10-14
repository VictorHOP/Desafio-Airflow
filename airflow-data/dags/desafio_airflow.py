from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd

# Configurações padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Função para exportar dados da tabela Order para output_orders.csv
def extract_orders():
    conn = sqlite3.connect('/mnt/c/Users/victo/Desktop/clone/airflow_tooltorial/data/Northwind_small.sqlite')
    query = "SELECT * FROM 'Order'"
    orders_df = pd.read_sql(query, conn)
    orders_df.to_csv('/mnt/c/Users/victo/Desktop/clone/airflow_tooltorial/airflow-data/dags/output_orders.csv', index=False)
    conn.close()

# Função para calcular a quantidade total de pedidos com destino ao Rio de Janeiro
def calculate_rio_quantity():
    conn = sqlite3.connect('/mnt/c/Users/victo/Desktop/clone/airflow_tooltorial/data/Northwind_small.sqlite')
    
    # Carrega os dados do arquivo CSV
    orders_df = pd.read_csv('/mnt/c/Users/victo/Desktop/clone/airflow_tooltorial/airflow-data/dags/output_orders.csv')
    order_details_df = pd.read_sql("SELECT * FROM OrderDetail", conn)
    conn.close()
    
    # Converte o tipo da coluna 'Id' para garantir que seja int
    orders_df['Id'] = orders_df['Id'].astype(int)
    
    # Converte o tipo da coluna 'OrderId' para garantir que seja int
    order_details_df['OrderId'] = order_details_df['OrderId'].astype(int)
    
    # Realiza o JOIN entre as tabelas
    merged_df = pd.merge(orders_df, order_details_df, left_on='Id', right_on='OrderId')
    
    # Calcula a soma para Rio de Janeiro
    rio_total = merged_df[merged_df["ShipCity"] == "Rio de Janeiro"]["Quantity"].sum()
    
    # Salva o resultado em count.txt
    with open('/mnt/c/Users/victo/Desktop/clone/airflow_tooltorial/airflow-data/dags/count.txt', 'w') as f:
        f.write(str(rio_total))

# Função para exportar a resposta final, que já está no arquivo
def export_final_answer():
    import base64

    # Importa o valor de count
    with open('/mnt/c/Users/victo/Desktop/clone/airflow_tooltorial/airflow-data/dags/count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email + count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open('/mnt/c/Users/victo/Desktop/clone/airflow_tooltorial/airflow-data/dags/final_output.txt', 'w') as f:
        f.write(base64_message)

# Definindo o DAG
with DAG(
    'Desafio7Airflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    
    # Task 1: Extrair pedidos
    task1 = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders,
    )

    # Task 2: Calcular a quantidade para Rio de Janeiro
    task2 = PythonOperator(
        task_id='calculate_rio_quantity',
        python_callable=calculate_rio_quantity,
    )
    
    # Task 3: Exportar resposta final
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
    )

    # Definindo a ordem das tasks
    task1 >> task2 >> export_final_output
