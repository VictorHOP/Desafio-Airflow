import base64
from airflow.models import Variable

def export_final_answer():
    with open('/mnt/c/Users/victo/Desktop/clone/airflow_tooltorial/airflow-data/dags/count.txt') as f:
        count = f.readlines()[0]

    my_email = "victor.pereira@indicium.tech"  # Substitua pela variável Airflow se estiver testando no Airflow
    message = my_email + count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open('/mnt/c/Users/victo/Desktop/clone/airflow_tooltorial/airflow-data/dags/final_output.txt', 'w') as f:
        f.write(base64_message)

export_final_answer()
print("Arquivo final_output.txt gerado com sucesso!")