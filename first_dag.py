from airflow import DAG
from datetime import datetime
import pandas as pd
import requests
import json
from airflow.operators.python import PythonOperator # operadores | importamos o python, pois estamos criando os comandos com o python
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator

# funcao faz uma chamada a uma API e retorna a quantidade de linhas do DataFrame
def captura_conta_dados():
  url = "https://data.cityofnewyork.us/resource/rc75-m7u3.json"
  response = requests.get(url)
  df = pd.DataFrame(json.loads(response.content))
  qtd = len(df.index)
  return qtd

# Verifica a quantidade de registros no arquivos
def e_valido(ti):
  qtd = ti.xcom_pull(task_ids = 'captura_conta_dados') # usada para compartilhar informacoes entre as tasks
  
  if (qtd > 1000):
    return 'valido'
  return 'nvalido'

# id tem que ser um indentificado unico
tag_id = 'tutorial_dag'

# Data de inicio de execucao
start_date = datetime(2022, 11, 1)

# Periodicidade de execucao da DAG, temos que informar no formato CROM
schedule_interval = '30 * * * *' # 30 em 30 minutos

# Cria a DAG apartir da ultima execucao
catchup = False

with DAG(tag_id, start_date, schedule_interval, catchup) as dag:
  
  # criando uma task
  captura_conta_dados = PythonOperator(
    task_id = 'captura_conta_dados',
    python_callable = captura_conta_dados
  )
  
  # verifica determinada condicao, apartir disso decide pra onde vai
  e_valida = BranchPythonOperator(
    task_id = 'e_valido',
    python_callable = e_valido
  )
  
  valido = BashOperator(
    task_id = 'valido',
    bash_command = "echo 'Quantidade OK'"
  )
    
  nvalido = BashOperator(
    task_id = 'nvalido',
    bash_command = "echo 'Quantidade nao OK'"
  )
  # odem de execucao das tasks
  captura_conta_dados >> e_valida >> [valido, nvalido]