FROM apache/airflow:2.10.0-python3.12

# Copia o arquivo de dependências para dentro da imagem
COPY requirements.txt /requirements.txt

# Definição dos argumentos na hora de realizar a instalação
ARG AIRFLOW_VERSION=2.10.0
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"


# Instala as dependências do projeto (groq, psycopg2, dotenv, etc)
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"
