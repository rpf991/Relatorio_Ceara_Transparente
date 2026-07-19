# Relatorio_Ceara_Transparente

# Sobre o Projeto

O projeto consiste na automação de um pipeline de dados orquestrados pelo Apache Airflow que consome e analisa os contratos públicos da API do Ceará Transparente. O objetivo principal é extrair os registros de contratos nas ultimas duas semanas, armazená-los no PostegreSQL, e utilizar um modelo de Large Language Model (LLM) para classificar semanticamente os 30 registros com os maiores valores financeiros. A solução poderá contribuir
para agilizar a análise das áreas que recebem os maiores investimentos públicos, gerando um relatório automatizado em HTML


# 1) Arquitetura do Pipeline

```mermaid
graph TD
    A[API Ceará Transparente] -->|01. Extração de Dados| B(Task: contratos_api)
    B -->|02. Persistência Bruta| C[(PostgreSQL: Tabela contratos_api)]
    C -->|03. Consulta 30 maiores| D(Task: Classificação com LLM)
    D -->|04. Persistência Enriquecida| E[(PostgreSQL: Tabela contratos_classificados)]
    E -->|05. Geração de Relatório| F[Relatório Final HTML]

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#bbf,stroke:#333,stroke-width:2px
    style E fill:#bbf,stroke:#333,stroke-width:2px
    style F fill:#bfb,stroke:#333,stroke-width:2px
```

# 2) Pré-Requisitos

    Antes de Inciar certifique-se de ter instalado na sua máquina:
    - Docker Desktop (https://www.docker.com/products/docker-desktop/)
    - PostegreSQL (https://www.postgresql.org/download/)
    - Uma API de serviço de LLM configurada (OpenAI, Groq, Gemini, etc.)



## Preparação no Docker

Caso seu container ainda não esteja criado no Docker Desktop,
dentro do terminal do VSCODE execute os seguintes comandos

1) **Criação dos Containers**

    **docker compose up --build -d**
    - Realiza a criação de um Docker Compose contendo os serviços
      a serem utilizados na execução do projeto, além de excutar o 
      documento que instala as dependênciais esperada para a dag rodar

2) **Verificar se os containers estão funcionando**
  
    **docker compose ps**
    - Avaliar os status de cada container no Docker Compose

    **docker compose logs -f**
    - Analisa os logs em tempo real


3) **Verificar se está funcionando**

    Acesse o site localhost:8080 para entrar na interface do Airflow Webserver.
    Caso a DAG demore para aparecer na listagem, realize os seguintes comandos no terminal:

    -**docker compose down** (remoção dos containers)
    - **docker compose up -d** (inserção dos mesmos containers)

    Esses comandos reiniciam os containers, forçando o scheduler do Airflow a ler
    a pasta de Dags novamente e atualizar a interface


4) **Comandos para manutenção**
   
    **docker compose stop**
    - Encerra as atividades do docker compose sem remover os dados

    **docker compose down**
    - Para e remove todos os containers e redes criados pelo projeto

    **docker compose start**
    * Inicia os containers que estavam pausados
