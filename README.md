# Relatorio_Ceara_Transparente

# Sobre o Projeto

O projeto consiste na automação de um pipeline de dados orquestrados pelo Apache Airflow
que consome e analisa os contratos públicos da API do Ceará Transparente. O objetivo principal
é extrair os registros de contratos nas ultimas duas semanas, armazená-los no PostegreSQL, 
e utilizar um modelo de Large Language Model (LLM) para classificar semanticamente 
os 30 registros com os maiores valores financeiros. A solução poderá contribuir
para agilizar a análise das áreas que recebem os maiores investimentos públicos,
gerando um relatório automatizado em HTML
