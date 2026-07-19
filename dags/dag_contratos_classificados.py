# Importação de Bibliotecas
import requests
import pandas as pd
import numpy as np
import json
import logging
import os
import re
import time
import pendulum
from groq import Groq
from datetime import date, timedelta, datetime
from dotenv import load_dotenv
import psycopg2
import psycopg2.extensions
from pathlib import Path
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.decorators import task

# Carregamento das variáveis
DIRETORIO_DAG = Path(__file__).parent.resolve()
CAMINHO_DAG = DIRETORIO_DAG / ".env"
load_dotenv(dotenv_path=CAMINHO_DAG)

# Configuração do Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Configurações Básicas
MAX_TRIES = int(os.getenv("MAX_TRIES"))
HTTP_REQUEST = os.getenv("HTTP_API")
TOKEN_GROQ = os.getenv("GROQ_API_KEY")
MODELO_GROQ = os.getenv("MODELO_LLM")
BATCH_SIZE = int(os.getenv("BATCH_SIZE"))
PAGE_LIMIT = int(os.getenv("PAGE_LIMT"))

CATEGORIAS = [
    "Saúde", "Educação", "Infraestrutura e Obras", "Tecnologia da Informação",
    "Alimentação e Nutrição", "Segurança Pública", "Meio Ambiente e Saneamento",
    "Transporte e Logística", "Administrativo e Material de Escritório",
    "Serviços Gerais", "Outros"
]

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "database": os.getenv("MAIN_DB"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

COLUNAS_ESPERADAS = [
    "id", "cod_concedente", "cod_financiador", "cod_gestora", "cod_orgao", "cod_secretaria",
    "descricao_modalidade", "descricao_objeto", "descricao_tipo", "descricao_url",
    "data_assinatura", "data_processamento", "data_termino", "flg_tipo", "isn_parte_destino",
    "isn_sic", "num_spu", "valor_contrato", "isn_modalidade", "isn_entidade", "tipo_objeto",
    "num_spu_licitacao", "descricao_justificativa", "valor_can_rstpg", "data_publicacao_portal",
    "descricao_url_pltrb", "descricao_url_ddisp", "descricao_url_inexg", "cod_plano_trabalho",
    "num_certidao", "descricao_edital", "cpf_cnpj_financiador", "num_contrato",
    "valor_original_concedente", "valor_original_contrapartida", 
    "valor_atualizado_concedente", "valor_atualizado_contrapartida",
    "created_at", "updated_at", "plain_num_contrato", "calculated_valor_aditivo", "calculated_valor_ajuste", "calculated_valor_empenhado", "calculated_valor_pago",
    "contract_type", "infringement_status", "cod_financiador_including_zeroes",
    "accountability_status", "plain_cpf_cnpj_financiador", "descricao_situacao", "data_publicacao_doe",
    "descricao_nome_credor", "isn_parte_origem", "data_auditoria", "data_termino_original",
    "data_inicio", "data_rescisao", "confidential", "gestor_contrato", "data_finalizacao_prestacao_contas", "sequential", "emergency", "law", "has_non_profit_transfer",
    "nome_fiscal", "emenda_parlamentar", "ano_emenda_parlamentar",  "codigo_emenda_parlamentar"
]

COLUNAS_NUMERICAS = [
    'valor_contrato', 'valor_can_rstpg', 'valor_original_concedente', 
    'valor_original_contrapartida', 'valor_atualizado_concedente', 
    'valor_atualizado_contrapartida', 'calculated_valor_aditivo', 
    'calculated_valor_ajuste', 'calculated_valor_empenhado'
]

COLUNAS_BOOLEANAS = ['confidential', 'emergency', 'has_non_profit_transfer']

COLUNAS_DATA = [
    'data_assinatura', 'data_processamento', 'data_termino',
    'data_publicacao_portal', 'created_at', 'updated_at',
    'data_publicacao_doe', 'data_auditoria', 'data_termino_original', 
    'data_inicio', 'data_rescisao', 'data_finalizacao_prestacao_contas'
]

def get_db_connection():
    """
    Estabelece uma conexão ativa com o banco de dados PostgreSQL
    """
    connection = psycopg2.connect(**DB_CONFIG)
    return connection


def _criacao_tabelas():
    """
    Garante a existência das tabelas de contratos e classificações no banco de dados
    """
    
    ddl_contratos = f"""
        CREATE TABLE IF NOT EXISTS {os.getenv("DB_CONTRATOS")}(
            id BIGINT PRIMARY KEY,
            cod_concedente TEXT,
            cod_financiador TEXT,
            cod_gestora TEXT,
            cod_orgao TEXT,
            cod_secretaria TEXT,
            descricao_modalidade TEXT,
            descricao_objeto TEXT,
            descricao_tipo TEXT,
            descricao_url TEXT,
            data_assinatura TIMESTAMPTZ,
            data_processamento TIMESTAMPTZ,
            data_termino TIMESTAMPTZ,
            flg_tipo TEXT,
            isn_parte_destino TEXT,
            isn_sic TEXT,
            num_spu TEXT,
            valor_contrato NUMERIC(18, 2),
            isn_modalidade TEXT,
            isn_entidade TEXT,
            tipo_objeto TEXT,
            num_spu_licitacao TEXT,
            descricao_justificativa TEXT,
            valor_can_rstpg NUMERIC(18, 2),
            data_publicacao_portal TIMESTAMPTZ,
            descricao_url_pltrb TEXT,
            descricao_url_ddisp TEXT,
            descricao_url_inexg TEXT,
            cod_plano_trabalho TEXT,
            num_certidao TEXT,
            descricao_edital TEXT,
            cpf_cnpj_financiador TEXT,
            num_contrato TEXT,
            valor_original_concedente NUMERIC(18, 2),
            valor_original_contrapartida NUMERIC(18, 2),
            valor_atualizado_concedente NUMERIC(18, 2),
            valor_atualizado_contrapartida NUMERIC(18, 2),
            created_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ,
            plain_num_contrato TEXT,
            calculated_valor_aditivo NUMERIC(18, 2),
            calculated_valor_ajuste NUMERIC(18, 2),
            calculated_valor_empenhado NUMERIC(18, 2),
            calculated_valor_pago NUMERIC(18, 2),
            contract_type TEXT,
            infringement_status TEXT,
            cod_financiador_including_zeroes TEXT,
            accountability_status TEXT,
            plain_cpf_cnpj_financiador TEXT,
            descricao_situacao TEXT,
            data_publicacao_doe TIMESTAMPTZ,
            descricao_nome_credor TEXT,
            isn_parte_origem TEXT,
            data_auditoria TIMESTAMPTZ,
            data_termino_original TIMESTAMPTZ,
            data_inicio TIMESTAMPTZ,
            data_rescisao TIMESTAMPTZ,
            confidential BOOLEAN,
            gestor_contrato TEXT,
            data_finalizacao_prestacao_contas TIMESTAMPTZ,
            sequential TEXT,
            emergency BOOLEAN,
            law TEXT,
            has_non_profit_transfer BOOLEAN,
            nome_fiscal TEXT,
            emenda_parlamentar TEXT,
            ano_emenda_parlamentar TEXT, 
            codigo_emenda_parlamentar TEXT
        );
    """

    ddl_classificacoes =  f"""
        DROP TABLE IF EXISTS {os.getenv("DB_CLASSIFICACOES")};

        CREATE TABLE IF NOT EXISTS {os.getenv("DB_CLASSIFICACOES")}(
            id_contrato bigint primary key,
            valor_contrato NUMERIC (18,2),
            data_assinatura TIMESTAMPTZ,
            categoria TEXT,
            justificativa TEXT,
            tokens INTEGER,
            modelo TEXT
        );
    """

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS public")
            cur.execute(ddl_contratos)
            cur.execute(ddl_classificacoes)
        conn.commit()
    
    logger.info("Tabelas criadas com sucesso!")


# Formatação de valores
def parse_data(valor):
    """
    Converte uma string em um objeto de datetime válido, realizando o parse
    de string baseando-se nos padrões de data mais comuns e tratando exceções e valores nulos 
    """
    if isinstance(valor, pd.Timestamp): return valor
    if valor is None or pd.isna(valor) or valor in ('NaT', '0001-01-01'): return None
    
    # Limpeza de espaços em branco
    date_str = valor.strip()

    # Recuperação dos 19 primeiros carracteres para recuperar o formato padrão de data (dd/MM/yyyy HH:mm:ss)
    date_str = date_str[:19]

    # Adaptação de cada data em seu padrão de string esperado
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", 
                "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d","%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue    

    return None


def parse_boolean(valor):
    """
    Converte uma string em um objeto booleano
    """
    if isinstance(valor, bool): return valor 
    if valor is None or pd.isna(valor) or valor == 'NaT': return None

    texto = str(valor).strip().lower()

    if texto in {"true", "t", "1", "sim", "s", "yes", "y"}: return True
    if texto in {"false", "f", "0", "nao", "não", "n", "no"}: return False

    return None


def parse_number(valor):
    """
    Converte uma string em float, limpando caracteres inválidos
    """
    if isinstance(valor, float): return valor
    if valor is None or pd.isna(valor) or valor == 'NaT': return None
    
    texto = str(valor).strip()
    possui_letras = bool(re.search(r'[a-zA-Z]', texto))

    if possui_letras == False:
        # Avalia se a string fornece o padrão numerico ideal
        eh_numero = bool(re.match(r"^\d+(\.\d+)?$", texto))
        if eh_numero:
            return float(texto)
    return None


# TASK 1 - Extração de Dados da API
def extracao_registros() -> list[dict]:
    """
    Consome a API do Ceará Transparente, buscando contratos nas últimas duas semanas.
    Realiza a paginação automática e possui mecanismos de tentativas caso ocorra 
    timeout ou falha na requisição HTTP. 
    """
    hoje = date.today()
    data_inicio = (hoje - timedelta(weeks=2)).strftime("%d/%m/%Y")
    data_fim = hoje.strftime("%d/%m/%Y")
    
    registros, page = [], 1
    session = requests.session()
    logger.info(f"Extração de Registros de {data_inicio} a {data_fim}")

    try:
        while True:
            params = {
                "page": page,
                "data_assinatura_inicio": data_inicio,
                "data_assinatura_fim": data_fim
            }
            resp = None

            for tentantiva in range(1, MAX_TRIES+1):
                try:
                    resp = session.get(HTTP_REQUEST, params=params, timeout=60)
                    resp.raise_for_status()
                    break
                except requests.exceptions.RequestException as e:
                    logger.warning(f'Tentativa: {tentantiva}/{MAX_TRIES} falhou no acesso à pagina {page}')

                    if tentantiva == MAX_TRIES:
                        logger.error(f'Erro definitivo na página {page} - {e}')
                        return registros
                    time.sleep(2)
            
            if resp is None:
                logger.warning(f'A quantidade de tentativas de acesso a página {page} esgotou')
                break
            if resp.status_code == 204:
                logger.warning(f'A página {page} está sem registros para obter')
                break

            try:
                page_encoding = resp.apparent_encoding or "utf-8"
                page_content = resp.content.decode(page_encoding, errors="replace")
                page_payload = json.loads(page_content)
            
                total_pages = page_payload.get('sumary', {}).get('total_pages', '')
                page_data = page_payload.get("data", {})

                if page == 1 and total_pages != '':
                    logger.info(f'Total de Páginas a Extrair: {total_pages}')

                if page_data and isinstance(page_data, list) and total_pages != '':
                    registros.extend(page_data)
                    logger.info(f'Página {page}: {len(page_data)} contratos encontrados')
                else:
                    logger.warning(f'Página {page}: sem contratos encontrados')

            except json.JSONDecodeError:
                logger.error(f'Erro ao decodificar o JSON na página {page}')
                break

            if page >= total_pages or page >= PAGE_LIMIT:
                break
            page += 1
            time.sleep(0.3)
    finally:
        session.close()
    return registros


# TASK 2 - Armazenamento dos Contratos
def _normalizacao_dados(registros: list[dict]) -> list[dict]:
    """
    Tratamento, Limpeza e padronização dos registros antes de sua persistência
    """
    if len(registros) == 0:
        logger.warning('Sem dados para normalizar')
        return []

    df = pd.DataFrame(registros).copy()
    # Renomeação de colunas inesperadas
    df.columns = df.columns.str.replace("descriaco", "descricao")
    
    # Avaliar se os registros possuem todas as colunas esperadas
    df = df[[col for col in df.columns if col in COLUNAS_ESPERADAS]].copy()

    if "data_assinatura" not in df.columns:
        logger.error(f'A data de assinatura precisa estar contida no conjunto de dados')
    
    # Remoção de Duplicadas
    if "id" in df.columns:
        if "updated_at" in df.columns:
            df = df.sort_values(by="updated_at", ascending=True)
        df = df.drop_duplicates(subset=['id'], keep="last")

    df = df.dropna(subset=['data_assinatura'])

    if df.empty:
        logger.warning('Sem dados para normalizar')
        return []
    
    # Fromatar as colunas com cada tipagem esperada (Numerico, Booleano, Datetime)
    mapeamento_funcoes = {
        parse_number: COLUNAS_NUMERICAS,
        parse_boolean: COLUNAS_BOOLEANAS,
        parse_data: COLUNAS_DATA,
    }

    for funcao_limpeza, colunas in mapeamento_funcoes.items():  
        for coluna in colunas:
            df[coluna] = df[coluna].apply(funcao_limpeza)
    
    db_registros = [row.to_dict() for _, row in df.iterrows()]
    return db_registros


def insercao_contratos(registros: list[dict]):
    """
    Inserção do conjunto de dados para a tabela de contratos
    """
    if len(registros) == 0:
        logger.warning("Não apresenta nenhum registro para adaptar")
        return 0

    registros_principais = _normalizacao_dados(registros)
    _criacao_tabelas()
    
    if len(registros_principais) == 0:
        logger.warning("Não apresenta nenhum registro normalizado para inserir na tabela")
        return 0
    
    colunas_insert = ", ".join(COLUNAS_ESPERADAS)
    colunas_update = ", ".join([f"{c} = EXCLUDED.{c}" for c in COLUNAS_ESPERADAS])

    insercao_contratos = f"""
        INSERT INTO {os.getenv("DB_CONTRATOS")} ({colunas_insert})
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET {colunas_update}
    """

    registro_tuplas = [
        tuple(
            None if pd.isna(registro.get(coluna)) else registro.get(coluna)
            for coluna in COLUNAS_ESPERADAS
        )
        for registro in registros_principais
    ]

    selecao_dados = f"""SELECT count(*) from public.{os.getenv("DB_CONTRATOS")}"""
    inseridos = 0

    with get_db_connection() as conn:
        with conn.cursor() as cur:
           execute_values(cur, insercao_contratos, registro_tuplas)
           inseridos = cur.rowcount
           
           cur.execute(selecao_dados)
           total_registros = cur.fetchone()[0]
        conn.commit()
    
    logger.info(f'{len(registro_tuplas)} processados | {inseridos} inseridos')
    return total_registros


# TASK 3 - Classificação com Large Language Model (LLM)
def _chamada_provedor_llm(prompt_sistema: str, prompt_usuario: str):
    """
    Executa uma chamada à estrutura à API do GROQ para classificação de texto.
    """
    client = Groq(api_key=TOKEN_GROQ)

    response = client.chat.completions.create(
        model=MODELO_GROQ,
        messages=[
            {"role": "system", "content": prompt_sistema},
            {"role": "user", "content": prompt_usuario},
        ],
        temperature=0,
        max_tokens=1000,
        response_format={"type": "json_object"}
    )
    return {
        "conteudo": response.choices[0].message.content,
        "tokens": response.usage.total_tokens,
        "modelo": MODELO_GROQ
    }

def _criacao_prompt(registro: dict, categorias: list[str]):
    """
    Elaboração dos prompts de sistema e usuário que serão utilizados no modelo GROQ
    """
    categorias_formatadas = "\n".join(f"  - {c}" for c in categorias)

    objeto = str(registro.get('descricao_objeto') or registro.get('objeto') or '').strip()
    id_contrato = registro.get('id')
    valor_contrato = registro.get('valor_contrato') or registro.get("valor_contrato")
    contrato = f"[{id_contrato} | {valor_contrato}] {objeto}"
    
    prompt_comando = f"""Você é um especialista em transferência pública.
    Seu objetivo é analisar um contrato e classificá-lo semanticamente
    
    REGRAS OBRIGATÓRIAS:
    1. Mantenha o ID_CONTRATO idêntico ao enviado na entrada
    2. Retorne estritamente um objeto JSON nesse formato
    3. Caso retorne alguma abreviação explique-a

    Formato JSON de Saída:
    {{
        "classificação": 
        {{
            "id_contrato": '',
            "categoria": "string",
            "justificativa": "string curto"
        }}
    }}
    
    CATEGORIAS PERMITIDAS:
    {categorias_formatadas}
    """
    prompt_usuario = f'Classifique o seguinte contrato:\n{contrato}'
    return prompt_comando, prompt_usuario


def _extracao_top_30():
    """
    Filtra e retorna os 30 contratos de maior valor financeiro
    """
    selecao_dados = f"""SELECT id, valor_contrato, data_assinatura, descricao_objeto 
    from public.{os.getenv("DB_CONTRATOS")} order by valor_contrato desc limit 30"""

    registros_top30 = []

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(selecao_dados)
            colunas = [c[0] for c in cur.description]
            linhas = cur.fetchall()
            registros_top30 = [dict(zip(colunas, linha)) for linha in linhas]
            logger.info("30 registros recuperados")
    
    return registros_top30


def classificacao_top_30_contratos(categorias: list[str]):
    """
    Efetua o tratamento dos 30 contratos de maior valor financeiro
    """
    top_30 = _extracao_top_30()
    resultados_finais, soma_tokens = [], 0
    time.sleep(0.5)

    for i, register in enumerate(top_30):
        logger.info(f'Enviando registro {i+1} para LLM')
        prompt_sistema, prompt_usuario = _criacao_prompt(register, categorias)

        try:
            resposta = _chamada_provedor_llm(prompt_sistema=prompt_sistema, 
                                             prompt_usuario=prompt_usuario)
            
            conteudos = json.loads(resposta['conteudo'])
            classificacao = conteudos.get("classificação") or {}
            tokens, modelo = resposta['tokens'], resposta['modelo']

            if classificacao == {}:
                classificacao['id_contrato'] = register.get("id")
                classificacao['categoria'] = 'Outros'
                classificacao['justificativa'] = 'Falha no processamento do registro.'

            classificacao['valor_contrato'] = register.get("valor_contrato") or register.get("valor_total")

            # Deve ser retornada como string para não afetar a logica de timestampzone da biblioteca pendulum
            if register.get("data_assinatura"):
                classificacao['data_assinatura'] = register.get("data_assinatura").strftime("%Y-%m-%d %H:%M:%S")
            else:
                classificacao['data_assinatura'] = None

            classificacao['tokens'] = tokens
            classificacao['modelo'] = modelo
            
            resultados_finais.append(classificacao)
            soma_tokens += tokens
            logger.info(f'Sucesso! Total de Tokens Utilizados nesse Lote: {tokens}')

        except Exception as e:
            logger.error(f"Erro no LLM: {e}")
            resultados_finais.append(
                {
                    "id_contrato": register.get("id"),
                    "categoria": "Outros",
                    "justificativa": "Falha no processamento do registro.",
                    "valor_contrato": register.get("valor_total") or register.get("valor_contrato"),
                    "data_assinatura": register.get("data_assinatura").strftime("%Y-%m-%d %H:%M:%S") if register.get("data_assinatura") else register.get("data_assinatura"),
                    "tokens": 0,
                    "modelo": MODELO_GROQ  
                }
            )
        
        if (i + 1) % BATCH_SIZE == 0:
            logger.info("Processando ...")
            time.sleep(2.0)
    
    logger.info(f'Dados Classificados! Total de tokens utilizados: {soma_tokens}')
    return resultados_finais


# Task 4 - Armazenamento dos Contratos Classificados
def insercao_contratos_classificados(registros_classificados: list[dict]):
    """
    Inserção das classificações estruturadas pela LLM na tabela de classificações de contratos
    """
    if len(registros_classificados) == 0:
        logger.warning('Não apresenta nenhum registro para inserir')
        return

    colunas_registro = ['id_contrato', 'valor_contrato', 'data_assinatura', 
                        'categoria', 'justificativa', 'tokens', 'modelo']
    
    colunas_insert = ', '.join(colunas_registro)
    colunas_update = ", ".join([f"{c} = EXCLUDED.{c}" for c in colunas_registro])

    insercao_contratos = f"""
        INSERT INTO {os.getenv("DB_CLASSIFICACOES")} ({colunas_insert})
        VALUES %s
        ON CONFLICT (id_contrato) DO UPDATE SET {colunas_update};
    """

    registro_tuplas = [
        (
            registro.get("id_contrato"), registro.get("valor_contrato"),
            parse_data(registro.get("data_assinatura")), registro.get("categoria"),
            registro.get("justificativa"), registro.get("tokens"), registro.get("modelo")
        )
        for registro in registros_classificados
    ]

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insercao_contratos, registro_tuplas)
            logger.info(f'Inserção de {len(registro_tuplas)} valores na tabela de classificações')
        conn.commit()


# TASK 5 - Geração do Relatório
def geracao_relatorio_html(quantidade_registros, lista_classificacoes: list[dict]):
    """
    Renderiza o relatório HTML com os indicadores financeiros e o ranking dos top contratos 
    """

    # Processamento de variáveis
    data_exec = datetime.now().strftime("%d/%m/%Y %H:%M")
    modelo_usado = lista_classificacoes[0].get("modelo", "?") if lista_classificacoes else "?"

    # Calculos Financeiros
    valores_financeiros = [c.get("valor_contrato", 0) for c in lista_classificacoes]
    media_contratos = round(np.average(valores_financeiros), 2) if valores_financeiros else 0

    # Quantidade de Tokens Consumidos
    tokens_consumidos = np.sum([c.get("tokens", 0) for c in lista_classificacoes])

    # Ranking Contratos
    contratos_ordenados = sorted(lista_classificacoes, key=lambda x: x.get("valor_contrato"), reverse=True)

    ranking_contratos = "" 
    for c in contratos_ordenados:

        assinatura = "Indefinido" if c.get("data_assinatura") is None \
                else c.get("data_assinatura").replace("00:00:00", "").replace("'T'", "").replace("T", "") \
                if (re.search("00:00:00", c.get("data_assinatura"))) \
                else c.get("data_assinatura").replace("'T'", "").replace("T", "")

        ranking_contratos += f"""
            <tr>
                <td class='no-break'>{c.get("id_contrato")}</td>
                <td class='no-break'>{c.get("valor_contrato"):.2f}</td>
                <td class='no-break'>{assinatura}</td>
                <td class='no-break'>{c.get("categoria", "Outros")}</td>
                <td class='no-break'>{c.get("justificativa", "")}</td>
            </tr>
        """

    # Frequência por Categoria
    df = pd.DataFrame([{"categoria": c["categoria"]} for c in lista_classificacoes])
    categorias_por_frequencia = df['categoria'].value_counts().to_dict()

    lista_frequencias = "" 
    for categoria, valor_total in categorias_por_frequencia.items():
        percentual = (valor_total/len(lista_classificacoes)) * 100
        lista_frequencias += f"""
            <tr>
                <td>{categoria}</td>
                <td class='no-break'>{valor_total}</td>
                <td>{percentual:.2f} %</td>
            </tr>
        """
    
    string_html = f"""
        <!DOCTYPE html>
        <html lang="pt-BR">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Relatório de Classificação de Contratos</title>
            </head>

            <style>
                :root {{
                    --primary-color: #1e293b;
                    --secondary-color: #3b82f6;
                    --background-color: #f8fafc;
                    --card-background: #ffffff;
                    --text-color: #334155;
                    --border-color: #e2e8f0;
                }}

                * {{
                    box-sizing: border-box;
                    margin: 0;
                    padding: 0;
                }}

                body {{
                    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                    background-color: var(--background-color);
                    color: var(--text-color);
                    line-height: 1.6;
                    padding: 2rem;
                }}

                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                }}

                /* Header */
                .header {{
                    background-color: var(--primary-color);
                    color: #ffffff;
                    padding: 2rem;
                    border-radius: 12px;
                    margin-bottom: 2rem;
                    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
                }}

                .header h1 {{
                    font-size: 1.8rem;
                    margin-bottom: 0.5rem;
                    font-weight: 700;
                    text-align: center;
                }}

                .header p {{
                    font-size: 0.9rem;
                    color: #94a3b8;
                    text-align: center;
                }}

                /* Cards */
                .cards-container {{
                    display: grid;
                    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
                    gap: 1.5rem;
                    margin-bottom: 2rem;
                }}

                .card {{
                    background: var(--card-background);
                    padding: 1.5rem;
                    border-radius: 8px;
                    border: 1px solid var(--border-color);
                    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
                    transition: transform 0.2s;
                }}

                .card:hover {{
                    transform: translateY(-2px);
                }}

                .card .lbl {{
                    font-size: 0.85rem;
                    text-transform: uppercase;
                    letter-spacing: 0.05em;
                    color: #64748b;
                    margin-bottom: 0.5rem;
                    font-weight: 600;
                }}

                .card .num {{
                    font-size: 1.8rem;
                    font-weight: 700;
                    color: var(--primary-color);
                }}

                /* Tabelas */
                .tables-grid {{
                    display: grid;
                    grid-template-columns: 2fr 1fr;
                    gap: 1.5rem;
                    align-items: start;
                }}

                .table-wrapper {{
                    background: var(--card-background);
                    border-radius: 8px;
                    border: 1px solid var(--border-color);
                    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
                    overflow: auto;
                    padding: 1.25rem;
                    width: 100%
                }}

                .table-wrapper h2 {{
                    font-size: 1.1rem;
                    margin-bottom: 1rem;
                    color: var(--primary-color);
                    border-bottom: 2px solid var(--border-color);
                    padding-bottom: 0.5rem;
                }}

                table {{
                    width: 100%;
                    border-collapse: collapse;
                    text-align: left;
                    font-size: 0.9rem;
                }}

                th {{
                    background-color: #f1f5f9;
                    color: #475569;
                    font-weight: 600;
                    padding: 10px 12px;
                    border-bottom: 2px solid var(--border-color);
                }}

                td {{
                    padding: 10px 12px;
                    border-bottom: 1px solid var(--border-color);
                    color: #334155;
                }}

                tr:last-child td {{
                    border-bottom: none;
                }}

                tr:hover td {{
                    background-color: #f8fafc;
                }}

                .no-break {{
                    white-space: nowrap;
                }}
            </style>

            <body>
                <div class="header">
                    <h1> Relatório de Classificações de Contratos - Ceará Transparente </h1>
                    <p> Gerado em: {data_exec} | Modelo Utilizado: {modelo_usado} </p>
                </div>    
            </body>

            <div class="cards-container">
                <div class="card">
                    <div class="lbl">Quantidade de Contratos</div>
                    <div class="num">{quantidade_registros}</div>
                </div>
                <div class="card">
                    <div class="lbl">Valor Médio dos Contratos</div>
                    <div class="num">{media_contratos}</div>
                </div>        
                <div class="card">
                    <div class="lbl">Total de Tokens Consumidos</div>
                    <div class="num">{tokens_consumidos}</div>
                </div>       
            </div>

            <div class="tables-grid">
                <div class="table-wrapper">
                    <h2> Ranking dos Contratos com os maiores valores </h2>
                    <table>
                        <thead>
                            <tr>    
                                <th>ID</th>
                                <th>Valor (R$)</th>
                                <th>Assinatura</th>
                                <th>Categoria</th>
                                <th>Justificativa</th>
                            </tr>
                        </thead>
                        <tbody>
                            {ranking_contratos}
                        </tbody>
                    </table>
                </div>

                <div class="table-wrapper">
                    <h2> Classificações Mais Frequentes </h2>
                    <table>
                        <thead>
                            <tr>    
                                <th>Classificação</th>
                                <th>Frequência</th>
                                <th>Percentual</th>
                            </tr>
                        </thead>
                        <tbody>
                            {lista_frequencias}
                        </tbody>
                    </table>
                </div>
            </div>
        </html>
    """

    pasta_registros = DIRETORIO_DAG / "registros"

    # Cria a pasta de registros se não existir
    pasta_registros.mkdir(parents=True, exist_ok=True)

    caminho_path = pasta_registros / "relatorio_contratos_classificados.html"

    #caminho_path = Path(r"registros/relatorio_contratos_classificados.html")
    caminho_path.write_text(string_html, encoding="utf-8")
    
    logger.info(f'Relatório atualizado em: {caminho_path}')


# Execução da DAG
with DAG (
    dag_id="classificacao_ceara_transparente",
    description = 
        "Pipeline Diário: Extrai os contratos da API do Ceará Transparente. Classifica os contratos com maiores valores financeiros através do GROQ. Armazena no Postegres a Lista de contratos extraída e os contratos com maiores valores financeiros",
    start_date=datetime(2026,1,1, tzinfo=pendulum.timezone("America/Sao_Paulo")),
    schedule="0 10 * * *",
    catchup=False,
    tags=['groq', 'llm', 'pstg']
) as dag:
    
    @task 
    def task_extracao():
        return extracao_registros()
    
    @task 
    def task_insercao_contratos(lista_contratos):
        return insercao_contratos(lista_contratos)

    @task 
    def task_top30(categorias):
        return classificacao_top_30_contratos(categorias)

    @task 
    def task_insercao_contratos_classificados(lista_contratos):
        return insercao_contratos_classificados(lista_contratos)
    
    @task 
    def task_relatorio_html(contratos, contratos_classificados):
        return geracao_relatorio_html(contratos, contratos_classificados)

    registros_extraidos = task_extracao()
    total_registros = task_insercao_contratos(registros_extraidos)
    top30_contratos = task_top30(CATEGORIAS)
    insercao_classificados = task_insercao_contratos_classificados(top30_contratos)
    relatorio_final = task_relatorio_html(total_registros, top30_contratos)

    registros_extraidos >> total_registros >> top30_contratos >> insercao_classificados >> relatorio_final
