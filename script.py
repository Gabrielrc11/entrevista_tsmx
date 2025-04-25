import psycopg2
from psycopg2 import OperationalError
import pandas as pd
import numpy as np

def connect_db():
    # Configurações de conexão
    config = {
        'host': 'localhost',
        'database': 'tsmxdb',
        'user': 'postgres',
        'password': 'admin',
        'port': '5432'
    }

    try:
        # Tentar estabelecer conexão
        connect = psycopg2.connect(**config)
        print("Conexão bem-sucedida!")
        return connect
    
    except OperationalError as e:
        # Captura de erro de conexão
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def load_file(file_path):
    # Função para carregar um arquivo XLSX
    try:
        # Define o mapeamento de colunas
        column_mapping = {
            "Nome/Razão Social": "nome_razao_social",
            "Nome Fantasia": "nome_fantasia",
            "CPF/CNPJ": "cpf_cnpj",
            "Data Nasc.": "data_nascimento",
            "Data Cadastro cliente": "data_cadastro"
        }
        
        # Lê apenas as colunas que estão no mapeamento
        df = pd.read_excel(file_path, usecols=column_mapping.keys())
        print(f"Arquivo {file_path} carregado com sucesso!")
        print(f"Dimensões: {df.shape[0]} linhas x {df.shape[1]} colunas")
        
        # Rename columns to match database
        df.rename(columns=column_mapping, inplace=True)
        
        # Mostrar as colunas carregadas para confirmação
        print("Colunas carregadas:", list(df.columns))
        return df
        
    except Exception as e:
        print(f"Erro ao carregar o arquivo XLSX: {e}")
        return None

def save_to_database(df, table_name, conn=None):
    if conn is None:
        conn = connect_db()
        if conn is None:
            return False
        close_conn = True
    else:
        close_conn = False
    
    try:
        # Substituir NaN por None para compatibilidade com SQL
        df_clean = df.replace({np.nan: None})
        
        cursor = conn.cursor()
        
        # Verificar se a tabela existe
        cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print(f"Tabela não existe.")
            return False
        else:
            # Preparar valores para inserção
            cols = ', '.join([f'"{col}"' for col in df_clean.columns])
            placeholders = ', '.join(['%s'] * len(df_clean.columns))
            
            # Crie a parte "ON CONFLICT" da consulta para atualizar registros existentes
            update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in df_clean.columns if col != 'cpf_cnpj']
            update_clause = ', '.join(update_cols)
            
            # Inserir dados com tratamento de conflitos
            insert_query = f"""
                INSERT INTO {table_name} ({cols}) 
                VALUES ({placeholders})
                ON CONFLICT (cpf_cnpj) DO UPDATE SET {update_clause}
            """
            
            # Converter DataFrame para lista de tuplas para inserção
            rows = [tuple(row) for row in df_clean.values]
            
            cursor.executemany(insert_query, rows)
            conn.commit()
            
            print(f"Processados {len(df_clean)} registros na tabela '{table_name}' (inseridos ou atualizados)")
            return True
        
    except Exception as e:
        print(f"Erro ao salvar dados no banco: {e}")
        conn.rollback()
        return False
    
    finally:
        if close_conn and conn:
            conn.close()
            print("Conexão fechada.")

# Executar o teste de conexão e importação
if __name__ == "__main__":
    conn = connect_db()
    file_path = "./src/dados_importacao.xlsx"
    df = load_file(file_path)
    
    if df is not None and conn is not None:
        table_name = "tbl_clientes"
        save_to_database(df, table_name, conn)
        
        # Fechar conexão
        if conn is not None:
            conn.close()
        print("Processo de importação concluído!")