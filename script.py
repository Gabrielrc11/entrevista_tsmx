import psycopg2
from psycopg2 import OperationalError
import pandas as pd

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
        connect.close()
    
    except OperationalError as e:
        # Captura de erro de conexão
        print(f"Erro ao conectar ao PostgreSQL: {e}")

def load_file(file_path):
    # Função para carregar um arquivo XLSX
    try:
        df = pd.read_excel(file_path)
        print(f"Arquivo {file_path} carregado com sucesso!")
        print(f"Dimensões: {df.shape[0]} linhas x {df.shape[1]} colunas")
        return df
    except Exception as e:
        print(f"Erro ao carregar o arquivo XLSX: {e}")
        return None

# Executar o teste de conexão
if __name__ == "__main__":
    connect_db()
    file_path = "./src/dados_importacao.xlsx"
    df = load_file(file_path)