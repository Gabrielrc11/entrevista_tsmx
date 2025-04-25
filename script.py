import psycopg2
from psycopg2 import OperationalError

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
        print(f"Erro ao conectar ao PostgreSQL: {e}")

# Executar o teste de conexão
if __name__ == "__main__":
    connect_db()