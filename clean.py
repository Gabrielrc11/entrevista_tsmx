import psycopg2
from psycopg2 import OperationalError

def connect_db():
    config = {
        'host': 'localhost',
        'database': 'tsmxdb',
        'user': 'postgres',
        'password': 'admin',
        'port': '5432'
    }

    try:
        connect = psycopg2.connect(**config)
        print("Conexão bem-sucedida!")
        return connect
    except OperationalError as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def clean_tables():
    conn = connect_db()
    if conn is None:
        return

    try:
        cursor = conn.cursor()
        
        # Lista de tabelas a serem limpas
        tables = [
            'tbl_cliente_contatos',
            'tbl_cliente_contratos',
            'tbl_clientes',
            'tbl_planos',
            'tbl_status_contrato',
            'tbl_tipos_contato'
        ]
        
        # Limpa cada tabela
        for table in tables:
            try:
                cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
                print(f"Tabela {table} limpa com sucesso!")
            except Exception as e:
                print(f"Erro ao limpar a tabela {table}: {e}")
        
        conn.commit()
        print("\nTodas as tabelas foram limpas com sucesso!")
        
    except Exception as e:
        print(f"Erro durante a limpeza das tabelas: {e}")
        conn.rollback()
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    clean_tables()
