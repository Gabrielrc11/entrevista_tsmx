import psycopg2
from psycopg2 import OperationalError
import pandas as pd
import numpy as np

# Dicionário de mapeamento de nomes de estados para siglas
ESTADO_PARA_SIGLA = {
    'Acre': 'AC',
    'Alagoas': 'AL',
    'Amapá': 'AP',
    'Amazonas': 'AM',
    'Bahia': 'BA',
    'Ceará': 'CE',
    'Distrito Federal': 'DF',
    'Espírito Santo': 'ES',
    'Goiás': 'GO',
    'Maranhão': 'MA',
    'Mato Grosso': 'MT',
    'Mato Grosso do Sul': 'MS',
    'Minas Gerais': 'MG',
    'Pará': 'PA',
    'Paraíba': 'PB',
    'Paraná': 'PR',
    'Pernambuco': 'PE',
    'Piauí': 'PI',
    'Rio de Janeiro': 'RJ',
    'Rio Grande do Norte': 'RN',
    'Rio Grande do Sul': 'RS',
    'Rondônia': 'RO',
    'Roraima': 'RR',
    'Santa Catarina': 'SC',
    'São Paulo': 'SP',
    'Sergipe': 'SE',
    'Tocantins': 'TO'
}

def connect_db():
    """Estabelece conexão com o banco de dados PostgreSQL"""
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

def load_file(file_path):
    """Carrega um arquivo XLSX e prepara os dados para importação"""
    try:
        column_mapping = {
            "Nome/Razão Social": "nome_razao_social",
            "Nome Fantasia": "nome_fantasia",
            "CPF/CNPJ": "cpf_cnpj",
            "Data Nasc.": "data_nascimento",
            "Data Cadastro cliente": "data_cadastro",
            "Endereço": "endereco_logradouro",
            "Número": "endereco_numero",
            "Complemento": "endereco_complemento",
            "Bairro": "endereco_bairro",
            "Cidade": "endereco_cidade",
            "CEP": "endereco_cep",
            "UF": "endereco_uf",
            "Plano": "descricao",
            "Vencimento": "dia_vencimento",
            "Isento": "isento",
            "Plano Valor": "valor",
            "Status": "status",
            "Telefones": "telefone",
            "Emails": "email", 
            "Celulares": "celular"
        }
        
        df = pd.read_excel(file_path, usecols=column_mapping.keys())
        print(f"Arquivo {file_path} carregado com sucesso!")
        print(f"Dimensões: {df.shape[0]} linhas x {df.shape[1]} colunas")
        
        df.rename(columns=column_mapping, inplace=True)
        
        if 'endereco_uf' in df.columns:
            df['endereco_uf'] = df['endereco_uf'].apply(
                lambda x: ESTADO_PARA_SIGLA.get(x.strip().title(), x) if pd.notna(x) else x
            )
        
        print("Colunas carregadas:", list(df.columns))
        return df
        
    except Exception as e:
        print(f"Erro ao carregar o arquivo XLSX: {e}")
        return None

def send_db(df, table_name, conn=None, conflict_key=None, conflict_columns=None, ignore_duplicates=False):
    """Envia dados para o banco de dados PostgreSQL com verificação de duplicidade"""
    if conn is None:
        conn = connect_db()
        if conn is None:
            return False
        close_conn = True
    else:
        close_conn = False
    
    try:
        df_clean = df.replace({np.nan: None})
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print(f"Tabela {table_name} não existe.")
            return False
            
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}'
        """)
        existing_columns = [row[0] for row in cursor.fetchall()]
        valid_columns = [col for col in df_clean.columns if col in existing_columns]
        
        if not valid_columns:
            print(f"Nenhuma coluna válida encontrada para a tabela {table_name}")
            return False
            
        cols = ', '.join([f'"{col}"' for col in valid_columns])
        placeholders = ', '.join(['%s'] * len(valid_columns))
        
        # Lidar com inserções para evitar erros de duplicidade
        if ignore_duplicates:
            # Usa inserção individual com try/except para ignorar erros de duplicidade
            insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            success_count = 0
            error_count = 0
            
            for _, row_data in df_clean.iterrows():
                try:
                    values = [row_data[col] for col in valid_columns]
                    cursor.execute(insert_query, values)
                    success_count += 1
                except psycopg2.errors.UniqueViolation:
                    # Ignora erros de duplicidade
                    conn.rollback()  # Necessário após um erro para continuar operações
                    error_count += 1
                    continue
                
            conn.commit()
            print(f"Processados {success_count} registros com sucesso e {error_count} duplicidades ignoradas na tabela '{table_name}'")
        else:
            # Comportamento normal com ON CONFLICT quando há uma única chave
            insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
            
            if conflict_key and conflict_key in valid_columns:
                if conflict_columns is None:
                    update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in valid_columns if col != conflict_key]
                else:
                    update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in conflict_columns if col in valid_columns]
                
                if update_cols:
                    update_clause = ', '.join(update_cols)
                    insert_query += f" ON CONFLICT ({conflict_key}) DO UPDATE SET {update_clause}"
                else:
                    insert_query += f" ON CONFLICT ({conflict_key}) DO NOTHING"
            
            rows = [tuple(row) for row in df_clean[valid_columns].values]
            cursor.executemany(insert_query, rows)
            conn.commit()
            
            print(f"Processados {len(df_clean)} registros na tabela '{table_name}'")
        
        return True
        
    except Exception as e:
        print(f"Erro ao salvar dados no banco: {e}")
        conn.rollback()
        return False
    finally:
        if close_conn and conn:
            conn.close()

def prepare_clientes_data(df):
    """Prepara dados para tbl_clientes"""
    return df[["nome_razao_social", "nome_fantasia", "cpf_cnpj", "data_nascimento", "data_cadastro"]]

def prepare_contratos_data(df, conn):
    """Prepara dados para tbl_cliente_contratos com conversão correta de tipos"""
    try:
        # Obter mapeamentos necessários
        cursor = conn.cursor()
        cursor.execute("SELECT id, cpf_cnpj FROM tbl_clientes")
        clientes_map = {row[1]: row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT id, descricao FROM tbl_planos")
        planos_map = {row[1]: row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT id FROM tbl_status_contrato WHERE status = 'Ativo' LIMIT 1")
        status_id = cursor.fetchone()[0] if cursor.rowcount > 0 else 1
        
        # Criar DataFrame de contratos
        contratos_df = df[["dia_vencimento", "isento", "endereco_logradouro", "endereco_numero", 
                         "endereco_complemento", "endereco_bairro", "endereco_cidade", 
                         "endereco_cep", "endereco_uf", "cpf_cnpj", "descricao"]].copy()
        
        # CONVERSÃO DE TIPOS:
        
        # 1. Converter campo 'isento' para boolean
        contratos_df['isento'] = contratos_df['isento'].apply(
            lambda x: str(x).lower() in ('sim', 's', 'true', 't', '1', 'yes', 'y') if pd.notna(x) else False
        )
        
        # 2. Garantir que UF tenha 2 caracteres
        contratos_df['endereco_uf'] = contratos_df['endereco_uf'].str[:2].str.upper()
        
        # 3. Preencher campos obrigatórios
        contratos_df['endereco_logradouro'] = contratos_df['endereco_logradouro'].fillna('Endereço não informado')
        contratos_df['endereco_numero'] = contratos_df['endereco_numero'].fillna('S/N').astype(str).str[:15]
        contratos_df['endereco_bairro'] = contratos_df['endereco_bairro'].fillna('Bairro não informado')
        contratos_df['endereco_cidade'] = contratos_df['endereco_cidade'].fillna('Cidade não informada')
        contratos_df['endereco_uf'] = contratos_df['endereco_uf'].fillna('DF')
        contratos_df['endereco_cep'] = contratos_df['endereco_cep'].fillna('00000000').astype(str).str.replace('[^0-9]', '').str[:8]
        
        # Adicionar relacionamentos
        contratos_df['cliente_id'] = contratos_df['cpf_cnpj'].map(clientes_map)
        contratos_df['plano_id'] = contratos_df['descricao'].map(planos_map)
        contratos_df['status_id'] = status_id
        
        # Filtrar registros válidos
        contratos_df = contratos_df.dropna(subset=['cliente_id', 'plano_id'])
        
        return contratos_df[[
            'cliente_id', 'plano_id', 'dia_vencimento', 'isento',
            'endereco_logradouro', 'endereco_numero', 'endereco_complemento',
            'endereco_bairro', 'endereco_cidade', 'endereco_cep', 'endereco_uf',
            'status_id'
        ]]
        
    except Exception as e:
        print(f"Erro ao preparar contratos: {e}")
        return None

def prepare_contratos_data_with_check(df, conn):
    """Prepara dados para tbl_cliente_contratos e verifica duplicidades"""
    contratos_df = prepare_contratos_data(df, conn)
    
    if contratos_df is None:
        return None
    
    try:
        # Consultar contratos existentes para evitar duplicidades
        cursor = conn.cursor()
        cursor.execute("SELECT cliente_id, plano_id FROM tbl_cliente_contratos")
        existing_contratos = {(row[0], row[1]) for row in cursor.fetchall()}
        
        # Filtrar apenas registros que não existem
        filtered_df = contratos_df[~contratos_df.apply(
            lambda row: (row['cliente_id'], row['plano_id']) in existing_contratos, axis=1
        )]
        
        print(f"Contratos após filtro de duplicidade: {len(filtered_df)} de {len(contratos_df)}")
        return filtered_df
    
    except Exception as e:
        print(f"Erro ao verificar duplicidade em contratos: {e}")
        return contratos_df  # Retorne o original em caso de erro

def prepare_contatos_data(df, conn):
    """Prepara dados para tbl_cliente_contatos"""
    try:
        # Obter mapeamento de CPF/CNPJ para ID de cliente
        cursor = conn.cursor()
        cursor.execute("SELECT id, cpf_cnpj FROM tbl_clientes")
        clientes_map = {row[1]: row[0] for row in cursor.fetchall()}
        
        # Obter mapeamento de tipo de contato
        cursor.execute("SELECT id, tipo_contato FROM tbl_tipos_contato")
        tipos_map = {row[1]: row[0] for row in cursor.fetchall()}
        
        # Preparar dados de contatos
        contatos_data = []
        
        for _, row in df.iterrows():
            cliente_id = clientes_map.get(row['cpf_cnpj'])
            if not cliente_id:
                continue
                
            # Telefone - garantir que seja string
            if pd.notna(row['telefone']):
                contatos_data.append({
                    'cliente_id': cliente_id,
                    'tipo_contato_id': tipos_map.get('Telefone', 1),
                    'contato': str(row['telefone']).strip()
                })
            
            # Celular - garantir que seja string
            if pd.notna(row['celular']):
                contatos_data.append({
                    'cliente_id': cliente_id,
                    'tipo_contato_id': tipos_map.get('Celular', 2),
                    'contato': str(row['celular']).strip()
                })
            
            # Email - garantir que seja string
            if pd.notna(row['email']):
                contatos_data.append({
                    'cliente_id': cliente_id,
                    'tipo_contato_id': tipos_map.get('E-mail', 3),
                    'contato': str(row['email']).strip()
                })
        
        return pd.DataFrame(contatos_data)
    except Exception as e:
        print(f"Erro ao preparar contatos: {e}")
        return None

def prepare_contatos_data_with_check(df, conn):
    """Prepara dados para tbl_cliente_contatos e verifica duplicidades"""
    contatos_df = prepare_contatos_data(df, conn)
    
    if contatos_df is None:
        return None
    
    try:
        # Consultar contatos existentes para evitar duplicidades
        cursor = conn.cursor()
        cursor.execute("SELECT cliente_id, tipo_contato_id, contato FROM tbl_cliente_contatos")
        
        # Converter todos os valores para string para garantir comparação consistente
        # Isso é importante especialmente para valores numéricos como telefones
        existing_contatos = {(
            row[0], 
            row[1], 
            str(row[2]).strip() if row[2] is not None else None
        ) for row in cursor.fetchall()}
        
        # Filtrar apenas registros que não existem
        # Converter os valores do DataFrame para o mesmo formato que os dados do banco
        def not_duplicate(row):
            key = (
                row['cliente_id'], 
                row['tipo_contato_id'], 
                str(row['contato']).strip() if row['contato'] is not None else None
            )
            return key not in existing_contatos
        
        filtered_df = contatos_df[contatos_df.apply(not_duplicate, axis=1)]
        
        print(f"Contatos após filtro de duplicidade: {len(filtered_df)} de {len(contatos_df)}")
        return filtered_df
    
    except Exception as e:
        print(f"Erro ao verificar duplicidade em contatos: {e}")
        return contatos_df  # Retorne o original em caso de erro

if __name__ == "__main__":
    conn = connect_db()
    file_path = "./src/dados_importacao.xlsx"
    df = load_file(file_path)
    
    if df is not None and conn is not None:
        try:
            # 1. Importar status
            print("\nImportando status...")
            status_df = pd.DataFrame([{'status': 'Ativo'}, {'status': 'Inativo'}])
            send_db(status_df, "tbl_status_contrato", conn, conflict_key="status")
            
            # 2. Importar tipos de contato
            print("\nImportando tipos de contato...")
            tipos_contato_df = pd.DataFrame([
                {'tipo_contato': 'Telefone'},
                {'tipo_contato': 'Celular'},
                {'tipo_contato': 'E-mail'}
            ])
            send_db(tipos_contato_df, "tbl_tipos_contato", conn, conflict_key="tipo_contato")
            
            # 3. Importar planos
            print("\nImportando planos...")
            planos_df = df[["descricao", "valor"]].drop_duplicates(subset=["descricao"])
            send_db(planos_df, "tbl_planos", conn, conflict_key="descricao")
            
            # 4. Importar clientes
            print("\nImportando clientes...")
            clientes_df = prepare_clientes_data(df)
            send_db(clientes_df, "tbl_clientes", conn, conflict_key="cpf_cnpj")
            
            # 5. Importar contratos com verificação de duplicidade
            print("\nImportando contratos...")
            contratos_df = prepare_contratos_data_with_check(df, conn)
            if contratos_df is not None and not contratos_df.empty:
                # Usar cliente_id e plano_id como chaves compostas para evitar duplicidade
                # Como não é possível usar chaves compostas no ON CONFLICT, filtramos antes
                send_db(contratos_df, "tbl_cliente_contratos", conn)
            else:
                print("Nenhum novo contrato para importar.")
            
            # 6. Importar contatos com verificação de duplicidade
            print("\nImportando contatos...")
            contatos_df = prepare_contatos_data_with_check(df, conn)
            if contatos_df is not None and not contatos_df.empty:
                # Usar cliente_id, tipo_contato_id e contato como chaves compostas
                # Usando o novo parâmetro ignore_duplicates para contornar erros de duplicidade
                send_db(contatos_df, "tbl_cliente_contatos", conn, ignore_duplicates=True)
            else:
                print("Nenhum novo contato para importar.")
            
            print("\nProcesso de importação concluído com sucesso!")
            
        except Exception as e:
            print(f"Erro durante o processo de importação: {e}")
        finally:
            if conn is not None:
                conn.close()