import psycopg2
import pandas as pd
import numpy as np

# Dicionário de mapeamento de nomes de estados para siglas
ESTADO_PARA_SIGLA = {
    'Acre': 'AC', 'Alagoas': 'AL', 'Amapá': 'AP', 'Amazonas': 'AM',
    'Bahia': 'BA', 'Ceará': 'CE', 'Distrito Federal': 'DF',
    'Espírito Santo': 'ES', 'Goiás': 'GO', 'Maranhão': 'MA',
    'Mato Grosso': 'MT', 'Mato Grosso do Sul': 'MS',
    'Minas Gerais': 'MG', 'Pará': 'PA', 'Paraíba': 'PB',
    'Paraná': 'PR', 'Pernambuco': 'PE', 'Piauí': 'PI',
    'Rio de Janeiro': 'RJ', 'Rio Grande do Norte': 'RN',
    'Rio Grande do Sul': 'RS', 'Rondônia': 'RO', 'Roraima': 'RR',
    'Santa Catarina': 'SC', 'São Paulo': 'SP', 'Sergipe': 'SE',
    'Tocantins': 'TO'
}

def connect_db():
    """Conecta ao banco de dados PostgreSQL."""
    config = {
        'host': 'localhost',
        'database': 'tsmxdb',
        'user': 'postgres',
        'password': 'admin',
        'port': '5432'
    }
    
    try:
        conn = psycopg2.connect(**config)
        print("Conexão bem-sucedida!")
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None

def load_file(file_path):
    """Carrega e formata os dados do arquivo Excel."""
    try:
        # Mapeamento de colunas do Excel para o banco de dados
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
        print(f"Arquivo carregado com sucesso: {df.shape[0]} linhas x {df.shape[1]} colunas")
        
        # Renomear colunas e converter UF para siglas
        df.rename(columns=column_mapping, inplace=True)
        if 'endereco_uf' in df.columns:
            df['endereco_uf'] = df['endereco_uf'].apply(
                lambda x: ESTADO_PARA_SIGLA.get(x.strip().title(), x) if pd.notna(x) else x
            )
        
        return df
        
    except Exception as e:
        print(f"Erro ao carregar o arquivo: {e}")
        return None

def send_db(df, table_name, conn=None, conflict_key=None, conflict_columns=None):
    """Insere ou atualiza dados no banco de dados."""
    close_conn = False
    if conn is None:
        conn = connect_db()
        if conn is None:
            return False
        close_conn = True
    
    try:
        df_clean = df.replace({np.nan: None})
        cursor = conn.cursor()
        
        # Verificar se a tabela existe
        cursor.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}');")
        if not cursor.fetchone()[0]:
            print(f"Tabela {table_name} não existe.")
            return False
            
        # Obter colunas existentes na tabela
        cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
        existing_columns = {row[0] for row in cursor.fetchall()}
        valid_columns = [col for col in df_clean.columns if col in existing_columns]
        
        if not valid_columns:
            print(f"Nenhuma coluna válida para a tabela {table_name}")
            return False
            
        # Preparar query de inserção
        cols = ', '.join([f'"{col}"' for col in valid_columns])
        placeholders = ', '.join(['%s'] * len(valid_columns))
        insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        
        # Adicionar cláusula ON CONFLICT se especificada
        if conflict_key and conflict_key in valid_columns:
            if conflict_columns:
                update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in conflict_columns if col in valid_columns]
            else:
                update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in valid_columns if col != conflict_key]
            
            if update_cols:
                update_clause = ', '.join(update_cols)
                insert_query += f" ON CONFLICT ({conflict_key}) DO UPDATE SET {update_clause}"
            else:
                insert_query += f" ON CONFLICT ({conflict_key}) DO NOTHING"
        
        # Executar a inserção
        rows = [tuple(row) for row in df_clean[valid_columns].values]
        cursor.executemany(insert_query, rows)
        conn.commit()
        
        print(f"Processados {len(df_clean)} registros na tabela '{table_name}'")
        return True
        
    except Exception as e:
        print(f"Erro ao salvar dados: {e}")
        conn.rollback()
        return False
    finally:
        if close_conn and conn:
            conn.close()

def formatar_documento(doc):
    """Formata CPF ou CNPJ para o padrão brasileiro."""
    if pd.isna(doc):
        return None
        
    doc_limpo = ''.join(filter(str.isdigit, str(doc)))
    
    if len(doc_limpo) == 11:  # CPF
        return f"{doc_limpo[:3]}.{doc_limpo[3:6]}.{doc_limpo[6:9]}-{doc_limpo[9:]}"
    elif len(doc_limpo) == 14:  # CNPJ
        return f"{doc_limpo[:2]}.{doc_limpo[2:5]}.{doc_limpo[5:8]}/{doc_limpo[8:12]}-{doc_limpo[12:]}"
    
    return doc_limpo

def importar_dados(conn, df):
    """Importa todos os dados para o banco de dados."""
    cursor = conn.cursor()
    
    # 1. Importar status de contrato
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
    clientes_df = df[["nome_razao_social", "nome_fantasia", "cpf_cnpj", "data_nascimento", "data_cadastro"]].copy()
    clientes_df['cpf_cnpj'] = clientes_df['cpf_cnpj'].apply(formatar_documento)
    clientes_df = clientes_df.dropna(subset=['cpf_cnpj'])
    send_db(clientes_df, "tbl_clientes", conn, conflict_key="cpf_cnpj")
    
    # 5. Importar contratos
    print("\nImportando contratos...")
    # Obter mapeamentos de IDs
    cursor.execute("SELECT id, cpf_cnpj FROM tbl_clientes")
    clientes_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT id, descricao FROM tbl_planos")
    planos_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT id FROM tbl_status_contrato WHERE status = 'Ativo' LIMIT 1")
    status_id = cursor.fetchone()[0] if cursor.rowcount > 0 else 1
    
    # Preparar dados de contratos
    contratos_df = df[["dia_vencimento", "isento", "endereco_logradouro", "endereco_numero", 
                     "endereco_complemento", "endereco_bairro", "endereco_cidade", 
                     "endereco_cep", "endereco_uf", "cpf_cnpj", "descricao"]].copy()
    
    # Tratamento de dados de endereço e contrato
    contratos_df['isento'] = contratos_df['isento'].apply(
        lambda x: str(x).lower() in ('sim', 's', 'true', 't', '1', 'yes', 'y') if pd.notna(x) else False
    )
    contratos_df['endereco_uf'] = contratos_df['endereco_uf'].str[:2].str.upper()
    contratos_df['endereco_logradouro'] = contratos_df['endereco_logradouro'].fillna('Endereço não informado')
    contratos_df['endereco_numero'] = contratos_df['endereco_numero'].fillna('S/N').astype(str).str[:15]
    contratos_df['endereco_bairro'] = contratos_df['endereco_bairro'].fillna('Bairro não informado')
    contratos_df['endereco_cidade'] = contratos_df['endereco_cidade'].fillna('Cidade não informada')
    contratos_df['endereco_uf'] = contratos_df['endereco_uf'].fillna('DF')
    
    # Formatação de CEP
    contratos_df['endereco_cep'] = contratos_df['endereco_cep'].apply(
        lambda cep: '00000-000' if pd.isna(cep) else 
        f"{''.join(filter(str.isdigit, str(cep)))[:5]}-{''.join(filter(str.isdigit, str(cep)))[5:]}" 
        if len(''.join(filter(str.isdigit, str(cep)))) == 8 else '00000-000'
    )
    
    # Adicionar IDs relacionais
    contratos_df['cliente_id'] = contratos_df['cpf_cnpj'].map(clientes_map)
    contratos_df['plano_id'] = contratos_df['descricao'].map(planos_map)
    contratos_df['status_id'] = status_id
    
    # Filtrar registros válidos
    contratos_df = contratos_df.dropna(subset=['cliente_id', 'plano_id'])
    
    # Verificar duplicidades e inserir apenas novos contratos
    cursor.execute("SELECT cliente_id, plano_id FROM tbl_cliente_contratos")
    existing_contratos = {(row[0], row[1]) for row in cursor.fetchall()}
    
    contratos_df = contratos_df[~contratos_df.apply(
        lambda row: (row['cliente_id'], row['plano_id']) in existing_contratos, axis=1
    )]
    
    if not contratos_df.empty:
        contratos_colunas = [
            'cliente_id', 'plano_id', 'dia_vencimento', 'isento',
            'endereco_logradouro', 'endereco_numero', 'endereco_complemento',
            'endereco_bairro', 'endereco_cidade', 'endereco_cep', 'endereco_uf',
            'status_id'
        ]
        send_db(contratos_df[contratos_colunas], "tbl_cliente_contratos", conn)
        print(f"Importados {len(contratos_df)} novos contratos")
    else:
        print("Nenhum novo contrato para importar")
    
    # 6. Importar contatos
    print("\nImportando contatos...")
    cursor.execute("SELECT id, tipo_contato FROM tbl_tipos_contato")
    tipos_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Preparar dados de contato
    contatos_data = []
    for _, row in df.iterrows():
        cliente_id = clientes_map.get(row['cpf_cnpj'])
        if not cliente_id:
            continue
            
        # Adicionar telefone se existir
        if pd.notna(row['telefone']):
            contatos_data.append({
                'cliente_id': cliente_id,
                'tipo_contato_id': tipos_map.get('Telefone', 1),
                'contato': str(row['telefone']).strip()
            })
        
        # Adicionar celular se existir
        if pd.notna(row['celular']):
            contatos_data.append({
                'cliente_id': cliente_id,
                'tipo_contato_id': tipos_map.get('Celular', 2),
                'contato': str(row['celular']).strip()
            })
        
        # Adicionar email se existir
        if pd.notna(row['email']):
            contatos_data.append({
                'cliente_id': cliente_id,
                'tipo_contato_id': tipos_map.get('E-mail', 3),
                'contato': str(row['email']).strip()
            })
    
    contatos_df = pd.DataFrame(contatos_data)
    if not contatos_df.empty:
        # Verificar contatos existentes e inserir apenas novos
        cursor.execute("SELECT cliente_id, tipo_contato_id, contato FROM tbl_cliente_contatos")
        existing_contatos = {(row[0], row[1], str(row[2]).strip()) for row in cursor.fetchall()}
        
        contatos_df = contatos_df[~contatos_df.apply(
            lambda row: (row['cliente_id'], row['tipo_contato_id'], str(row['contato']).strip()) in existing_contatos, axis=1
        )]
        
        if not contatos_df.empty:
            send_db(contatos_df, "tbl_cliente_contatos", conn)
            print(f"Importados {len(contatos_df)} novos contatos")
        else:
            print("Nenhum novo contato para importar")
    else:
        print("Nenhum contato para importar")
    
    print("\nImportação concluída com sucesso!")

# Função principal
if __name__ == "__main__":
    conn = connect_db()
    file_path = "./src/dados_importacao.xlsx"
    df = load_file(file_path)
    
    if df is not None and conn is not None:
        try:
            importar_dados(conn, df)
        finally:
            if conn is not None:
                conn.close()