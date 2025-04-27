# 📊 Projeto de Entrevista - Analista de Dados

Este projeto tem como objetivo importar dados de um arquivo `.xlsx` para um banco de dados PostgreSQL, conforme proposto no desafio técnico da TSMX.

## 📂 Arquivos utilizados

- `dados_importacao.xlsx` — Planilha com os dados a serem importados.
- `schema_database_pgsql.sql` — Arquivo SQL para criação das tabelas no PostgreSQL.

## ⚙️ Tecnologias e Bibliotecas

- **Python**
- **PostgreSQL**
- **Pandas**
- **Psycopg2**
- **NumPy**

## 📋 Funcionalidades implementadas

- Conexão segura com o banco de dados PostgreSQL.
- Carregamento e tratamento de dados da planilha Excel.
- Normalização de dados (ex.: siglas de estados, CEPs, campos nulos).
- Evita inserção de registros duplicados (clientes, planos, contratos, contatos).
- Exibe resumo final da importação (quantidade de registros processados e ignorados).

## 🚀 Como executar o projeto

1. Clone este repositório:

```bash
git clone https://github.com/gcardsantos/entrevista_tsmx.git
```

2. Acesse o diretório:
```bash
cd projeto-entrevista-analista-dados
```

3. Instale as dependências:
```bash
pip install -r requirements.txt
```

4. Configure o banco de dados PostgreSQL:
- Crie um novo banco de dados.
- Execute o script schema_database_pgsql.sql para criar as tabelas necessárias.

5. Ajuste as credenciais de acesso ao banco no arquivo script.py, se necessário:
```python
config = {
    'host': 'localhost',
    'database': 'tsmxdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}
```

6. Garanta que o arquivo `dados_importacao.xlsx` esteja na pasta `./src/dados_importacao.xlsx`

7. Execute o script:
```bash
python script.py
```