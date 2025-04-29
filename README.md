# Entrevista TSMX

Este script Python automatiza a importação de dados de clientes, planos, contratos e informações de contato de um arquivo Excel para um banco de dados PostgreSQL.

## Funcionalidades

- Leitura de dados de clientes a partir de arquivos Excel
- Conexão e persistência no banco de dados PostgreSQL
- Tratamento e formatação automática de dados (CPF/CNPJ, endereços, CEP, etc.)
- Importação em múltiplas tabelas relacionadas
- Prevenção de registros duplicados
- Mapeamento automático de estados brasileiros para siglas (UF)

## Pré-requisitos

- Python
- PostgreSQL
- Bibliotecas Python:
  - pandas
  - numpy
  - psycopg2

## Instalação

1. Clone este repositório ou baixe os arquivos
2. Instale as dependências

3. Configure o banco de dados PostgreSQL com as seguintes tabelas:
   - tbl_status_contrato
   - tbl_tipos_contato
   - tbl_planos
   - tbl_clientes
   - tbl_cliente_contratos
   - tbl_cliente_contatos

## Estrutura do Banco de Dados

O script espera encontrar as seguintes tabelas no banco de dados:

### tbl_status_contrato
- id (chave primária)
- status (texto: 'Ativo' ou 'Inativo')

### tbl_tipos_contato
- id (chave primária)
- tipo_contato (texto: 'Telefone', 'Celular', 'E-mail')

### tbl_planos
- id (chave primária)
- descricao (texto)
- valor (numérico)

### tbl_clientes
- id (chave primária)
- nome_razao_social (texto)
- nome_fantasia (texto)
- cpf_cnpj (texto - formatado como chave única)
- data_nascimento (data)
- data_cadastro (data)

### tbl_cliente_contratos
- id (chave primária)
- cliente_id (chave estrangeira para tbl_clientes)
- plano_id (chave estrangeira para tbl_planos)
- dia_vencimento (numérico)
- isento (booleano)
- endereco_logradouro (texto)
- endereco_numero (texto)
- endereco_complemento (texto)
- endereco_bairro (texto)
- endereco_cidade (texto)
- endereco_cep (texto formatado)
- endereco_uf (texto - siglas dos estados brasileiros)
- status_id (chave estrangeira para tbl_status_contrato)

### tbl_cliente_contatos
- id (chave primária)
- cliente_id (chave estrangeira para tbl_clientes)
- tipo_contato_id (chave estrangeira para tbl_tipos_contato)
- contato (texto - telefone, celular ou email dependendo do tipo)

## Formato do Arquivo Excel

O script espera um arquivo Excel com as seguintes colunas:

- Nome/Razão Social
- Nome Fantasia
- CPF/CNPJ
- Data Nasc.
- Data Cadastro cliente
- Endereço
- Número
- Complemento
- Bairro
- Cidade
- CEP
- UF
- Plano
- Vencimento
- Isento
- Plano Valor
- Status
- Telefones
- Emails
- Celulares

## Configuração

Edite as seguintes configurações no script conforme necessário:

```python
config = {
    'host': 'localhost',
    'database': 'tsmxdb',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

file_path = "./src/dados_importacao.xlsx"
```

## Uso

Execute o script principal:

```bash
python script.py
```

O script irá:
1. Conectar ao banco de dados PostgreSQL
2. Carregar o arquivo Excel especificado
3. Importar os dados nas tabelas correspondentes
4. Registrar o progresso no console