# Apresentação TSMX

## Estrutura do Código

### Componentes Principais:
1. **Conexão com Banco de Dados**
   - Função `connect_db()` para estabelecer conexão com PostgreSQL

2. **Carregamento de Dados**
   - Função `load_file()` para leitura do arquivo Excel
   - Mapeamento de colunas do Excel para o banco de dados

3. **Validações e Formatações**
   - Validação de CPF/CNPJ
   - Formatação de documentos
   - Tratamento de dados nulos ou inválidos

4. **Preparação de Dados**
   - Funções específicas para cada tipo de dado:
     - `prepare_clientes_data()`
     - `prepare_contratos_data()`
     - `prepare_contatos_data()`
     - `prepare_planos_data()`

## Fluxo de Execução

1. **Inicialização**
   - Conexão com o banco de dados
   - Carregamento do arquivo Excel

2. **Processamento**
   - Importação de dados para as tabelas do banco de dados:
    - `tbl_clientes`
    - `tbl_cliente_contratos`
    - `tbl_cliente_contatos`
    - `tbl_planos`
    - `tbl_status_contrato`
    - `tbl_tipos_contato`

3. **Verificações**
   - Validação de dados antes da inserção
   - Verificação de duplicidades
   - Tratamento de erros

## 6. Conclusão

O script demonstra uma implementação profissional de ETL (Extract, Transform, Load) com:
- Código bem estruturado e documentado
- Tratamento robusto de erros
- Validações adequadas
- Boas práticas de programação

---