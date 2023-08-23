# Processo de ETL e Análise de Dados de Ecommerce

![ER](https://github.com/vitor-martinsb/ecommerce/assets/59899402/6b42ce50-96bf-429f-880e-207b44d39378)


Este repositório contém um código que realiza um processo de ETL (Extração, Transformação e Carregamento) e conduz análises de dados usando o framework PySpark. O código lê dados de um banco de dados PostgreSQL, salva tabelas selecionadas como arquivos Parquet e, em seguida, realiza várias tarefas de análise de dados. Abaixo está uma visão geral do processo e das análises executadas no código.

## Processo de ETL

1. **Configuração da Sessão Spark**: Uma sessão Spark é criada para facilitar o processamento de dados usando o PySpark.

2. **Configuração da Conexão com o Banco de Dados**: Os parâmetros para conexão com um banco de dados PostgreSQL são configurados, incluindo o host, porta, nome do banco de dados, nome de usuário e senha.

3. **Extração de Dados e Conversão em Parquet**: O código conecta-se ao banco de dados e obtém uma lista de tabelas do esquema 'public'. Para cada tabela, ele executa uma consulta para obter dados, converte o DataFrame Pandas recuperado em uma tabela PyArrow e salva-o como um arquivo Parquet.

4. **Mapeamento de Tipos de Dados para DBML**: O código mapeia os tipos de dados Pandas para os tipos de dados correspondentes no DBML (Linguagem de Marcação de Banco de Dados). Esse mapeamento é usado para gerar código DBML para criar tabelas.

## Análise de Dados

1. **Análise de Clientes e Pedidos**:
   - Os dados de clientes e pedidos são carregados a partir de arquivos Parquet.
   - O código realiza um "join" à esquerda entre os dados de clientes e pedidos com base na chave 'customer_number'.
   - Os pedidos cancelados são filtrados, e a contagem de pedidos cancelados é agregada por país.
   - O país com o maior número de pedidos cancelados é determinado e exibido.

2. **Análise de Produtos**:
   - Detalhes de pedidos, pedidos e dados de produtos são carregados a partir de arquivos Parquet.
   - Os pedidos são filtrados com base em seu status, data do pedido e dados do produto.
   - O código realiza junções entre pedidos, detalhes de pedidos e dados de produtos.
   - O produto com o maior número de pedidos é determinado, e a receita total gerada a partir desse produto é calculada e exibida.

3. **Análise de Funcionários**:
   - Os dados de funcionários e de escritórios são carregados a partir de arquivos Parquet.
   - As informações dos funcionários são associadas aos dados do escritório com base na chave 'office_code'.
   - O código filtra os funcionários que estão localizados no Japão e oculta seus endereços de e-mail.
   - O resultado é exibido, mostrando o primeiro nome, sobrenome e e-mail mascarado dos funcionários no Japão.

## Arquivos e Diretórios

- `ER.png`: Este diretório contém subdiretórios para cada tabela. Dentro do diretório de cada tabela, os arquivos Parquet são armazenados.
- `main.ipynb`: O script Python principal contendo o processo de ETL e as tarefas de análise de dados.

## Utilização

Para executar o código, certifique-se de ter as dependências necessárias instaladas, incluindo `psycopg2`, `pyarrow`, `pandas`, `pyspark`, `matplotlib` e `difflib`. Em seguida, execute o arquivo `script.py`. O código realizará o processo de ETL e exibirá os resultados da análise de dados.

Certifique-se de que as dependências necessárias estejam instaladas e que os detalhes da conexão com o banco de dados sejam precisos antes de executar o código.
