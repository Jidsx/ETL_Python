# Exercício de ETL com Google Colab usando Python

#### Extração (Extract): 
Nesta etapa, extraímos os dados das tabelas relevantes do banco de dados. Por exemplo, podemos extrair informações das Filiais: (Filial 1 e Filial 2).
#### Transformação (Transform): 
Nesta etapa, aplicamos transformações aos dados extraídos para prepará-los para a análise ou carga em outro sistema. Aplicamos a seguinte transformação nos resultados obtidos
anteriormente: Calcular o valor total de cada venda multiplicando a quantidade de produto pelo preço
unitário.
#### Carga (Load): 
Nesta etapa, carregamos os dados transformados em um destino final, como um data warehouse, um banco de dados analítico ou um sistema de relatórios.

## Cenário De Extração
Iremos utilizar duas bases de dados diferentes:

* Tabela vendas_filial1 arquivo em CSV


* Tabela vendas_filial2 arquivo em XLSX

~~~python
import pandas as pd

# Extração dos dados da filial 1 (CSV)
vendas_filial1 = pd.read_csv('vendas_filial1.csv')

# Extração dos dados da filial 2 (Excel)
vendas_filial2 = pd.read_excel('vendas_filial2.xlsx')

# Transformações
# Podemos fazer transformações adicionais aqui, se necessário

# Visualização dos dados
print("\nDados da Filial 1: ")
print(vendas_filial1.head())

print("\nDados da Filial 2: ")
print(vendas_filial2.head())
~~~
![Dados Obtidos](https://github.com/Jidsx/ETL_Python/assets/113401757/9252d52a-7b36-4444-bcb5-c2a0793a9817) 

## Cenário De Transformação

~~~python
# Transformação dos dados: Calcular o valor total de vendas para cada filial
vendas_filial1['Valor Total'] = vendas_filial1['Quantidade Vendida'] * vendas_filial1['Preço Unitário']
vendas_filial2['Valor Total'] = vendas_filial2['Quantidade Vendida'] * vendas_filial2['Preço Unitário']

# Visualizar os dados transformados
print("Dados Transformados da Filial 1 :")
print(vendas_filial1.head())

print("\nDados Transformados da Filial 2 :")
print(vendas_filial2.head())
~~~

* Depois da transformação, adicionamos uma nova coluna chamada "Valor Total" em cada DataFrame,  calculando o valor total de vendas multiplicando a quantidade vendida pelo preço unitário para cada registro.

![Dados Obtidos](https://github.com/Jidsx/ETL_Python/assets/113401757/4a703ebd-dce7-447b-8aca-b44f9b590419)

## Cenário De Carregamento

~~~python
# Carregamento dos daods transformados para um arquivo CSV
vendas_filial1.to_csv('vendas_filial1_transformadas.csv', index=False)
vendas_filial2.to_csv('vendas_filial2_transformadas.csv', index=False)

# Apresentar os dados carregados dos arquivos CSV
dados_filial1 = pd.read_csv("vendas_filial1_transformadas.csv")
dados_filial2 = pd.read_csv("vendas_filial2_transformadas.csv")

print("Dados Transformados da Filial 1 (do arquivo CSV)")
print(dados_filial1.head())

print("\nDados Transformados da Filial 2 (do arquivo CSV)")
print(dados_filial2.head())
~~~
![Dados Obtidos](https://github.com/Jidsx/ETL_Python/assets/113401757/4ac40c70-026a-49b4-99bc-e7b6b8aab907)

Esta foi a etapa de carregamento deixando os dados com a mesma formatação.

Agora Iremos criar um banco de dados com as informações dos
conteúdos transformados.

~~~python
import sqlite3

#Criar uma conexão com o banco de dados
conn = sqlite3.connect('dados_transformados.db')

# Carregar os dados transformados para tabelas SQL
vendas_filial1.to_sql('vendas_filial1_transformadas', conn, index=False, if_exists='replace')
vendas_filial2.to_sql('vendas_filial2_transformadas', conn, index=False, if_exists='replace')

# Apresentar os dados carregados das tabelas SQL
dados_filial1 = pd.read_sql("SELECT * FROM vendas_filial1_transformadas", conn)
dados_filial2 = pd.read_sql("SELECT * FROM vendas_filial2_transformadas", conn)

print("Dados Transformados da Filial 1 (do banco de dados SQL)")
print(dados_filial1.head())

print("Dados Transformados da Filial 2 (do banco de dados SQL)")
print(dados_filial2.head())

# Fechar a conexão com o banco de dados
conn.close()
~~~

Após rodar o conteúdo do código do slide anterior teremos um
novo arquivo dados_transformados.db, esse novo arquivo irá conter os dados das duas bases de dados transformadas.

![Dados Obtidos](https://github.com/Jidsx/ETL_Python/assets/113401757/b46a0d82-e8b8-4c34-8a8a-b2985820069e)

Para verificar todos os dados integrados na base de dados_transformados.db teremos que executar o
código a seguir:

~~~python
import sqlite3

# Criar uma conexão com o banco de dados
conn = sqlite3.connect('dados_transformados.db')

# Criar um cursor para executar consultas SQL
cursor = conn.cursor()

# Executar uma consulta SQL para obter os dados da tabela de vendas_filial_transformadas
cursor.execute('SELECT * FROM vendas_filial1_transformadas')

# Obter os resultados da consulta
dados_filial1 = cursor.fetchall()

# Exibir os dados da filial 1
print("Dados Transformados fa Filial 1: ")
for linha in dados_filial1:
  print(linha)

# Executar uma consulta SQL para obter os dados da tabela vendas_filial2_transformadas
cursor.execute('SELECT * FROM vendas_filial2_transformadas')

# Obter os resultados da consulta
dados_filial2 = cursor.fetchall()

# Exibir os dados da filial 2
print("\nDados Transformados fa Filial 2: ")
for linha in dados_filial2:
  print(linha)

# Fechar o cursor e a conexão com o banco de dados
cursor.close()

conn.close()
~~~
![Dados Obtidos](https://github.com/Jidsx/ETL_Python/assets/113401757/b3d82957-a442-4b68-9431-31ea34b9b3ed)

