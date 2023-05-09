-- Databricks notebook source
SELECT 'Olá,  Mundo!'

-- COMMAND ----------

SELECT 2 + 2 

-- COMMAND ----------

SELECT 2 * 97

-- COMMAND ----------

SELECT POWER(10, 2) --POW(,)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC [SQL Spark Reference](https://spark.apache.org/docs/latest/api/sql/index.html)

-- COMMAND ----------

SELECT  9 % 2 -- MOD(9, 2)

-- COMMAND ----------

SELECT 'Olá, ' || 'Pessoa' -- CONCAT(STR1,,STR2)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SELECT FROM (DAY-2/A3)

-- COMMAND ----------

-- DBTITLE 1,Consultando tabela
SELECT *
FROM silver.olist.pedido
LIMIT 10

-- COMMAND ----------

SELECT *,
       (vlPreco + vlFrete) AS vlTotal
FROM silver.olist.item_pedido
LIMIT 10

-- COMMAND ----------

SELECT idPedido,
       idProduto,
       vlPreco,
       vlFrete,
       (vlPreco + vlFrete) AS vlTotal
FROM silver.olist.item_pedido
LIMIT 10

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC # Trabalhando com Datas (DAY-2/A5)

-- COMMAND ----------

SELECT '2023-01-01' -- YYYY-MM-DD

-- COMMAND ----------

-- DBTITLE 1,Somando dias
SELECT date_add('2023-01-01', 45) -- YYYY-MM-DD

-- COMMAND ----------

-- DBTITLE 1,Subtraindo dias (1ª forma)
SELECT date_add('2023-01-01', -45)

-- COMMAND ----------

-- DBTITLE 1,Subtraindo dias (2ª forma)
SELECT date_sub('2023-01-01', 45)

-- COMMAND ----------

-- DBTITLE 1,Navegando em meses
SELECT '2023-01-01', add_months('2023-01-01', 12), add_months('2023-01-01', -12)

-- COMMAND ----------

-- DBTITLE 1,Extraindo partes
SELECT year('2023-01-01'), month('2023-01-01'), day('2023-01-01'), dayofweek('2023-01-01')

-- COMMAND ----------

-- DBTITLE 1,Diferença entre datas
SELECT date_diff('2023-06-01','2023-01-01'), months_between('2023-06-01','2023-01-01')

-- COMMAND ----------

SELECT idPedido, idCliente, dtPedido, dtEntregue, date_diff(dtEntregue, dtPedido) AS diasENtreEntregaPedido FROM silver.olist.pedido LIMIT 1000
