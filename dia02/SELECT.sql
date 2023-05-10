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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SELECT FROM WHERE (DAY-2/A6)

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido WHERE vlPreco >= 500

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido WHERE vlFrete > vlPreco

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido WHERE vlPreco >= 100 AND vlFrete > vlPreco

-- COMMAND ----------

SELECT * FROM silver.olist.produto WHERE descCategoria = 'pet_shop' OR descCategoria = 'telefonia' OR descCategoria = 'bebes'

-- COMMAND ----------

SELECT * FROM silver.olist.produto WHERE descCategoria IN ('pet_shop', 'telefonia', 'bebes')

-- COMMAND ----------

-- DBTITLE 1,Pedidos de jan/2017
SELECT idPedido, idCliente, descSituacao, dtPedido, date(dtPedido)
FROM silver.olist.pedido
WHERE date(dtPedido) >= '2017-01-01' AND date(dtPedido) <= '2017-01-31'


-- SELECT idPedido, idCliente, descSituacao, dtPedido
-- FROM silver.olist.pedido
-- WHERE dtPedido >= '2017-01-01' AND dtPedido <= '2017-01-31'
-- Como os dados estão em timestamp a consulta exclui dados do dia 31 após 00hrs

-- COMMAND ----------

SELECT idPedido, idCliente, descSituacao, dtPedido, date(dtPedido)
FROM silver.olist.pedido
WHERE year(dtPedido) = 2017 AND month(dtPedido) = 1

-- COMMAND ----------

SELECT *
FROM silver.olist.pedido
WHERE year(dtPedido) = 2017 AND (month(dtPedido) = 1 OR month(dtPedido) = 6)
-- Parenteses podem alterar o resultado

-- COMMAND ----------

SELECT *
FROM silver.olist.pedido
WHERE year(dtPedido) = 2017 AND month(dtPedido) IN (1, 6)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SELECT CASE (DAY-2/A7)

-- COMMAND ----------

SELECT *,
  CASE WHEN vlPreco <= 100 THEN '000 -| 100'
       WHEN vlPreco <= 200 THEN '101 -| 200'
       WHEN vlPreco <= 300 THEN '201 -| 300'
       ELSE '>300'
  END AS faixaPreco
FROM silver.olist.item_pedido

-- COMMAND ----------

SELECT *,
  CASE WHEN vlFrete / (vlFrete + vlPreco) = 0 THEN 'Frete Gratuito'
       WHEN vlFrete / (vlFrete + vlPreco) <= 0.2 THEN 'Frete Baixo'
       WHEN vlFrete / (vlFrete + vlPreco) <= 0.4 THEN 'Frete Médio'
       ELSE 'Frete Alto'
  END AS descFrete
FROM silver.olist.item_pedido

-- COMMAND ----------

SELECT *,
  CASE WHEN descUF IN ('SC', 'PR', 'RS') THEN 'Sul'
       WHEN descUF IN ('SP', 'MG', 'RJ', 'ES') THEN 'Sudeste'
       WHEN descUF IN ('MS', 'MT', 'GO', 'DF') THEN 'Centro-Oeste'
       WHEN descUF IN ('AC', 'RO', 'AM', 'RR', 'PA', 'AP', 'TO') THEN 'Norte'
       ELSE 'Nordeste'
  END AS descRegiao
FROM silver.olist.cliente

-- COMMAND ----------

SELECT *,
  CASE WHEN descUF = 'SP' THEN 'Paulista'
       WHEN descUF = 'RJ' THEN 'Fluminense'
       WHEN descUF = 'MG' THEN 'Mineiro'
       WHEN descUF = 'AC' THEN 'Acreano'
       WHEN descUF = 'BA' THEN 'Baiano'
       ELSE 'Não Mapeado'
  END AS descNacionalidade
FROM silver.olist.vendedor

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # SELECT CASE FROM WHERE (DAY-2/A8)

-- COMMAND ----------

SELECT *, 
  CASE
    WHEN vlPreco <= 100 THEN '000 -| 100'
    WHEN vlPreco <= 200 THEN '101 -| 200'
    WHEN vlPreco <= 300 THEN '201 -| 300'
    WHEN vlPreco <= 400 THEN '301 -| 400'
    WHEN vlPreco <= 500 THEN '401 -| 500'
    WHEN vlPreco <= 600 THEN '501 -| 600'
    WHEN vlPreco <= 700 THEN '601 -| 700'
    WHEN vlPreco <= 800 THEN '701 -| 800'
    WHEN vlPreco <= 900 THEN '801 -| 900'
    WHEN vlPreco <= 1000 THEN '901 -| 1000'
    ELSE '+100' 
  END AS fxPreco,
  CASE 
    WHEN vlFrete / (vlFrete + vlPreco) = 0 THEN 'Frete Gratuito'
    WHEN vlFrete / (vlFrete + vlPreco) <= 0.2 THEN 'Frete Baixo'
    WHEN vlFrete / (vlFrete + vlPreco) <= 0.4 THEN 'Frete Médio'
    ELSE 'Frete Alto'
  END AS fxFrete
FROM silver.olist.item_pedido
WHERE vlPreco >= 50
