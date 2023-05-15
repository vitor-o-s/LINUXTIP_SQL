-- Databricks notebook source
SELECT * FROM silver.olist.item_pedido ORDER BY vlPreco --ASC

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido ORDER BY vlPreco DESC

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido ORDER BY vlPreco ASC, vlFrete DESC

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido ORDER BY vlPreco DESC, vlFrete DESC LIMIT 3

-- COMMAND ----------

SELECT * 
FROM silver.olist.produto 
WHERE descCategoria = 'perfumaria'
ORDER BY nrTamanhoNome DESC
LIMIT 5

-- COMMAND ----------

SELECT *,
       vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volumeCm3 
FROM silver.olist.produto 
WHERE descCategoria = 'perfumaria'
ORDER BY vlComprimentoCm * vlAlturaCm * vlLarguraCm DESC
LIMIT 5
