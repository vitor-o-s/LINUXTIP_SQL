-- Databricks notebook source
-- MAGIC %md
-- MAGIC Somente alguns bancos aceitam o comando em cima de windows functions, do contrário utilize  CTE

-- COMMAND ----------

SELECT 
  idProduto,
  descCategoria,
  vlPesoGramas,
  row_number() OVER (PARTITION BY descCategoria ORDER BY vlPesoGramas DESC) AS rnProduto
FROM silver.olist.produto

WHERE descCategoria IS NOT NULL

QUALIFY row_number() OVER (PARTITION BY descCategoria ORDER BY vlPesoGramas DESC) = 1

-- COMMAND ----------

SELECT 
  idProduto,
  descCategoria,
  vlPesoGramas,
  row_number() OVER (PARTITION BY descCategoria ORDER BY vlPesoGramas DESC) AS rnProduto
FROM silver.olist.produto

WHERE descCategoria IS NOT NULL

QUALIFY rnProduto = 1

-- COMMAND ----------

SELECT 
  idProduto,
  descCategoria,
  vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volumeProduto,
  row_number() OVER (PARTITION BY descCategoria ORDER BY vlComprimentoCm * vlAlturaCm * vlLarguraCm DESC) AS rnProduto
FROM silver.olist.produto

WHERE descCategoria IS NOT NULL


QUALIFY  row_number() OVER (PARTITION BY descCategoria ORDER BY vlComprimentoCm * vlAlturaCm * vlLarguraCm DESC) <= 3

-- COMMAND ----------

SELECT 
       idProduto,
       descCategoria,
       row_number() OVER (PARTITION BY descCategoria ORDER BY vlPesoGramas DESC) AS rnPeso,
       row_number() OVER (PARTITION BY descCategoria ORDER BY vlComprimentoCm * vlAlturaCm * vlLarguraCm DESC) AS rnVolume

FROM silver.olist.produto

WHERE descCategoria IS NOT null

QUALIFY rnPeso <= 5 AND rnVolume <= 5


-- COMMAND ----------


SELECT 
       idProduto,
       descCategoria

FROM silver.olist.produto

WHERE descCategoria IS NOT null

QUALIFY row_number() OVER (PARTITION BY descCategoria ORDER BY vlPesoGramas DESC) <= 5 AND
        row_number() OVER (PARTITION BY descCategoria ORDER BY vlComprimentoCm * vlAlturaCm * vlLarguraCm DESC) <= 5
