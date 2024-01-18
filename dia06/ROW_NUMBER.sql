-- Databricks notebook source
WITH tb_row_number AS (
  SELECT 
    idProduto,
    descCategoria,
    vlPesoGramas,
    row_number() OVER (PARTITION BY descCategoria ORDER BY vlPesoGramas DESC) AS rnProduto
  FROM silver.olist.produto

  WHERE descCategoria IS NOT NULL
)

SELECT 
  * 
FROM tb_row_number
WHERE rnProduto = 1

-- COMMAND ----------

WITH tb_row_number AS (
  SELECT 
    idProduto,
    descCategoria,
    vlPesoGramas,
    row_number() OVER (PARTITION BY descCategoria ORDER BY vlPesoGramas DESC) AS rnProduto
  FROM silver.olist.produto

  WHERE descCategoria IS NOT NULL
)

SELECT 
  * 
FROM tb_row_number
WHERE rnProduto <= 5

-- COMMAND ----------

WITH tb_row_number AS (
  SELECT 
    idProduto,
    descCategoria,
    vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volumeProduto,
    row_number() OVER (PARTITION BY descCategoria ORDER BY vlComprimentoCm * vlAlturaCm * vlLarguraCm DESC) AS rnProduto
  FROM silver.olist.produto

  WHERE descCategoria IS NOT NULL
)

SELECT 
  * 
FROM tb_row_number
WHERE rnProduto = 1
