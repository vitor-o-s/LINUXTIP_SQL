-- Databricks notebook source
SELECT count(idVendedor),
       count(DISTINCT idVendedor)
FROM silver.olist.vendedor
WHERE descUF= 'RJ'

-- COMMAND ----------

SELECT count(DISTINCT idCliente),
       count(DISTINCT idClienteUnico),
       count(DISTINCT descCidade)
FROM silver.olist.cliente
WHERE descUF= 'SP'

-- COMMAND ----------

SELECT count(*),
       avg(vlPesoGramas),
       percentile(vlPesoGramas, 0.5),
       std(vlPesoGramas)
FROM silver.olist.produto
WHERE descCategoria = 'bebes'
