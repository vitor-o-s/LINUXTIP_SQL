-- Databricks notebook source
SELECT count(DISTINCT idVendedor)
FROM silver.olist.vendedor
WHERE descUF = 'SP'

-- COMMAND ----------

SELECT descUF, count(DISTINCT idVendedor)
FROM silver.olist.vendedor
GROUP BY descUF
ORDER BY 2 DESC
-- ORDER BY descUF 

-- COMMAND ----------

SELECT descCategoria,
       count(idProduto) AS qtProduto,
       avg(vlPesoGramas) AS avgPeso, 
       percentile(vlPesoGramas, 0.5) AS medianaPeso,

       avg(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS avgVolume,
       percentile(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) AS medianaVolume
FROM silver.olist.produto

GROUP BY descCategoria

ORDER BY qtProduto DESC

-- COMMAND ----------

SELECT year(dtPedido) || '-' || month(dtPedido) AS anoMes,
       count(idPedido)
FROM silver.olist.pedido

GROUP BY anoMes
ORDER BY anoMes

-- COMMAND ----------

SELECT date(date_trunc('MONTH', dtPedido)) AS anoMes,
       count(idPedido)
FROM silver.olist.pedido

GROUP BY anoMes
ORDER BY anoMes
