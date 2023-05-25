-- Databricks notebook source
SELECT descCategoria,
       count(DISTINCT idProduto) AS qtdeProduto,
       avg(vlPesoGramas) AS avgPeso

FROM silver.olist.produto

WHERE descCategoria IN ('bebes', 'perfumaria')
OR descCategoria like '%moveis%'

GROUP BY descCategoria

HAVING qtdeProduto > 100 
AND avg(vlPesoGramas) > 1000

ORDER BY avgPeso DESC
