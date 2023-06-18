-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Quais são os vendedores de cada estado que tem +R$1000,00 em vendas durante o ano de 2017?

-- COMMAND ----------

SELECT * 
FROM silver.olist.pedido AS t1
WHERE year(t1.dtPedido) = 2017

-- COMMAND ----------

-- Apenas itens de pedidos de 2017
SELECT t1.idPedido,
       t2.idVendedor,
       t2.vlPreco 
FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE year(t1.dtPedido) = 2017

-- COMMAND ----------

-- 
SELECT t1.idPedido,
       t2.idVendedor,
       t2.vlPreco,
       t3.descUF AS descUFVendedor
FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.vendedor AS t3
ON t2.idVendedor = t3.idVendedor

WHERE year(t1.dtPedido) = 2017

-- COMMAND ----------

-- Valor de cada vendedor
SELECT t2.idVendedor,
       t3.descUF AS descUFVendedor,
       sum(t2.vlPreco) AS totalVendido
FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.vendedor AS t3
ON t2.idVendedor = t3.idVendedor

WHERE year(t1.dtPedido) = 2017

GROUP BY t2.idVendedor, t3.descUF

-- COMMAND ----------

-- Quais são os vendedores de cada estado que tem +R$1000,00 em vendas durante o ano de 2017?
SELECT t2.idVendedor,
       t3.descUF AS descUFVendedor,
       sum(t2.vlPreco) AS totalVendido
FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.vendedor AS t3
ON t2.idVendedor = t3.idVendedor

WHERE year(t1.dtPedido) = 2017

GROUP BY t2.idVendedor, t3.descUF

HAVING totalVendido >= 1000

ORDER BY t3.descUF, totalVendido DESC
