-- Databricks notebook source
SELECT t2.idVendedor,
       t3.descUF,
       count(t1.idPedido)
FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido t2 
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.vendedor t3
ON t2.idVendedor = t3.idVendedor

WHERE year(t1.dtPedido) = 2017
AND t3.descUF = 'SP'

GROUP BY t2.idVendedor, t3.descUF

ORDER BY count(t1.idPedido) DESC

LIMIT 5

-- COMMAND ----------

SELECT t2.idVendedor,
       t3.descUF,
       count(t1.idPedido)
FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido t2 
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.vendedor t3
ON t2.idVendedor = t3.idVendedor

WHERE year(t1.dtPedido) = 2017
AND t3.descUF = 'RJ'

GROUP BY t2.idVendedor, t3.descUF

ORDER BY count(t1.idPedido) DESC

LIMIT 5

-- COMMAND ----------

(SELECT t2.idVendedor,
       t3.descUF,
       count(t1.idPedido)
FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido t2 
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.vendedor t3
ON t2.idVendedor = t3.idVendedor

WHERE year(t1.dtPedido) = 2017
AND t3.descUF = 'SP'

GROUP BY t2.idVendedor, t3.descUF

ORDER BY count(t1.idPedido) DESC

LIMIT 5)

UNION ALL -- FAZ APPEND DAS LINHAS

(SELECT t2.idVendedor,
       t3.descUF,
       count(t1.idPedido)
FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido t2 
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.vendedor t3
ON t2.idVendedor = t3.idVendedor

WHERE year(t1.dtPedido) = 2017
AND t3.descUF = 'RJ'

GROUP BY t2.idVendedor, t3.descUF

ORDER BY count(t1.idPedido) DESC

LIMIT 5)


-- COMMAND ----------

(SELECT t2.idVendedor,
       t3.descUF,
       count(t1.idPedido)
FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido t2 
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.vendedor t3
ON t2.idVendedor = t3.idVendedor

WHERE year(t1.dtPedido) = 2017
AND t3.descUF = 'SP'

GROUP BY t2.idVendedor, t3.descUF

ORDER BY count(t1.idPedido) DESC

LIMIT 5)

UNION -- FAZ DISTINCT DAS LINHAS

(SELECT t2.idVendedor,
       t3.descUF,
       count(t1.idPedido)
FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido t2 
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.vendedor t3
ON t2.idVendedor = t3.idVendedor

WHERE year(t1.dtPedido) = 2017
AND t3.descUF = 'RJ'

GROUP BY t2.idVendedor, t3.descUF

ORDER BY count(t1.idPedido) DESC

LIMIT 5)

-- UNION SÓ FUNCIONA QUANDO AMBAS AS TABELAS TEM O MESMO NUMERO DE COLUNAS
-- A ORDEM DAS COLUNAS É *RELEVANTE* O UNION IRÁ MISTURAR OS RESULTADOS CASO TENHA MESMO NUMERO DE COLUNAS EM ORDENS DIFERENTES
