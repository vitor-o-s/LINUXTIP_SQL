-- Databricks notebook source
SELECT *
FROM (
  SELECT *
  FROM silver.olist.pedido
  WHERE descSituacao = 'delivered'
  AND dtEntregue <= dtEstimativaEntrega
)

-- COMMAND ----------

WITH tb_pedidos_sem_atraso AS (
  SELECT *
  FROM silver.olist.pedido
  WHERE descSituacao = 'delivered'
  AND dtEntregue <= dtEstimativaEntrega
)

SELECT * 
FROM tb_pedidos_sem_atraso

-- COMMAND ----------

WITH tb_pedidos_sem_atraso AS (
  SELECT *
  FROM silver.olist.pedido
  WHERE descSituacao = 'delivered'
  AND dtEntregue <= dtEstimativaEntrega
),

tb_produtos_bebes AS (
  SELECT *
  FROM silver.olist.produto
  WHERE descCategoria = 'bebes'
)

SELECT * 
FROM tb_produtos_bebes

-- COMMAND ----------

WITH tb_pedidos_sem_atraso AS (
  SELECT *
  FROM silver.olist.pedido
  WHERE descSituacao = 'delivered'
  AND dtEntregue <= dtEstimativaEntrega
),

tb_produtos_bebes AS (
  SELECT *
  FROM silver.olist.produto
  WHERE descCategoria = 'bebes'
)

SELECT * 
FROM tb_pedidos_sem_atraso AS t1
INNER JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

INNER JOIN tb_produtos_bebes AS t3
ON t2.idProduto = t3.idProduto

-- COMMAND ----------

-- 1. Quais são os TOP 10 vendedores que mais venderam (em quantidade) no mês com maior número de vendas no Olist
WITH tb_mes AS (
    SELECT
      date(date_trunc('month', dtPedido))  AS dtMes
    FROM silver.olist.pedido
    GROUP BY dtMes
    ORDER BY count(DISTINCT idPedido) DESC
    LIMIT 1
)
SELECT t2.idVendedor,
      count(*) AS qtdeItens
FROM silver.olist.pedido AS t1
INNER JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE date(date_trunc('month',t1.dtPedido )) = (SELECT * FROM tb_mes)
GROUP BY t2.idVendedor

ORDER BY qtdeItens DESC

LIMIT 10

-- COMMAND ----------

-- Total de vendas históricas (independente da categoria) dos vendedores que venderam ao menos um produto da categoria bebes na blackfriday de 2017-01-01
WITH tb_vendedores AS (
  SELECT 
    DISTINCT t2.idVendedor
  FROM silver.olist.pedido AS t1

  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto AS t3
  ON t2.idProduto = t3.idProduto

  WHERE date(date_trunc('month', t1.dtPedido)) = '2017-01-01'
  AND t3.descCategoria = 'bebes'
)

SELECT 
  idVendedor,
  count(DISTINCT idPedido) AS qtdePedido
FROM silver.olist.item_pedido

WHERE idVendedor IN (SELECT * FROM tb_vendedores)

GROUP BY idVendedor

-- COMMAND ----------

WITH tb_vendedores AS (
  SELECT 
    DISTINCT t2.idVendedor
  FROM silver.olist.pedido AS t1

  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto AS t3
  ON t2.idProduto = t3.idProduto

  WHERE date(date_trunc('month', t1.dtPedido)) = '2017-01-01'
  AND t3.descCategoria = 'bebes'
)

SELECT 
  t1.idVendedor,
  count(DISTINCT t1.idPedido) AS qtdePedido
FROM silver.olist.item_pedido AS t1

INNER JOIN tb_vendedores AS t2
ON t1.idVendedor = t2.idVendedor
-- WHERE idVendedor IN (SELECT * FROM tb_vendedores)

GROUP BY t1.idVendedor
