-- Databricks notebook source
WITH tb_dia_receita AS (
  SELECT
    date(t2.dtPedido) AS dtPedido,
    SUM(t1.vlPreco) AS receitaDia
  FROM silver.olist.item_pedido AS t1

  INNER JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t2.descSituacao = 'delivered'

  GROUP BY 1
  ORDER BY 1
), -- Traz a receita diaria

tb_lag AS (
  SELECT 
    *,
    LAG(receitaDia) OVER (PARTITION BY 1 ORDER BY dtPedido) AS lag1Receita,
    LAG(receitaDia, 2) OVER (PARTITION BY 1 ORDER BY dtPedido) AS lag2Receita
  FROM tb_dia_receita
) -- Faz o shift linear

SELECT
  *,
  receitaDia - lag1Receita AS diffLag1,
  receitaDia - lag2Receita AS diffLag2
FROM tb_lag

-- COMMAND ----------

WITH tb_dia_receita AS (
  SELECT
    date(t2.dtPedido) AS dtPedido,
    SUM(t1.vlPreco) AS receitaDia
  FROM silver.olist.item_pedido AS t1

  INNER JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t2.descSituacao = 'delivered'

  GROUP BY 1
  ORDER BY 1
), -- Traz a receita diaria

tb_lag AS (
  SELECT 
    *,
    LEAD(receitaDia) OVER (PARTITION BY 1 ORDER BY dtPedido) AS led1Receita,
    LEAD(receitaDia, 2) OVER (PARTITION BY 1 ORDER BY dtPedido) AS led2Receita
  FROM tb_dia_receita
) -- Faz o shift linear

SELECT
  *,
  receitaDia - led1Receita AS diffLed1,
  receitaDia - led2Receita AS diffLed2
FROM tb_lag

-- COMMAND ----------

WITH tb_dia_receita AS (
  SELECT
    date(t2.dtPedido) AS dtPedido,
    SUM(t1.vlPreco) AS receitaDia
  FROM silver.olist.item_pedido AS t1

  INNER JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t2.descSituacao = 'delivered'

  GROUP BY 1
  ORDER BY 1
), -- Traz a receita diaria

tb_lag AS (
  SELECT 
    *,
    LAG(receitaDia, -1) OVER (PARTITION BY 1 ORDER BY dtPedido) AS lag1Receita,
    LAG(receitaDia, -2) OVER (PARTITION BY 1 ORDER BY dtPedido) AS lag2Receita
  FROM tb_dia_receita
) -- Faz o shift linear

SELECT
  *,
  receitaDia - lag1Receita AS diffLag1,
  receitaDia - lag2Receita AS diffLag2
FROM tb_lag

-- COMMAND ----------

WITH tb_vendedor AS (

  SELECT DISTINCT
         idVendedor,
         date(dtPedido) AS dtPedido

  FROM silver.olist.item_pedido AS t1

  INNER JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido

  ORDER BY idVendedor, dtPedido

),

tb_lag AS (

  SELECT *,
         LAG(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) AS lag1Data

  FROM tb_vendedor

),

tb_diff AS (

  SELECT *,
         datediff(dtPedido, lag1Data) AS diffData

  FROM tb_lag

)

SELECT idVendedor,
       avg(diffData) AS mediaIntervaloVendas,
       percentile(diffData, 0.5) AS medianaIntervaloVendas

FROM tb_diff

GROUP BY idVendedor

HAVING count(*) >= 2 -- Garante que somente vendedores com + de 1 venda estejam na base
