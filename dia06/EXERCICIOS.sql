-- Databricks notebook source
-- 1. Quais são os Top 5 vendedores campeões de vendas de cada UF?

WITH tb_venda AS (
  SELECT
    t1.idVendedor,
    t2.descUF,
    sum(t1.vlPreco) AS receitaVendedor,
    count(*) AS qtdeVendaItens,
    count(DISTINCT  t1.idPedido) AS qtdeVendaPedidos
  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.vendedor AS t2
  ON t1.idVendedor = t2.idVendedor

  GROUP BY t1.idVendedor, t2.descUF
)


SELECT 
  *,
  row_number() OVER (PARTITION BY descUF ORDER BY qtdeVendaPedidos DESC) AS rnPedidos,
  row_number() OVER (PARTITION BY descUF ORDER BY receitaVendedor DESC) AS rnReceita
FROM tb_venda

QUALIFY rnPedidos <= 5

-- COMMAND ----------

-- 2. Quais são os Top 5 vendedores campeões de vendas em cada categoria?

WITH tb_venda AS (
  SELECT
    t1.idVendedor,
    t2.descCategoria,
    count(*) AS qtdeVendaItens,
    count(DISTINCT  t1.idPedido) AS qtdeVendaPedidos
  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.produto AS t2
  ON t1.idProduto = t2.idProduto

  WHERE t2.descCategoria IS NOT NULL

  GROUP BY t1.idVendedor, t2.descCategoria
)


SELECT 
  *,
  row_number() OVER (PARTITION BY descCategoria ORDER BY qtdeVendaPedidos DESC) AS rnPedidos
FROM tb_venda

QUALIFY rnPedidos <= 5

-- COMMAND ----------

-- 3. Qual é a Top 1 categoria de cada vendedor
WITH tb_venda AS (
  SELECT
    t1.idVendedor,
    t2.descCategoria,
    count(*) AS qtdeVendaItens
  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.produto AS t2
  ON t1.idProduto = t2.idProduto

  WHERE t2.descCategoria IS NOT NULL

  GROUP BY t1.idVendedor, t2.descCategoria
)


SELECT 
  *,
  row_number() OVER (PARTITION BY idVendedor ORDER BY qtdeVendaItens DESC) AS rnItem
FROM tb_venda

QUALIFY rnItem <= 1

-- COMMAND ----------

-- 4. Quais são as Top 2 categorias que mais vendem para clientes de cada estado?

WITH tb_completa AS (
  SELECT
    *
  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.produto AS t2
  ON t1.idProduto = t2.idProduto

  INNER JOIN silver.olist.pedido AS t3
  ON t1.idPedido = t3.idPedido

  LEFT JOIN silver.olist.cliente AS t4
  ON  t3.idCliente = t4.idCliente 

  WHERE t2.descCategoria IS NOT NULL
),

tb_group AS (
  SELECT 
    descUF, 
    descCategoria,
    count(*) AS qtdeItens
  FROM tb_completa

  GROUP BY descUF, descCategoria

  ORDER BY  descUF, descCategoria
)

SELECT
  *,
  row_number() OVER (PARTITION BY descUF ORDER BY qtdeItens DESC) AS rnCategoria
FROM tb_group

QUALIFY rnCategoria <= 2


-- COMMAND ----------

-- 5. Quantidade acumulada de itens vendidos por categoria ao longo do tempo.
WITH tb_vendas AS (
  SELECT
    *
  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.produto AS t2
  ON t1.idProduto = t2.idProduto

  INNER JOIN silver.olist.pedido AS t3
  ON t1.idPedido = t3.idPedido

  WHERE t2.descCategoria IS NOT NULL
),

tb_group AS (
  SELECT
    descCategoria,
    date(dtPedido) AS dataPedido,
    count(*) AS qtdeItens
  FROM tb_vendas

  GROUP BY descCategoria, dataPedido
  ORDER BY descCategoria, dataPedido
) -- Faz o shift linear

SELECT 
  *,
  sum(qtdeItens) OVER (PARTITION BY descCategoria ORDER BY dataPedido ASC) AS qtdeAcumItens
FROM tb_group


-- COMMAND ----------

-- 6. Receita acumulada por categoria ao longo do tempo
WITH tb_vendas AS (
  SELECT
    *
  FROM silver.olist.item_pedido AS t1

  LEFT JOIN silver.olist.produto AS t2
  ON t1.idProduto = t2.idProduto

  INNER JOIN silver.olist.pedido AS t3
  ON t1.idPedido = t3.idPedido

  WHERE t2.descCategoria IS NOT NULL
),

tb_group AS (
  SELECT
    descCategoria,
    date(dtPedido) AS dataPedido,
    SUM(vlPreco) AS vlTotal
  FROM tb_vendas

  GROUP BY descCategoria, dataPedido
  ORDER BY descCategoria, dataPedido
) -- Faz o shift linear

SELECT 
  *,
  sum(vlTotal) OVER (PARTITION BY descCategoria ORDER BY dataPedido ASC) AS receitaAcum
FROM tb_group

-- COMMAND ----------

-- 7. PLUS: Selecione um dia de venda aleatório de cada vendedor
WITH tb_dia_vendedor AS (
  SELECT DISTINCT
    t1.idVendedor,
    date(t2.dtPedido) AS dtPedido
  FROM silver.olist.item_pedido AS t1

  INNER JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido
)

SELECT
  *
FROM tb_dia_vendedor

QUALIFY row_number() OVER (PARTITION BY idVendedor ORDER BY rand()) = 1
