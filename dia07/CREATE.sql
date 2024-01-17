-- Databricks notebook source
CREATE TABLE sandbox.linuxtips.top5_pedidos_vitoros AS (
  SELECT * 
  FROM silver.olist.pedido 
  LIMIT 5
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sandbox.linuxtips.top5_pedidos_vitoros AS (
  SELECT * 
  FROM silver.olist.pedido 
  ORDER BY RAND()
  LIMIT 5
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sandbox.linuxtips.top50_pedidos_vitoros AS (
  SELECT idPedido 
  FROM silver.olist.pedido 
  ORDER BY RAND()
  LIMIT 5
)

-- COMMAND ----------

SELECT * FROM sandbox.linuxtips.top50_pedidos_vitoros

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sandbox.linuxtips.nova_tabela_vazia_vitoros (
  descNome STRING,
  vlIdade INT,
  vlSalario FLOAT
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sandbox.linuxtips.ex07_dia06_vitoros
WITH tb_dia_venvedor AS (
  SELECT DISTINCT
    t1.idVendedor,
    date(t2.dtPedido) AS dtPedido
  FROM silver.olist.item_pedido AS  t1

  INNER JOIN silver.olist.pedido AS t2
  ON t1.idPedido = t2.idPedido
)

SELECT
  *,
  row_number() OVER (PARTITION BY idVendedor ORDER BY rand()) AS rnVendedor

FROM tb_dia_venvedor

QUALIFY rnVendedor <= 2
