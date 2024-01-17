-- Databricks notebook source
DROP TABLE sandbox.linuxtips.top5_pedidos_vitoros

-- COMMAND ----------

SELECT * FROM sandbox.linuxtips.top5_pedidos_vitoros -- Will not work

-- COMMAND ----------

CREATE TABLE sandbox.linuxtips.top5_pedidos_vitoros AS (
  SELECT * 
  FROM silver.olist.pedido 
  LIMIT 5
)

-- COMMAND ----------

SELECT * FROM sandbox.linuxtips.top5_pedidos_vitoros -- Will  work

-- COMMAND ----------

DROP TABLE IF EXISTS sandbox.linuxtips.top5_pedidos_vitoros;

CREATE TABLE sandbox.linuxtips.top5_pedidos_vitoros AS (
  SELECT * 
  FROM silver.olist.pedido 
  LIMIT 5
)

-- COMMAND ----------

SELECT * FROM sandbox.linuxtips.top5_pedidos_vitoros -- Will  work
