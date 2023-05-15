-- Databricks notebook source
SELECT
    COUNT(*),
    COUNT(1)
FROM silver.olist.pedido

-- COMMAND ----------

SELECT COUNT(descSituacao
FROM silver.olist.pedido -- Linhas não nulas deste campo

-- COMMAND ----------

SELECT COUNT(descSituacao),
       COUNT(DISTINCT descSituacao)
FROM silver.olist.pedido -- Linhas não nulas deste campo e linhas distintas do campo

-- COMMAND ----------

SELECT COUNT(idPedido),
       COUNT(DISTINCT idPedido)
FROM silver.olist.pedido
-- Count de coluna é mais performático, caso seja chave primária não é necessário o distinct
