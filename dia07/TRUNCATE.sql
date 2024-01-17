-- Databricks notebook source
SELECT * FROM sandbox.linuxtips.top5_pedidos_vitoros 

-- COMMAND ----------

TRUNCATE TABLE sandbox.linuxtips.top5_pedidos_vitoros -- Limpa a tabela (tabela vazia, NÃO GERA ERRO)

-- COMMAND ----------

SELECT * FROM sandbox.linuxtips.top5_pedidos_vitoros 
