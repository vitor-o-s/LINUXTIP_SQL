-- Databricks notebook source
CREATE TABLE sandbox.linuxtips.usuarios_vitoros (
  id INT,
  nome STRING,
  idade INT
);

-- COMMAND ----------

SELECT * FROM  sandbox.linuxtips.usuarios_vitoros

-- COMMAND ----------

INSERT INTO sandbox.linuxtips.usuarios_vitoros (id, nome, idade) VALUES (1, 'Nome1', 18)

-- COMMAND ----------

SELECT * FROM  sandbox.linuxtips.usuarios_vitoros

-- COMMAND ----------

INSERT INTO sandbox.linuxtips.usuarios_vitoros (id, nome, idade) --schema é opcional desde que insira valores na ordem
VALUES (2, 'Nome2', 19), (3, 'Nome3', 20)

-- COMMAND ----------

SELECT * FROM  sandbox.linuxtips.usuarios_vitoros

-- COMMAND ----------

INSERT INTO sandbox.linuxtips.usuarios_vitoros (id, nome) --schema é opcional desde que insira valores na ordem
VALUES (4, 'Nome4')

-- COMMAND ----------

SELECT * FROM  sandbox.linuxtips.usuarios_vitoros

-- COMMAND ----------

INSERT INTO sandbox.linuxtips.usuarios_vitoros --schema é opcional desde que insira valores na ordem
VALUES (5, 'Nome5', 21)

-- COMMAND ----------

SELECT * FROM  sandbox.linuxtips.usuarios_vitoros

-- COMMAND ----------

SELECT * FROM silver.olist.cliente LIMIT 10

-- COMMAND ----------

CREATE TABLE sandbox.linuxtips.cliente_olist_vitoros(
  id STRING,
  estado STRING
)

-- COMMAND ----------

INSERT INTO sandbox.linuxtips.cliente_olist_vitoros 

SELECT 
  idCliente As id, 
  descUF AS estado 
FROM silver.olist.cliente 
LIMIT 10

-- COMMAND ----------

SELECT * FROM sandbox.linuxtips.cliente_olist_vitoros

-- COMMAND ----------

WITH tb_rj AS (
  SELECT
    *
  FROM silver.olist.cliente
  WHERE descUF = 'RJ'
)

INSERT INTO sandbox.linuxtips.cliente_olist_vitoros

SELECT 
  idCliente AS id,
  descUF AS estado
FROM tb_rj
LIMIT 10

-- COMMAND ----------

SELECT * FROM sandbox.linuxtips.cliente_olist_vitoros
