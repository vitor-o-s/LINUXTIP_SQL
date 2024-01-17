-- Databricks notebook source
DROP TABLE IF EXISTS sandbox.linuxtips.usuarios_vitoros;

CREATE TABLE sandbox.linuxtips.usuarios_vitoros (
    id int,
    nome string,
    idade int,
    profissao string
);

INSERT INTO sandbox.linuxtips.usuarios_vitoros
VALUES (1, 'Téo', 31, 'Streamer'),
       (2, 'Nah', 32, 'Artesã'),
       (3, 'Mah', 17, 'Estudante'),
       (4, 'Ana', 27, 'Dev'),
       (5, 'João', 45, 'Dev'),
       (6, 'Marcelo', 12, 'Eng. Mecânico'),
       (7, 'Zé', 41, 'Streamer'),
       (8, 'Diná', 33, 'Eng. Mecânico'),
       (9, 'Carlos', 51, 'Analista Financeiro'),
       (10, 'Jeff', 23, 'Cientista de Dados'),
       (11, 'Alex', 18, 'Analista de Dados')
;

SELECT * FROM sandbox.linuxtips.usuarios_vitoros;

-- COMMAND ----------

DELETE FROM sandbox.linuxtips.usuarios_vitoros -- desta forma é igual truncate, deleta todos os dados

-- COMMAND ----------

DELETE FROM sandbox.linuxtips.usuarios_vitoros WHERE idade < 18 -- Remove usuários menor de idade

-- COMMAND ----------

DELETE FROM sandbox.linuxtips.usuarios_vitoros WHERE idade > 40 and profissao = 'Streamer' -- Remove streamer com mais de 40 anos 

-- COMMAND ----------

DELETE FROM sandbox.linuxtips.usuarios_vitoros WHERE 
idade > (SELECT avg(idade) FROM sandbox.linuxtips.usuarios_vitoros)
-- Remove pessoas com idade acima da média 

-- COMMAND ----------

SELECT * FROM sandbox.linuxtips.usuarios_vitoros;
