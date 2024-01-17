-- Databricks notebook source
-- 1. Crie uma tabela em sandbox.linuxtips com o seu nome. Exemplo: sandbox.linuxtips.teo_calvo. Esta tabela contém os seguintes campos: 1. id -> inteiro 2. nome -> string 3. dt_nascimento -> date 4. profissao -> string 5. renda -> float 6. uf -> string 7. nacionalidade -> string
DROP TABLE IF EXISTS sandbox.linuxtips.vitoros;
CREATE TABLE IF NOT EXISTS sandbox.linuxtips.vitoros (
  id INT,
  nome STRING,
  dt_nascimento DATE,
  profissao STRING,
  renda FLOAT,
  uf STRING,
  nacionalidade STRING
)

-- COMMAND ----------

-- 2. Insira nesta tabela criada os seguintes registros utilizando a cláusula INSERT INTO:
INSERT INTO sandbox.linuxtips.vitoros (id, nome, dt_nascimento, profissao, renda, uf, nacionalidade)
VALUES
(1, 'Maria', "1989-01-18", 'Artesã', 1450.90, 'MG', 'Brasileira' ),
(2, 'José', "1987-06-25", 'Mecânico', 2756.87, 'SP', 'Brasileira' ),
(3, 'Manoel', "1995-09-13", 'Operador de máquinas pesadas', 3245.53, 'SP', 'Brasileira' ),
(4, 'Antônia', "1991-02-28", 'Tratorista', 3135.47, 'SC', 'Brasileira' ),
(5, 'Maria Eduarda', "1985-12-29", 'Serviço gerais', 1649.21, 'BA', 'Brasileira'),
(6, 'João de Deus', "1999-03-14", 'Manobrista', 2375.78, 'PE', 'Brasileira'),
(7, 'Eduardo', "2003-05-04", 'Atendente', 3157.06, 'AM', 'Haiti'),
(8, 'Mônica', "2006-10-09", 'Estudante', 550.00, 'SP', 'Brasileira'),
(9, 'Bruno', "1998-02-26", 'Encanador', 1459.98, 'MG', 'Brasileira'),
(10, 'Letícia', "1982-04-01", 'Marceneira', 1698.74, 'SP', 'Angolana'),
(11, 'Tomé', "1971-07-31", 'Porteiro', 2670.32, 'SP', 'Brasileira')

-- COMMAND ----------

SELECT * FROM sandbox.linuxtips.vitoros

-- COMMAND ----------

-- 3. O atendente Eduardo, id=7, ganhou um aumento de 15% em seu salário. Precisamos atualizar seus dados. Pode fazer isso?
UPDATE sandbox.linuxtips.vitoros SET renda=renda*1.15 WHERE id = 7

-- COMMAND ----------

-- 4. Maria Eduarda, id=5 foi promovida à copeira e seu novo salário será de R$2150,00. Vamos atualizar seus dados?
UPDATE sandbox.linuxtips.vitoros SET renda=2150, profissao='Copeira' WHERE id = 5

-- COMMAND ----------

-- 5. Manoel, id=3, saiu da empresa e solicitou a exclusão de seus dados. Como podemos fazer essa operação?
DELETE FROM sandbox.linuxtips.vitoros WHERE id = 3

-- COMMAND ----------

-- 6. O salário mínimo nacional aumentou 5%, como podemos atualizar todos os salários da tabela?
UPDATE sandbox.linuxtips.vitoros SET renda=round(renda*1.05, 2) 

-- COMMAND ----------

-- 7. O governo brasileiro está dando incentivo para empresas que valorizam a mão de obra imigrante. Portanto, todas as pessoas não brasileiras receberam um aumento de 2,5% em seus respectivos salários.
UPDATE sandbox.linuxtips.vitoros SET renda=round(renda*1.025,2) WHERE nacionalidade != 'Brasileira'
