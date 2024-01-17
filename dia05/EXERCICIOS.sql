-- Databricks notebook source
-- 1. Qual a nota (média, mínima e máxima) de cada vendedor que tiveram vendas no ano de 2017? E o percentual de pedidos avaliados com nota 5
WITH tb_pedidos_dezessete AS (
  SELECT 
    idPedido 
  FROM silver.olist.pedido
  WHERE year(dtPedido) = '2017'
)

SELECT 
  t3.idVendedor,
  avg(t1.vlNota) AS notaMediaVendedor,
  min(t1.vlNota) AS notaMinVendedor,
  max(t1.vlNota) AS notaMaxVendedor,
  avg(CASE WHEN t1.vlNota = 5 THEN 1 ELSE 0 END) AS pctNota5
FROM silver.olist.avaliacao_pedido AS t1
INNER JOIN tb_pedidos_dezessete AS t2
ON t1.idPedido = t2.idPedido

INNER JOIN silver.olist.item_pedido AS t3
ON t2.idPedido = t3.idPedido
-- WHERE t3.idVendedor = 'ab9db4cf53b08b828b64dccaafc2d9f0' -- Usado apenas para validação de conhecimento Day-5
GROUP BY t3.idVendedor

ORDER BY t3.idVendedor

-- COMMAND ----------

-- 1. Qual a nota (média, mínima e máxima) de cada vendedor que tiveram vendas no ano de 2017? E o percentual de pedidos avaliados com nota 5 -- CORRECAO
WITH tb_pedidos_dezessete AS (
  SELECT DISTINCT -- Itens diferentes na mesma venda
    t1.idPedido,
    t2.idVendedor
  FROM silver.olist.pedido AS t1
  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE year(dtPedido) = '2017'
)

SELECT 
  t2.idVendedor,
  avg(t1.vlNota) AS notaMediaVendedor,
  min(t1.vlNota) AS notaMinVendedor,
  max(t1.vlNota) AS notaMaxVendedor,
  avg(CASE WHEN t1.vlNota = 5 THEN 1 ELSE 0 END) AS pctNota5
FROM silver.olist.avaliacao_pedido AS t1
INNER JOIN tb_pedidos_dezessete AS t2
ON t1.idPedido = t2.idPedido

GROUP BY t2.idVendedor

ORDER BY t2.idVendedor

-- COMMAND ----------

-- 2. Calcule o valor do pedido médio, o valor do pedido mais caro e mais barato de cada vendedor que realizaram vendas entre 2017-01-01 e 2017-06-30.
WITH tb_pedidos AS (
  SELECT 
    t1.idPedido,
    t2.idVendedor,
    t2.vlPreco -- Preço de item
  FROM silver.olist.pedido AS t1
  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE date(dtPedido) BETWEEN '2017-01-01' AND '2017-06-30'
  --Faltou agrupar para achar o preço de pedido
)

SELECT
  idVendedor,
  avg(vlPreco) AS pedidoMedio,
  max(vlPreco) AS pedidoMaximo,
  min(vlPreco) AS pedidoMinimo
FROM tb_pedidos 

GROUP BY  idVendedor

-- COMMAND ----------

-- 2. Calcule o valor do pedido médio, o valor do pedido mais caro e mais barato de cada vendedor que realizaram vendas entre 2017-01-01 e 2017-06-30. -- CORRECAO
WITH tb_item_pedidos AS (
  SELECT
    t2.idPedido,
    t2.idVendedor,
    t2.vlPreco

  FROM silver.olist.pedido AS t1

  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE date(dtPedido) BETWEEN '2017-01-01' AND '2017-06-30'
),

tb_pedido_receita AS (
  SELECT
    idVendedor,
    idPedido,
    sum(vlPreco) AS vlTotal

  FROM tb_item_pedidos

  GROUP BY idVendedor, idPedido
)

SELECT
  idVendedor,
  avg(vlTotal) AS pedidoMedio,
  max(vlTotal) AS pedidoMaximo,
  min(vlTotal) AS pedidoMinimo
FROM tb_pedido_receita

GROUP BY  idVendedor

-- COMMAND ----------

-- 2. Calcule o valor do pedido médio, o valor do pedido mais caro e mais barato de cada vendedor que realizaram vendas entre 2017-01-01 e 2017-06-30. -- CORRECAO 02
WITH tb_pedido_receita AS (
  SELECT
    t2.idPedido,
    t2.idVendedor,
    sum(t2.vlPreco) AS vlTotal

  FROM silver.olist.pedido AS t1

  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE date(dtPedido) BETWEEN '2017-01-01' AND '2017-06-30'

  GROUP BY t2.idPedido, t2.idVendedor
)

SELECT
  idVendedor,
  avg(vlTotal) AS pedidoMedio,
  max(vlTotal) AS pedidoMaximo,
  min(vlTotal) AS pedidoMinimo
FROM tb_pedido_receita
-- WHERE idVendedor = 'e2a1ac9bf33e5549a2a4f834e70df2f8' -- Usado apenas para validação de conhecimento Day-5
GROUP BY  idVendedor

-- COMMAND ----------

-- 3. Calcule a quantidade de pedidos por meio de pagamento que cada vendedor teve em seus pedidos entre 2017-01-01 e 2017-06-30.
WITH tb_pedido AS (
  SELECT
    t2.idPedido,
    t2.idVendedor
  FROM silver.olist.pedido AS t1

  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE date(dtPedido) BETWEEN '2017-01-01' AND '2017-06-30'

  GROUP BY t2.idPedido, t2.idVendedor
)

SELECT 
  t2.idVendedor,
  t1.descTipoPagamento,
  count(t1.descTipoPagamento) AS qtdeTipoPagamento
FROM silver.olist.pagamento_pedido AS t1
INNER JOIN tb_pedido AS t2
ON t1.idPedido = t2.idPedido

GROUP BY t2.idVendedor, t1.descTipoPagamento

ORDER BY t2.idVendedor, t1.descTipoPagamento

-- COMMAND ----------

-- 3. Calcule a quantidade de pedidos por meio de pagamento que cada vendedor teve em seus pedidos entre 2017-01-01 e 2017-06-30. -- CORRECAO
WITH tb_pedido AS (
  SELECT DISTINCT
    t2.idPedido,
    t2.idVendedor
  FROM silver.olist.pedido AS t1

  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE date(dtPedido) BETWEEN '2017-01-01' AND '2017-06-30'
),

tb_pedido_pagamento AS (
  SELECT 
    t2.idVendedor,
    t1.descTipoPagamento,
    t1.idPedido 
  FROM silver.olist.pagamento_pedido AS t1
  INNER JOIN tb_pedido AS t2
  ON t1.idPedido = t2.idPedido

)
SELECT 
  idVendedor,
  descTipoPagamento,
  count(DISTINCT idPedido) AS qtdePedido 
FROM tb_pedido_pagamento
-- WHERE idVendedor = '9de4643a8dbde634fe55621059d92273' -- Usado apenas para validação de conhecimento Day-5
GROUP BY idVendedor, descTipoPagamento

ORDER BY idVendedor, descTipoPagamento

-- COMMAND ----------

-- 4. Combine a query do exercício 2 e 3 de tal forma, que cada linha seja um vendedor, e que haja colunas para cada meio de pagamento (com a quantidade de pedidos) e as colunas das estatísticas do pedido do exercício 2 (média, maior valor e menor valor).
WITH tb_pedido_receita AS (
  SELECT
    t2.idPedido,
    t2.idVendedor,
    sum(t2.vlPreco) AS vlTotal

  FROM silver.olist.pedido AS t1

  INNER JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE date(dtPedido) BETWEEN '2017-01-01' AND '2017-06-30'

  GROUP BY t2.idPedido, t2.idVendedor
),

tb_sumario_pedido AS (
  SELECT
    idVendedor,
    avg(vlTotal) AS pedidoMedio,
    max(vlTotal) AS pedidoMaximo,
    min(vlTotal) AS pedidoMinimo
  FROM tb_pedido_receita

  GROUP BY  idVendedor
),

tb_pedido_pagamento AS (
  SELECT 
    t2.idVendedor,
    t1.descTipoPagamento,
    count(DISTINCT t1.idPedido) AS qtdePedido 
  FROM silver.olist.pagamento_pedido AS t1
  INNER JOIN tb_pedido_receita AS t2
  ON t1.idPedido = t2.idPedido

  GROUP BY t2.idVendedor, t1.descTipoPagamento

  ORDER BY t2.idVendedor, t1.descTipoPagamento

),

tb_pagamento_coluna AS (
  SELECT 
    idVendedor,
    SUM(CASE WHEN descTipoPagamento = 'boleto' THEN qtdePedido END) AS qtdeBoleto,
    SUM(CASE WHEN descTipoPagamento = 'credit_card' THEN qtdePedido END) AS qtdeCredit_card,
    SUM(CASE WHEN descTipoPagamento = 'voucher' THEN qtdePedido END) AS qtdeVoucher,
    SUM(CASE WHEN descTipoPagamento = 'debit_card' THEN qtdePedido END) AS qtdeDebit_card

  FROM tb_pedido_pagamento

  GROUP BY idVendedor
)

SELECT 
  t1.*,
  t2.qtdeBoleto,
  t2.qtdeCredit_card,
  t2.qtdeVoucher,
  t2.qtdeDebit_card

FROM tb_sumario_pedido AS t1

LEFT JOIN tb_pagamento_coluna AS t2
ON t1.idVendedor = t2.idVendedor
