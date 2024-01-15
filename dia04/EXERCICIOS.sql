-- Databricks notebook source
-- 1. Qual categoria tem mais produtos vendidos?
SELECT
  t2.descCategoria,
  count(t1.idProduto) AS qtdProdutos -- Pode ser um count(*)
FROM silver.olist.item_pedido AS t1
LEFT JOIN silver.olist.produto AS t2
ON t1.idProduto = t2.idProduto

GROUP BY t2.descCategoria

ORDER BY qtdProdutos DESC

LIMIT 5

-- COMMAND ----------

-- 2. Qual categoria tem produtos mais caros, em média? E Mediana?
SELECT 
  t2.descCategoria,
  avg(t1.vlPreco) AS avgPreco,
  percentile(t1.vlPreco, 0.5) AS medianaPreco
FROM silver.olist.item_pedido AS t1
LEFT JOIN silver.olist.produto AS t2
ON t1.idProduto = t2.idProduto

GROUP BY t2.descCategoria

ORDER BY avgPreco DESC

LIMIT 3

-- COMMAND ----------

-- 3. Qual categoria tem maiores fretes, em média?
SELECT 
  t2.descCategoria,
  avg(t1.vlFrete) AS avgFrete
FROM silver.olist.item_pedido AS t1
LEFT JOIN silver.olist.produto AS t2
ON t1.idProduto = t2.idProduto

GROUP BY t2.descCategoria

ORDER BY avgFrete DESC

LIMIT 3

-- COMMAND ----------

-- 4. Os clientes de qual estado pagam mais frete, em média?
SELECT
  t3.descUF,
  avg(vlFrete) as avgFreteItem
FROM silver.olist.pedido AS t1
LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido
LEFT JOIN silver.olist.cliente AS t3
ON t1.idCliente = t3.idCliente

GROUP BY t3.descUF

ORDER BY avgFreteItem DESC

LIMIT 1

-- COMMAND ----------

-- 4. Os clientes de qual estado pagam mais frete, em média? -- CORRECAO
SELECT
  t3.descUF,
  sum(t1.vlFrete) / count(DISTINCT t1.idPedido) AS avgFrete,
  avg(t1.vlFrete) as avgFreteItem,
  sum(t1.vlFrete) / count(DISTINCT t2.idCliente) AS avgFreteCliente
FROM silver.olist.item_pedido AS t1
INNER JOIN silver.olist.pedido AS t2
ON t1.idPedido = t2.idPedido
LEFT JOIN silver.olist.cliente AS t3
ON t2.idCliente = t3.idCliente

GROUP BY t3.descUF

ORDER BY avgFrete DESC

LIMIT 3

-- COMMAND ----------

-- 5. Clientes de quais estados avaliam melhor, em média? Proporção de 5?
SELECT 
  t1.descUF,
  avg(t3.vlNota) as avgAvaliacao,
  avg(CASE WHEN  t3.vlNota = 5 THEN 1 ELSE 0 END) as prop5
FROM silver.olist.cliente AS t1
LEFT JOIN silver.olist.pedido AS t2
ON t1.idCliente = t2.idCliente

INNER JOIN silver.olist.avaliacao_pedido AS t3 -- Apenas pedidos avaliados
ON t2.idPedido = t3.idPedido

GROUP BY t1.descUF

ORDER BY avgAvaliacao DESC 

-- SUGESTÃO/CORREÇÃO 
--SELECT 
--  t3.descUF,
--  avg(t1.vlNota) as avgAvaliacao,
--  avg(CASE WHEN  t3.vlNota = 5 THEN 1 ELSE 0 END) as prop5
--FROM silver.olist.avaliacao_pedido AS t1
--INNER JOIN silver.olist.pedido AS t2
--ON t1.idPedido = t2.idPedido
--INNER JOIN silver.olist.avaliacao_pedido AS t3
--ON t2.idCliente = t3.idCliente
--GROUP BY t1.descUF
--ORDER BY avgAvaliacao DESC 

-- COMMAND ----------

-- 6. Vendedores de quais estados têm as piores reputações?
SELECT 
  --t1.idVendedor,
  t1.descUF, 
  avg(t3.vlNota) as avgNota 
FROM silver.olist.vendedor AS t1
LEFT JOIN silver.olist.item_pedido AS t2
ON t1.idVendedor = t2.idVendedor
INNER JOIN silver.olist.avaliacao_pedido AS t3
ON t2.idPedido = t3.idPedido

GROUP BY t1.descUF

ORDER BY avgNota

LIMIT 5

-- SUGESTÃO TEO
--SELECT 
--  t3.descUF, 
--  avg(t1.vlNota) as avgNota 
--FROM silver.olist.avaliacao_pedido AS t1
--INNER JOIN silver.olist.item_pedido AS t2
--ON t1.idPedido = t2.idPedido
--LEFT JOIN silver.olist.vendedor AS t3
--ON t2.idVendedor = t3.idVendedor
--GROUP BY t3.descUF
--ORDER BY avgNota
--LIMIT 5

-- COMMAND ----------

-- 7. Quais estados de clientes levam mais tempo para a mercadoria chegar?
SELECT
  t2.descUF,
  avg(date_diff(t1.dtEntregue, t1.dtPedido)) as qtdDias
FROM silver.olist.pedido AS t1
LEFT JOIN silver.olist.cliente AS t2
ON t1.idCliente = t2.idCliente
WHERE dtEntregue IS NOT NULL

GROUP BY t2.descUF

ORDER BY qtdDias

-- COMMAND ----------

-- 8. Qual meio de pagamento é mais utilizado por clientes do RJ?
SELECT
  t1.descTipoPagamento,
  count(DISTINCT t1.idPedido) AS qtdPedidos
FROM silver.olist.pagamento_pedido AS t1
INNER JOIN silver.olist.pedido AS t2
ON t1.idPedido = t2.idPedido

LEFT JOIN silver.olist.cliente AS t3
ON t2.idCliente = t3.idCliente

WHERE t3.descUF = 'RJ'

GROUP BY t1.descTipoPagamento

ORDER BY qtdPedidos DESC

-- COMMAND ----------

-- 9. Qual estado sai mais ferramentas?
SELECT
  t3.descUF,
  count(*) AS qtdFerramentas
FROM silver.olist.item_pedido AS t1
LEFT JOIN silver.olist.produto AS t2
ON t1.idProduto = t2.idProduto
--AND t2.descCategoria LIKE '%ferramentas%'

LEFT JOIN silver.olist.vendedor as t3
ON t1.idVendedor = t3.idVendedor

WHERE t2.descCategoria LIKE '%ferramentas%'

GROUP BY t3.descUF

ORDER BY qtdFerramentas DESC

-- COMMAND ----------

-- 10. Qual estado tem mais compras por cliente?
SELECT 
  t2.descUF,
  count(DISTINCT t1.idPedido) AS qtdPedido,
  count(DISTINCT t2.idClienteUnico) AS qtdClienteUnico,
  count(DISTINCT t1.idPedido) / count(DISTINCT t2.idClienteUnico) AS avgPedidoCliente
FROM silver.olist.pedido AS t1
LEFT JOIN silver.olist.cliente AS t2
ON t1.idCliente = t2.idCliente

GROUP BY t2.descUF

ORDER BY avgPedidoCliente DESC
