-- Databricks notebook source
-- 1. Qual pedido com maior valor de frete? E o menor?
SELECT max(vlFrete), 
       min(vlFrete)
FROM silver.olist.item_pedido

-- COMMAND ----------

-- 1. Qual pedido com maior valor de frete? E o menor? - Correta
SELECT idPedido, 
       sum(vlFrete) AS totalFrete
FROM silver.olist.item_pedido
GROUP BY idPedido
ORDER BY totalFrete DESC -- ASC para o menor

-- COMMAND ----------

-- 2. Qual vendedor tem mais pedidos?
SELECT idVendedor,
       count(DISTINCT idPedido) AS qtdPedidos
FROM silver.olist.item_pedido
GROUP BY idVendedor
ORDER BY 2 DESC
LIMIT 1

-- COMMAND ----------

-- 3. Qual vendedor tem mais itens vendidos? E o com menos?
SELECT idVendedor,
       count(idProduto) AS qtItem
FROM silver.olist.item_pedido
GROUP BY idVendedor
ORDER BY 2 DESC -- ASC para 'E o com menos'

-- COMMAND ----------

-- 4. Qual dia tivemos mais pedidos?
SELECT count(DISTINCT idPedido),
       date(date_trunc('DAY', dtPedido))
FROM silver.olist.pedido
GROUP BY date(date_trunc('DAY', dtPedido))
ORDER BY 1 DESC

-- COMMAND ----------

-- 5. Quantos vendedores são do estado de São Paulo?
SELECT count(*) FROM silver.olist.vendedor WHERE descUF = 'SP'
-- SELECT count(DISTINCT idVendedor) AS qtVendedor FROM silver.olist.vendedor WHERE descUF = 'SP' -- Versão Teo

-- COMMAND ----------

-- 6. Quantos vendedores são de Presidente Prudente?
SELECT count(*) FROM silver.olist.vendedor WHERE descCidade = 'presidente prudente'
-- SELECT count(DISTINCT idVendedor) AS qtVendedor FROM silver.olist.vendedor WHERE descCidade = 'presidente prudente' -- Versão Teo

-- COMMAND ----------

-- 7. Quantos clientes são do estado do Rio de Janeiro?
SELECT count(*) FROM silver.olist.cliente WHERE descUF = 'RJ'
-- SELECT count(DISTINCT idCliente) AS qtCliente FROM silver.olist.cliente WHERE descUF = 'RJ' -- Versao Teo

-- COMMAND ----------

-- 8. Quantos produtos são de construção?
SELECT count(*) FROM silver.olist.produto WHERE descCategoria LIKE '%construcao%'
-- SELECT count(DISTINCT idProduto) AS qtProduto FROM silver.olist.produto WHERE descCategoria LIKE '%construcao%' -- Versao Teo
-- SELECT DISTINCT descCategoria FROM silver.olist.produto  WHERE descCategoria LIKE 'construcao_%' retorna 5 categorias

-- COMMAND ----------

-- 9. Qual o valor médio de um pedido? E do frete?
SELECT avg(vlPreco) AS valorMedioPedido,
       avg(vlFrete) AS valorMedidoFrete
FROM silver.olist.item_pedido

-- COMMAND ----------

-- 9. Qual o valor médio de um pedido? E do frete? -- CORRETA
SELECT sum(vlPreco)/ count(DISTINCT idPedido) AS vlMedioPedido,
       sum(vlFrete)/ count(DISTINCT idPedido) AS vlMedioFrete,
       avg(vlPreco) AS avgPedido, -- valor médio item
       avg(vlFrete) AS avgFrete -- valor frete medio item
FROM silver.olist.item_pedido

-- COMMAND ----------

-- 10. Em média os pedidos são de quantas parcelas de cartão? E o valor médio por parcela?
SELECT avg(nrParcelas) AS valorMedioNrParcelas,
       avg(vlPagamento/nrParcelas) AS valorMedioParcela
FROM silver.olist.pagamento_pedido
WHERE descTipoPagamento = 'credit_card'

-- COMMAND ----------

-- 11. Quanto tempo em média demora para um pedido chegar depois de aprovado?
SELECT avg(date_diff(dtEntregue, dtAprovado)) AS tempoMedioEmDiasEntrega
FROM silver.olist.pedido
WHERE dtAprovado IS NOT NULL AND dtEntregue IS NOT NULL

-- COMMAND ----------

-- 11. Quanto tempo em média demora para um pedido chegar depois de aprovado? -- Forma do Teo
SELECT avg(date_diff(dtEntregue, dtAprovado)) AS tempoMedioEmDiasEntrega
FROM silver.olist.pedido
WHERE descSituacao = 'delivered'

-- COMMAND ----------

-- 12. Qual estado tem mais vendedores?
SELECT count(*) AS qtdVendedoresEStado,
       descUF
FROM silver.olist.vendedor
GROUP BY descUF
ORDER BY 1 DESC

-- COMMAND ----------

-- 13. Qual cidade tem mais clientes?
SELECT count(*) AS qtdVendedoresEStado,
       count(DISTINCT idClienteUnico) AS qtClienteDistinto, -- Clientes unicos
       descCidade
FROM silver.olist.cliente
GROUP BY descCidade
ORDER BY 2 DESC

-- COMMAND ----------

-- 14. Qual categoria tem mais itens?
SELECT count(*) AS qtdItensCategoria,
       descCategoria
FROM silver.olist.produto
GROUP BY descCategoria
ORDER BY 1 DESC

-- COMMAND ----------

-- 15. Qual categoria tem maior peso médio de produto?
SELECT avg(vlPesoGramas)/1000 AS mediaPesoEmQuilos,
       descCategoria
FROM silver.olist.produto
GROUP BY descCategoria
ORDER BY 1 DESC

-- COMMAND ----------

-- 16. Qual a série histórica de pedidos por dia? E receita?
SELECT count(DISTINCT idPedido) qtPedido,
       date(dtPedido) AS diaPedido
FROM silver.olist.pedido
GROUP BY diaPedido
ORDER BY 2

-- COMMAND ----------

-- 17. Qual produto é o campeão de vendas? Em receita? Em quantidade?
SELECT idProduto,
       count(*) AS qtdVenda,
       sum(vlPreco) AS vlReceita
FROM silver.olist.item_pedido
GROUP BY idProduto
ORDER BY 3 DESC
