-- Databricks notebook source
-- 1. Qual pedido com maior valor de frete? E o menor?
SELECT max(vlFrete), 
       min(vlFrete)
FROM silver.olist.item_pedido

-----------------------------CORRIGIR

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
ORDER BY 2 DESC

-- COMMAND ----------

-- 4. Qual dia tivemos mais pedidos?
SELECT count(DISTINCT idPedido),
       date(date_trunc('DAY', dtPedido))
FROM silver.olist.pedido
GROUP BY date(date_trunc('DAY', dtPedido))
ORDER BY 1 DESC

-- COMMAND ----------

5. Quantos vendedores são do estado de São Paulo?
6. Quantos vendedores são de Presidente Prudente?
7. Quantos clientes são do estado do Rio de Janeiro?
8. Quantos produtos são de construção?
9. Qual o valor médio de um pedido? E do frete?
10. Em média os pedidos são de quantas parcelas de cartão? E o valor médio por parcela?
11. Quanto tempo em média demora para um pedido chegar depois de aprovado?
12. Qual estado tem mais vendedores?
13. Qual cidade tem mais clientes?
14. Qual categoria tem mais itens?
15. Qual categoria tem maior peso médio de produto?
16. Qual a série histórica de pedidos por dia? E receita?
17. Qual produto é o campeão de vendas? Em receita? Em quantidade?
