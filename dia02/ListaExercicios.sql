-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC 1. Selecione todos os clientes paulistanos.
-- MAGIC 2. Selecione todos os clientes paulistas.
-- MAGIC 3. Selecione todos os vendedores cariocas e paulistas.
-- MAGIC 4. Selecione produtos de perfumaria e bebes com altura maior que 5cm.
-- MAGIC 5. Selecione os pedidos com mais de um item.
-- MAGIC 6. Selecione os pedidos que o frete é mais caro que o item.
-- MAGIC 7. Lista de pedidos que ainda não foram enviados.
-- MAGIC 8. Lista de pedidos que foram entregues com atraso.
-- MAGIC 9. Lista de pedidos que foram entregues com 2 dias de antecedência.
-- MAGIC 10. Selecione os pedidos feitos em dezembro de 2017 e entregues com atraso.
-- MAGIC 11. Selecione os pedidos com avaliação maior ou igual que 4.
-- MAGIC 12. Selecione pedidos pagos com cartão de crédito com duas ou mais parcelas menores que R$40,00.

-- COMMAND ----------

SELECT * FROM silver.olist.cliente WHERE descCidade = 'sao paulo'

-- COMMAND ----------

SELECT * FROM silver.olist.cliente WHERE descUF = 'SP'

-- COMMAND ----------

SELECT * FROM silver.olist.vendedor WHERE descCidade = 'rio de janeiro' OR descUF = 'SP'

-- COMMAND ----------

SELECT * FROM silver.olist.produto WHERE descCategoria IN ('perfumaria', 'bebes') AND vlAlturaCm > 5

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido WHERE idPedidoItem >= 2

-- COMMAND ----------

SELECT * FROM silver.olist.item_pedido WHERE vlFrete > vlPreco 

-- COMMAND ----------

SELECT * FROM silver.olist.pedido WHERE descSituacao IN ('invoiced', 'processing', 'calceled', 'unavailable')
-- SELECT * FROM silver.olist.pedido WHERE dtEnvio IS NULL

-- COMMAND ----------

SELECT * FROM silver.olist.pedido WHERE date(dtEstimativaEntrega) < date(dtEntregue)

-- COMMAND ----------

SELECT * FROM silver.olist.pedido WHERE date_diff(date(dtEstimativaEntrega), date(dtEntregue)) = 2

-- COMMAND ----------

SELECT * FROM silver.olist.pedido WHERE year(dtPedido) = 2017 AND month(dtPedido) = 12 AND date(dtEstimativaEntrega) < date(dtEntregue)

-- COMMAND ----------

SELECT * FROM silver.olist.avaliacao_pedido WHERE vlNota >= 4

-- COMMAND ----------

SELECT * FROM silver.olist.pagamento_pedido WHERE descTipoPagamento = 'credit_card' AND nrParcelas >= 2 AND vlPagamento/nrParcelas < 40.00
