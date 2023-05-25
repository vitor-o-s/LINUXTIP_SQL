-- Databricks notebook source
SELECT avg(vlPreco), -- Média aritimética
       sum(vlPreco)/ count(vlPreco), -- média na mão
       min(vlPreco), -- Mínimo de um campoo
       max(vlFrete), -- Máximo de frete pago
       std(vlFrete), -- desvio padrão
       percentile(vlFrete, 0.5) -- mediana
FROM silver.olist.item_pedido
