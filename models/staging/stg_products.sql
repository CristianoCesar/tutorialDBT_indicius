with
    dados_fonte as (
        select
            row_number() over (order by product_id) as sk_produto --chave auto-incremental
            , product_id as id_produto
            , product_name as nome_produto
            , units_in_stock as unidades_em_estoque
            , category_id as id_categoria
            , unit_price as preco_unitario
            , quantity_per_unit as quantidade_por_unidade
            , reorder_level as nivel_estoque
            , supplier_id as id_fornecedor
            , units_on_order as unidades_encomendadas
            ,
              case
                when discontinued = 0 then 'No'
                else 'Yes'
              end as is_discontinued

        from {{source('northwind_raw_data','products')}}
    )

select *
from dados_fonte