with
    dados_fonte as (
        select
            order_id as id_pedido
            , product_id as id_produto
            , discount as desconto
            , unit_price as preco_unitario
            , quantity as quantidade
        from {{source('northwind_raw_data','order_details')}}
    )

select *
from dados_fonte