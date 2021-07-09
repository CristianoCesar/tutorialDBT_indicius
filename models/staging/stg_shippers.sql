with
    dados_fonte as (
        select
            row_number() over (order by shipper_id) as sk_transportador --chave auto-incremental
            , shipper_id as id_transportador
            , company_name as nome_da_companhia
            , phone as telefone
            
        from {{source('northwind_raw_data','shippers')}}
    )

select *
from dados_fonte