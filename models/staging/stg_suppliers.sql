with
    dados_fonte as (
        select
            row_number() over (order by supplier_id) as sk_fornecedor--chave auto-incremental
            ,  supplier_id as id_fornecedor
            ,  company_name as nome_da_empresa
            ,  contact_name as nome_do_responsavel 
            ,  contact_title as assunto
            ,  address as endereco
            ,  city as cidade
            ,  region as regiao
            ,  postal_code as cep
            ,  country as pais
            ,  phone as telefone
            ,  homepage as site
            ,  fax
        from {{source('northwind_raw_data','suppliers')}}
    )

select *
from dados_fonte