with
    dados_fonte as (
        select
            row_number() over (order by employee_id) as sk_funcionario --chave auto-incremental
            , employee_id as id_funcionario
            , first_name ||" "|| last_name as nome_completo
            , first_name as nome
            , last_name as sobrenome
            , country as pais
            , city as cidade
            , postal_code as cep
            , hire_date as adimissao
            , extension as complemento
            , address as endereco
            , birth_date as data_de_nascimento
            , region as regiao
            , photo_path as caminho_foto
            , home_phone as telefone_residencial
            , reports_to 
            , title as titulo
            , title_of_courtesy
            , notes as notas     
        from {{source('northwind_raw_data','employees')}}
    )

select *
from dados_fonte