with
    clientes as (
        select
        sk_cliente
        , id_cliente
        from {{ref('dim_clientes')}}
    )
    , funcionarios as (
        select
        sk_funcionario
        , id_funcionario
        from {{ref('dim_funcionarios')}}
    )
    , fornecedores as (
        select
        sk_fornecedor
        , id_fornecedor
        from {{ref('dim_fornecedores')}}
    )
    , transportadores as (
        select
          sk_transportador
        , id_transportador
        from {{ref('dim_transportadores')}}
    )
    , produtos as (
        select
        sk_produto
        , id_produto
        from {{ref('dim_produtos')}}
    )
    , pedidos_com_sk as (
        select
            pedidos.id_pedido
            , funcionarios.sk_funcionario as fk_funcionario
            , clientes.sk_cliente as fk_cliente
            , transportadores.sk_transportador as fk_transportador
            , pedidos.data_pedido
            , pedidos.regiao_entrega
            , pedidos.data_expedicao
            , pedidos.pais_entrega
            , pedidos.nome_entrega
            , pedidos.cep_entrega
            , pedidos.cidade_entrega
            , pedidos.frete
            , pedidos.endereco_entrega
            , pedidos.data_prevista
        from {{ref('stg_orders')}} pedidos
        left join funcionarios funcionarios on pedidos.id_funcionario = funcionarios.id_funcionario
        left join clientes clientes on pedidos.id_cliente = clientes.id_cliente
        left join transportadores transportadores on pedidos.id_transportador = transportadores.sk_transportador
    )
    , detalhes_dos_pedidos_sk as (
        select
            detalhes_dos_pedidos.id_pedido
            , produtos.sk_produto as fk_produto
            , detalhes_dos_pedidos.desconto
            , detalhes_dos_pedidos.preco_unitario
            , detalhes_dos_pedidos.quantidade
        from {{ref('stg_order_detail')}} detalhes_dos_pedidos
        left join produtos produtos on detalhes_dos_pedidos.id_produto = produtos.id_produto
    )

    
    , final as (
        select
        detalhes_dos_pedidos.id_pedido
        , pedidos.fk_funcionario
        , pedidos.fk_cliente
        , pedidos.data_pedido
        , pedidos.regiao_entrega
        , pedidos.data_expedicao
        , pedidos.nome_entrega
        , pedidos.cep_entrega
        , pedidos.cidade_entrega
        , pedidos.frete
        , pedidos.endereco_entrega
        , pedidos.data_prevista
        , detalhes_dos_pedidos.fk_produto
        , detalhes_dos_pedidos.desconto
        , detalhes_dos_pedidos.preco_unitario
        , detalhes_dos_pedidos.quantidade
        from pedidos_com_sk pedidos
        left join detalhes_dos_pedidos_sk detalhes_dos_pedidos on pedidos.id_pedido = detalhes_dos_pedidos.id_pedido
    )

select * from final