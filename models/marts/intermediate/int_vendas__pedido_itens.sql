with
    pedidos as (
        select *
        from {{ ref('stg_erp__pedidos') }}
    )
    , pedido_itens as (
        select *
        from {{ ref('stg_erp__pedido_detalhes') }}
    )
    , joined_pedidos_itens as (
        select
            pedidos.id_pedido
            , pedidos.id_funcionario
            , pedidos.id_cliente
            , pedidos.id_transportadora
            , pedido_itens.id_produto
            , pedido_itens.desconto_perc
            , pedido_itens.preco_da_unidade
            , pedido_itens.quantidade
            , pedidos.frete
            , pedidos.data_do_pedido
            , pedidos.data_do_envio
            , pedidos.data_requerida_entrega
            , pedidos.destinatario
            , pedidos.endereco_destinatario
            , pedidos.cep_destinatario
            , pedidos.cidade_destinatario
            , pedidos.regiao_destinatario
            , pedidos.pais_destinatario
        from pedido_itens
        left join pedidos on
            pedido_itens.id_pedido = pedidos.id_pedido
    )
select *
from joined_pedidos_itens