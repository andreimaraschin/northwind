with
    clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )

    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , pedido_item as (
        select *
        from {{ ref('int_vendas__pedido_itens') }}
    )

    , joined_tabelas as (
        select
             pedido_item.id_pedido
            , clientes.sk_cliente as fk_cliente
            , funcionarios.sk_funcionario as fk_funcionario
            , produtos.sk_produtos as fk_produto
            , pedido_item.id_transportadora
            , pedido_item.desconto_perc
            , pedido_item.preco_da_unidade
            , pedido_item.quantidade
            , pedido_item.frete
            , pedido_item.data_do_pedido
            , pedido_item.data_do_envio
            , pedido_item.data_requerida_entrega
            , pedido_item.destinatario
            , pedido_item.endereco_destinatario
            , pedido_item.cep_destinatario
            , pedido_item.cidade_destinatario
            , pedido_item.regiao_destinatario
            , pedido_item.pais_destinatario
            , clientes.nome_cliente
            , funcionarios.func_nome
            , funcionarios.id_gerente
            , produtos.nome_produto
            , produtos.nome_categoria
            , produtos.nome_fornecedor
            , produtos.esta_descontinuado
        from pedido_item
        left join clientes on pedido_item.id_cliente = clientes.id_cliente
        left join funcionarios on pedido_item.id_funcionario = funcionarios.id_func
        left join produtos on pedido_item.id_produto = produtos.id_produto
    )

     , transformacoes as (
        select
            {{ dbt_utils.generate_surrogate_key(['id_pedido', 'fk_produto']) }} as sk_venda
            , *
            , preco_da_unidade * quantidade as total_bruto
            , (1 - desconto_perc) * preco_da_unidade * quantidade as total_liquido
            , case
                when desconto_perc > 0 then true
                when desconto_perc = 0 then false
                else false
                end as desconto
        from joined_tabelas
    )
select *
from transformacoes