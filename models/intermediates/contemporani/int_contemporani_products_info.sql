with 

productos as (
    select *
    from {{ ref('stg_contemporani__admProductos') }}
),

precio as (
    select distinct cidproducto, cidproveedor
    from {{ ref('stg_contemporani__admPreciosCompra') }}
),

proveedores as (
    select 
        cidclienteproveedor,
        ccodigocliente,
        crazonsocial as proveedor,
        ctipocliente,
        cestatus
    from {{ ref('stg_contemporani__admClientes') }}
),

product_info as (
    select 
        pro.*,
        prov.*
    
    from productos pro
    left join precio pre
        on pro.cidproducto = pre.cidproducto
    left join proveedores prov
        on pre.cidproveedor = prov.cidclienteproveedor
)

select * from product_info