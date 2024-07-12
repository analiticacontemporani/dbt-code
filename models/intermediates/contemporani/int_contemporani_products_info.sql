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

clasificaciones_valores as (

    select * from {{ ref('stg_contemporani__admClasificacionesValores') }}

),

product_info as (
    select 
        pro.*,
        prov.*,
        cv.cvalorclasificacion as tipo,
        cv2.cvalorclasificacion as subtipo,
        cv3.cvalorclasificacion as detalle,
    
    from productos pro
    left join clasificaciones_valores cv 
        on cv.cidvalorclasificacion = pro.cidvalorclasificacion1
    left join clasificaciones_valores cv2 
        on cv2.cidvalorclasificacion = pro.cidvalorclasificacion2
    left join clasificaciones_valores cv3 
        on cv3.cidvalorclasificacion = pro.cidvalorclasificacion3
    left join precio pre
        on pro.cidproducto = pre.cidproducto
    left join proveedores prov
        on pre.cidproveedor = prov.cidclienteproveedor
)

select * from product_info