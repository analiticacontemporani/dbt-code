with

final as (

    select
        cnombrealmacen as Almacen,
        tipo,
        subtipo,
        detalle,
        ccodigoproducto as Codigo,
        cnombreproducto as Producto,
        existencia as Existencia,
        cprecio1 as Precio

    from {{ ref('int_contemporani__ventas') }}

)

select * from final