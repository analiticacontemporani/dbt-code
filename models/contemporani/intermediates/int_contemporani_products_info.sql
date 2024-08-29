-- Modelo que extrae todas las propiedades de los productos y sus clasificaciones
    -- Crea las nuevas categorias definidas por el cliente

-- Se creo ya que es siguiendo el principio DRY ya que esta informacion se requiere para todos los modelos
with 

productos as (
    select * from {{ ref('stg_contemporani__admProductos') }}
),

clasificaciones_valores as (

    select * from {{ ref('stg_contemporani__admClasificacionesValores') }}

),

product_info as (
    select 
        pro.*,
        cv.cvalorclasificacion as tipo,
        cv2.cvalorclasificacion as subtipo,
        cv3.cvalorclasificacion as subsubtipo,
        cv6.cvalorclasificacion as proveedor

    from productos pro
    left join clasificaciones_valores cv 
        on cv.cidvalorclasificacion = pro.cidvalorclasificacion1
    left join clasificaciones_valores cv2 
        on cv2.cidvalorclasificacion = pro.cidvalorclasificacion2
    left join clasificaciones_valores cv3 
        on cv3.cidvalorclasificacion = pro.cidvalorclasificacion3
    left join clasificaciones_valores cv6 on cv6.cidvalorclasificacion = pro.cidvalorclasificacion6
),

new_categories as (
    select 
        * except(tipo),
        tipo as tipo_base_de_datos,
        case
            when tipo NOT IN ('SALA', 'MESA', 'SILLA', 'BANCO', 'EXTERIOR', 'RECAMARA', 'LIBRERO', 'ACCESORIOS') then 'Categorizado Erroneamente'
            else tipo
        end as tipo

    from product_info
)

select * from new_categories order by ccodigoproducto