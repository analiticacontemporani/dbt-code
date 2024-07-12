-- Falta:
    -- Revisar como identificar de donde fue la venta San Pedro / Cancun
with

documentos as (
    select
        ciddocumento,
        ciddocumentode,
        cidconceptodocumento,
        cseriedocumento,
        cfecha,
        ccancelado

    from {{ ref('stg_contemporani__admDocumentos') }}

    where
        ciddocumentode in (4,5)  -- Factura y Devolucion sobre venta
        and ccancelado = 0  -- No cancelada
),

movimientos as (
    select
        cidmovimiento,
        ciddocumento,
        cidproducto,
        cidalmacen,
        cunidades,
        cunidadescapturadas,
        cneto,
        ccostoespecifico,
        cdescuento1

    from {{ ref('stg_contemporani__admMovimientos') }}
),

productos as (
    select 
        cidproducto,
        ccodigoproducto,
        cnombreproducto,
        cstatusproducto,
        proveedor,
        tipo,
        subtipo,
        detalle

    from {{ ref('int_contemporani_products_info') }}
),

almacenes as (
    select 
        cidalmacen,
        ccodigoalmacen,
        cnombrealmacen

    from {{ ref('stg_contemporani__admAlmacenes') }}
),

conceptos as (
    select 
        cidconceptodocumento,
        cnombreconcepto
    from {{ ref('stg_contemporani__admConceptos') }}
),

data as (
    select 
        doc.cfecha,
        doc.ciddocumento,
        doc.ciddocumentode,
        c.cnombreconcepto,
        c.cidconceptodocumento,
        mov.cunidades as piezas,
        mov.cunidadescapturadas,
        mov.cneto as importe_ventas,
        mov.ccostoespecifico as importe_costo,
        mov.cdescuento1 as descuento,
        pro.ccodigoproducto as codigo_producto,
        pro.cnombreproducto as nombre_producto,
        pro.cstatusproducto as status_producto,
        pro.proveedor,
        pro.tipo,
        pro.subtipo,
        pro.detalle

    from documentos doc 
    join conceptos c
        on doc.cidconceptodocumento = c.cidconceptodocumento
    join movimientos mov
        on doc.ciddocumento = mov.ciddocumento
    join almacenes a
        on mov.cidalmacen = a.cidalmacen 
    join productos pro
        on mov.cidproducto = pro.cidproducto
),

formulas as (
    select 
        cfecha,
        ciddocumento,

        case
            when ciddocumentode = 4 then 'Ventas'
            when ciddocumentode = 5 then 'Devolucion sobre Ventas'
            else 'Otros'
        end as tipo_de_factura,

        cnombreconcepto,
        cidconceptodocumento,
        piezas ,
        importe_ventas ,
        importe_costo ,
        descuento ,

        importe_ventas - descuento as venta_neta,
        importe_ventas - descuento - importe_costo as utilidad,
        NULLIF((importe_ventas - descuento - importe_costo),0) / NULLIF((importe_ventas - descuento),0) as margen,

        codigo_producto,
        nombre_producto,
        status_producto,
        proveedor,
        tipo,
        subtipo,
        detalle

        from data
)

select *
from formulas 
ORDER BY cfecha DESC 