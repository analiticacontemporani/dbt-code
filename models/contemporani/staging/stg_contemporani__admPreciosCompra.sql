with 

source as (

    select * from {{ source('contemporani', 'admPreciosCompra') }}

),

renamed as (

    select
        cidautoincsql,
        cidproducto,
        cidproveedor,
        cpreciocompra,
        cidmoneda,
        ccodigoproductoproveedor,
        cidunidad,
        
        parse_date('%m/%d/%Y', substr(ctimestamp, 1, 10)) as fecha_compra

    from source

)

select * from renamed
