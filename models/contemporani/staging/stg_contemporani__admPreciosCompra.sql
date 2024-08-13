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

        parse_date('%m/%d/%Y', substr(ctimestamp, 1, 10)) as fecha_compra

        --ccodigoproductoproveedor,
        --cidunidad,
        
    from source

)

select * from renamed
