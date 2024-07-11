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
        ctimestamp

    from source

)

select * from renamed
