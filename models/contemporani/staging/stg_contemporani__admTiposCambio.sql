with 

source as (

    select * from {{ source('contemporani', 'admTiposCambio') }}

),

renamed as (

    select
        cidtipocambio,
        cidmoneda,
        DATE(cfecha) as fecha_cambio,
        cimporte,

    from source

)

select * from renamed
