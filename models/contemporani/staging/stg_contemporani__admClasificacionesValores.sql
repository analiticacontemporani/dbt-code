with 

source as (

    select * from {{ source('contemporani', 'admClasificacionesValores') }}

),

renamed as (

    select
        cidvalorclasificacion,
        cvalorclasificacion,
        cidclasificacion,
        ccodigovalorclasificacion,
        csegcont1,
        csegcont2,
        csegcont3

    from source

)

select * from renamed
