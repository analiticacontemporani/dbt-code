with 

source as (

    select * from {{ source('contemporani_banca', 'Asociaciones') }}

),

renamed as (

    select
        id,
        rowversion,
        idctasup,
        idsubctade,
        ctasup,
        subctade,
        tiporel,
        timestamp

    from source

)

select * from renamed
