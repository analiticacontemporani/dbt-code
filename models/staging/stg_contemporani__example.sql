with 

source as (

    select * from {{ source('contemporani', 'example') }}

),

renamed as (

    select
        name

    from source

)

select * from renamed
