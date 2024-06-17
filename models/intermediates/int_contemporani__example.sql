with 

source as (

    select * from {{ ref('stg_contemporani__example') }}

),

renamed as (

    select
        name

    from source

)

select * from renamed
