with

final as (

    select
        *

    from {{ ref('int_contemporani__ventas') }}

)

select * from final