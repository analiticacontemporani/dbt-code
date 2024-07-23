with 

source as (

    select * from {{ source('contemporani_banca', 'Cuentas') }}

),

renamed as (

    select
        id,
        rowversion,
        codigo,
        nombre,
        nomidioma,
        tipo,
        esbaja,
        ctamayor,
        ctaefectivo,
        fecharegistro,
        sistorigen,
        idmoneda,
        digagrup,
        idsegneg,
        segnegmovtos,
        afectable,
        timestamp,
        idrubro,
        consume,
        idagrupadorsat,
        conceptosconsume

    from source

)

select * from renamed
