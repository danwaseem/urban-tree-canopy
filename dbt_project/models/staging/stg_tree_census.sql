/*
  stg_tree_census
  ---------------
  Cleans and standardises raw.tree_census produced by process_and_load.py.

  Key decisions:
  - Filters to status = 'Alive' (dead/stump rows stay in raw for auditability).
  - tree_dbh (diameter at breast height) is kept as a canopy size proxy.
  - loaded_at comes from the ingestion pipeline (not a surrogate timestamp).
  - boroname is derived from the 'borough' column in the source CSV.
  - x_32618/y_32618 are UTM Zone 18N metre coordinates written by process_and_load.py.
    geometry is not stored as a PostGIS type to avoid GeoAlchemy2 version sensitivity.
*/

with source as (
    select * from {{ source('raw', 'tree_census') }}
),

cleaned as (
    select
        -- identifiers
        cast(tree_id as integer)                                    as tree_id,
        cast(block_id as integer)                                   as block_id,

        -- timestamps
        cast(created_at as timestamp)                               as surveyed_at,
        loaded_at,

        -- location
        cast(latitude  as numeric(10, 7))                           as latitude,
        cast(longitude as numeric(10, 7))                           as longitude,
        address,
        cast(postcode as varchar)                                   as postcode,

        -- spatial context
        census_tract_id,
        boroname,

        -- tree attributes
        lower(trim(status))                                         as status,
        coalesce(nullif(lower(trim(health)), ''), 'unknown')        as health,
        lower(trim(spc_common))                                     as spc_common,
        lower(trim(spc_latin))                                      as spc_latin,
        cast(tree_dbh as numeric(6, 2))                             as tree_dbh,

        -- raster-derived
        cast(ndvi_value as numeric(6, 4))                           as ndvi_value,

        -- projected metre coordinates (EPSG:32618, UTM Zone 18N)
        cast(x_32618 as numeric(12, 2))                             as x_32618,
        cast(y_32618 as numeric(12, 2))                             as y_32618

    from source
    where
        latitude  is not null
        and longitude is not null
        and tree_id   is not null
        and lower(trim(status)) = 'alive'
)

select * from cleaned
