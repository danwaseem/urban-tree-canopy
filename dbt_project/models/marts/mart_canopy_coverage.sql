/*
  mart_canopy_coverage
  --------------------
  Borough- and census-tract-level canopy summary for the urban tree dashboard.

  canopy_density_score is a simple demo proxy (tree_count / 1000).
  In production this would divide by the tract area in hectares derived
  from the PostGIS geometry column.
*/

{{
  config(materialized='table')
}}

with base as (
    select * from {{ ref('stg_tree_census') }}
)

select
    census_tract_id,
    boroname,

    count(*)                                        as tree_count,
    round(avg(ndvi_value)::numeric, 4)              as avg_ndvi,
    count(distinct spc_common)                      as species_count,
    round(avg(tree_dbh)::numeric, 2)                as avg_tree_dbh,

    -- demo density proxy: trees per 1 000 (replace with per-hectare in prod)
    round((count(*) / 1000.0)::numeric, 4)          as canopy_density_score

from base
group by
    census_tract_id,
    boroname
