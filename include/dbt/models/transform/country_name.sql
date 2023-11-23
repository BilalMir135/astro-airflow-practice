SELECT name AS country_name
FROM {{ ref('pk_country') }}