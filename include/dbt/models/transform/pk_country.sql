SELECT 
    * 
FROM 
    {{ source('token_balances','country') }}
WHERE 
    iso = 'PK'