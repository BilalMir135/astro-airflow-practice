{% set view_name = 'token_balances.raw_logs' %}
{% set dataset_name = 'bigquery-public-data.crypto_ethereum' %}

CREATE OR REPLACE VIEW {{ view_name }} AS
SELECT *
FROM `{{ dataset_name }}.logs`;