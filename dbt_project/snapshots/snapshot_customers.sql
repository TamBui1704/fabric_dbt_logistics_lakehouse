{% snapshot snapshot_customers %}

{{
    config(
      target_schema='dbo',
      unique_key='customer_id',
      strategy='check',
      check_cols=['customer_name', 'customer_type'],
      invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    customer_name,
    customer_type
FROM {{ source('silver_layer', 'silver_customers') }}

{% endsnapshot %}

