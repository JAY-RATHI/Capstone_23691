{% snapshot snap_campaign %}

    {{
        config(
            target_schema="snapshots",
            unique_key="campaign_id",
            strategy="timestamp",
            updated_at="last_modified_date",
        )
    }}

    with
        flattened as (

            select
                campaign.value:campaign_id::varchar as campaign_id,

                campaign.value:last_modified_date::timestamp_ntz as last_modified_date,

                campaign.value as raw_campaign_data,

                b.source_file

            from
                {{ ref("br_campaign") }} as b,

                lateral flatten(input => b.raw_data:campaigns_data) as campaign

        ),

        latest_campaign as (

            select campaign_id, last_modified_date, raw_campaign_data

            from flattened

            qualify
                row_number() over (
                    partition by campaign_id
                    order by last_modified_date desc, source_file desc
                )
                = 1

        )

    select campaign_id, last_modified_date, raw_campaign_data

    from latest_campaign

{% endsnapshot %}
