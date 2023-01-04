with
  running_wallet_balances as (
    with
      base_data as (
        with
          days as (
            SELECT
              generate_series(
                date_trunc('day', min(evt_block_time)) :: TIMESTAMP,
                date_trunc('day', NOW()),
                '1 day'
              ) AS DAY
            FROM
              erc721."ERC721_evt_Transfer"
            WHERE
              contract_address = '{{erc721_contract_address}}'
          ),
          all_wallets as (
            select
              distinct wallet
            from
              (
                select
                  "from" as wallet
                FROM
                  erc721."ERC721_evt_Transfer"
                WHERE
                  contract_address = '{{erc721_contract_address}}'
                union all
                select
                  "to" as wallet
                FROM
                  erc721."ERC721_evt_Transfer"
                WHERE
                  contract_address = '{{erc721_contract_address}}'
              ) distinct_wallets
          )
        select
          day,
          wallet
        from
          days full -- ??
          outer
          join all_wallets on true -- ??
      ),

      aggregated_transfers as (
        with
          transfers as (
            (
              SELECT
                date_trunc('day', evt_block_time) as day,
                "to" as wallet,
                count(evt_tx_hash) as value
              FROM
                erc721."ERC721_evt_Transfer"
              WHERE
                contract_address = '{{erc721_contract_address}}' -- and evt_block_time >= '2021-06-20'
              group by
                day,
                wallet
            )
            union all
            (
              SELECT
                date_trunc('day', evt_block_time) as day,
                "from" as wallet,
                count(evt_tx_hash) * -1 as value
              FROM
                erc721."ERC721_evt_Transfer"
              WHERE
                contract_address = '{{erc721_contract_address}}' -- and evt_block_time >= '2021-06-20'
              group by
                day,
                wallet
            )
          )
        select
          day,
          wallet,
          sum(value) as resulting
        from
          transfers
        group by
          day,
          wallet
      )
    select
      base_data.day,
      base_data.wallet,
      sum(coalesce(resulting, 0)) over (
        partition by base_data.wallet
        order by
          base_data.day
      ) as holding
    from
      base_data
      left join aggregated_transfers on base_data.day = aggregated_transfers.day
      and base_data.wallet = aggregated_transfers.wallet -- where base_data.wallet != '\x0000000000000000000000000000000000000000'
  )
select
  day :: timestamp as day,
  count(wallet) filter (
    where
      holding > 0
  ),
  (
    count(wallet) filter (
      where
        holding > 0
    ) - LAG(
      count(wallet) filter (
        where
          holding > 0
      ),
      1
    ) OVER (
      ORDER BY
        day :: timestamp DESC
    )
  ) * -1 AS daily_change
from
  running_wallet_balances
group by
  1