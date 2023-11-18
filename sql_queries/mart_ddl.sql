drop table if exists STV2023070330__DWH.global_metrics;
create table STV2023070330__DWH.global_metrics(
    transaction_dt date not null,
    currency_code integer not null,
    amount_total number(16, 2) not null,
    cnt_transactions integer not null,
    avg_transactions_per_account number(12, 2) not null,
    cnt_accounts_make_transactions integer not null,
    load_dt date,
    "user" varchar(255)
)
order by transaction_dt,
    currency_code
segmented by hash(transaction_dt, currency_code) all nodes;