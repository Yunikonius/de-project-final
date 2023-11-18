insert into STV2023070330__DWH.global_metrics 
(
  transaction_dt
, currency_code
, amount_total
, cnt_transactions
, avg_transactions_per_account
, cnt_accounts_make_transactions
, load_dt
, "user"
)

with table1 as
(Select
  transaction_dt
, currency_code
, count(distinct account_number_from) as cnt_accounts_make_transactions --считаем уникальных отправителей
, count(1) as cnt_transactions
from STV2023070330__DWH.transactions
where account_number_to <> -1
  and account_number_from <> -1
  and upper(status) = 'DONE' 
group by 
  transaction_dt
, currency_code)

Select 
  t.transaction_dt
, t.currency_code 
, sum(round(t.amount*coalesce(cr.rate, 1),2)) as amount_total --в случае, если валюта 420 (руб), не меняем значение amount
, t1.cnt_transactions 
, round(t1.cnt_transactions / t1.cnt_accounts_make_transactions, 2) as avg_transactions_per_account
, t1.cnt_accounts_make_transactions
, CURRENT_DATE() as load_dt 
, user as "user"
From STV2023070330__DWH.transactions t
LEFT JOIN table1 t1
ON t.transaction_dt = t1.transaction_dt
  AND t.currency_code = t1.currency_code
LEFT JOIN STV2023070330__DWH.currencies_rate cr
ON t.currency_code = cr.currency_cd
where t.account_number_to <> -1
  and t.account_number_from <> -1
  and upper(t.status) = 'DONE'
  and not exists (select 1 from STV2023070330__DWH.global_metrics t2 where t2.transaction_dt = t.transaction_dt
  	and t2.currency_code = t.currency_code) --реализация инкрементальной загрузки
Group by 
  t.transaction_dt
, t.currency_code
, t1.cnt_transactions
, t1.cnt_accounts_make_transactions;