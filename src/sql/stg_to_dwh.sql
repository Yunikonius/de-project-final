--1.загрузка данных из stg в currencies (уникальный список валют)
insert into STV2023070330__DWH.currencies 
(currency_cd, load_dt, "user", action_cd)
Select 
  currency_cd
, CURRENT_DATE() as load_dt
, user as "user"
FROM
(Select distinct 
  currency_code as currency_cd
FROM STV2023070330__STAGING.currencies
  UNION 
Select distinct 
  currency_code_with as currency_cd
FROM STV2023070330__STAGING.currencies) t
Where not exists --реализация инкрементальной загрузки
(Select 
  1 
FROM STV2023070330__DWH.currencies t1 
WHERE t1.currency_cd = t.currency_cd);


--2. Вставка данных в таблицу с соотношением валют
Insert into STV2023070330__DWH.currencies_rate 
(currency_cd, date_update, rate, load_dt, "user", action_cd)
SELECT 
  currency_cd
, date_update
, rate 
, CURRENT_DATE() as load_dt
, user as "user"
FROM 
(Select 
  currency_cd 
, date_update
, avg(rate) over(partition by currency_cd, date_update) as rate --возьмем средний показатель соотношения валют за день
, ROW_NUMBER() OVER(PARTITION BY currency_cd, date_update ORDER BY date_update DESC) as rn --очищаем от дублей
FROM 
(Select 
  case when currency_code = '420' then currency_code_with  
  else currency_code
  end as currency_cd 
, date_update
, case when currency_code = '420' then round(currency_with_div, 4)
  else round(1/currency_with_div, 4) -- "переворачиваем" соотношение для руб справа
  end as rate
from STV2023070330__STAGING.currencies
where (currency_code = '420' or currency_code_with = '420')
) t
WHERE not exists -- реализация инкрементальной загрузки
(Select 
  1
FROM STV2023070330__DWH.currencies_rate t1 
where t1.currency_cd = t.currency_cd 
  and t1.date_update = t.date_update)) t2
Where t2.rn = 1;

--3. Вставка очищенных данных в таблицу с транзакциями
insert into STV2023070330__DWH.transactions
(operation_id, account_number_from, account_number_to, currency_code, country, status, transaction_type,
amount, transaction_dt, load_dt, "user", action_cd)
SELECT 
  operation_id
, account_number_from
, account_number_to  
, currency_code 
, country 
, status 
, transaction_type 
, coalesce(amount, 0) as amount 
, date(transaction_dt) as transaction_dt
, CURRENT_DATE() as load_dt
, user as "user"
FROM 
(Select 
  t2.*
, ROW_NUMBER() OVER(PARTITION BY operation_id, status ORDER BY transaction_dt desc) as rn 
from STV2023070330__STAGING.transactions t2
where operation_id is not null 
  and status is not null
  and account_number_from <> -1
  and account_number_to <> -1) t
Where rn = 1
  and not EXISTS (Select 1 from STV2023070330__DWH.transactions t1 --реализация инкрементальной загрузки
  				  where t1.operation_id = t.operation_id
  				  and t1.status = t.status);