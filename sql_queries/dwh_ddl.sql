--ddl детального слоя со списком уникальных кодов валют

DROP TABLE IF EXISTS STV2023070330__DWH.currencies;

CREATE TABLE STV2023070330__DWH.currencies
(
	currency_cd int,
	load_dt date,
	"user" varchar(255),
	constraint pk_cur_cd primary key (currency_cd)
);

--ddl таблицы детального слоя с соотношением валют

DROP TABLE IF EXISTS STV2023070330__DWH.currencies_rate;

CREATE table STV2023070330__DWH.currencies_rate
(
	currency_cd int,
	date_update date,
	rate number(8,6),
	load_dt date,
	"user" varchar(255),
	constraint pk_cur_date primary key (currency_cd, date_update)
)
ORDER BY date_update, currency_cd
segmented by hash(date_update, currency_cd) all nodes;

ALTER TABLE STV2023070330__DWH.currencies_rate 
ADD FOREIGN KEY (currency_cd) REFERENCES STV2023070330__DWH.currencies (currency_cd);

--таблица с транзакциями

DROP TABLE IF EXISTS STV2023070330__DWH.transactions

CREATE TABLE STV2023070330__DWH.transactions 
(
	operation_id varchar(1000),
	account_number_from int,
	account_number_to int,
	currency_code int,
	country varchar(100),
	status varchar(100),
	transaction_type varchar(100),
	amount number(14,2),
	transaction_dt date,
	load_dt date,
	"user" varchar(255),
	constraint pk_id_stts primary key (operation_id, status)
)
ORDER BY transaction_dt, currency_code
segmented by hash(transaction_dt, currency_code) all nodes;

ALTER TABLE STV2023070330__DWH.transactions 
ADD FOREIGN KEY (currency_code) REFERENCES STV2023070330__DWH.currencies (currency_cd);


