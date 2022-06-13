# Проект 3го спринта

## Описание ETL:

1. Получение `increment_id` с помощью функции `get_increment()`, которая сперва очищает целевую папку для новых инкрементов.
Затем из соединени достает имя хоста и `report_id` для создания URL по которому мы и получим соответствующий `increment_id`.
Сам инкремент берется либо как `execution date`, либо значение из `increment_date` конфига, который можно передавать явно при триггере дага.
Последнее позволяет легко прогнать конкретный инкремент из прошлого, если нужно. А чтобы запустить backfilling по всем инкрементам,
используемся параметр `catchup=True` вместе с `start_date` в виде даты самого первого инкремента.   

2. По полученному `increment_id` из прошлого шага мы скачиваем три файла: `user_orders_log_inc.csv`, `user_activity_log_inc.csv`, `customer_research_inc.csv`.

3. Удаляем все записи соответвующие нашему инкременту, чтобы обеспечить `idempotence` нашего дага.

4. Добавляем новый инкремент в таблицы слоя `stage` (их определение описано ниже).

5. Добавляем новый инкремент в таблицы слоя `mart` (их определение описано ниже). Запросы с трансформациями вынесены в отдельные файлы в папке `queries`.

6. Вычисление `customer retention` на основе всех данных включая новый инкремент.

### Скрипт для создания таблиц в схеме `stage`

```
CREATE TABLE stage.customer_research (
	id          SERIAL4        NOT NULL,
	date_id     TIMESTAMP      NULL,
	category_id INT4           NULL,
	geo_id      INT4           NULL,
	sales_qty   INT4           NULL,
	sales_amt   NUMERIC(14, 2) NULL,
	CONSTRAINT customer_research_pkey PRIMARY KEY (id)
);
CREATE TABLE stage.user_activity_log (
	id SERIAL4 NOT NULL,
	date_time TIMESTAMP NULL,
	action_id int8 NULL,
	customer_id int8 NULL,
	quantity int8 NULL,
	CONSTRAINT user_activity_log_pkey PRIMARY KEY (id)
);
CREATE TABLE stage.user_orders_log (
	id             SERIAL4        NOT NULL,
	date_time      TIMESTAMP      NULL,
	city_id        INT4           NULL,
	city_name      VARCHAR(100)   NULL,
	customer_id    INT8           NULL,
	first_name     VARCHAR(100)   NULL,
	last_name      VARCHAR(100)   NULL,
	item_id        INT4           NULL,
	item_name      VARCHAR(100)   NULL,
	quantity       INT8           NULL,
	payment_amount NUMERIC(14, 2) NULL,
	status         TEXT DEFAULT 'shipped',
	CONSTRAINT user_order_log_pkey PRIMARY KEY (id)
);
```

### Скрипт для создания таблиц в схеме `mart`:

```
CREATE TABLE mart.d_customer(
   id               SERIAL      PRIMARY KEY,
   customer_id      INT         NOT NULL UNIQUE,
   first_name       VARCHAR(15),
   last_name        VARCHAR(15),
   city_id          INT
);
CREATE INDEX d_cust1  ON mart.d_customer (customer_id);

CREATE TABLE mart.d_item(
   id               SERIAL      PRIMARY KEY,
   item_id          INT         NOT NULL UNIQUE,
   item_name        VARCHAR(50)
);
CREATE INDEX d_item_idx  ON mart.d_item (item_id);

CREATE TABLE mart.d_calendar(
   date_id          SERIAL4     PRIMARY KEY,
   day_num          SMALLINT,
   month_num        SMALLINT,
   month_name       VARCHAR(20),
   year_num         SMALLINT
);
CREATE INDEX d_calendar1  ON mart.d_calendar (year_num);

CREATE TABLE mart.f_daily_sales(
   date_id          INT NOT NULL,
   item_id          INT NOT NULL,
   customer_id      INT NOT NULL,
   quantity         BIGINT,
   payment_amount   DECIMAL(10,2),
   status 			TEXT,
   PRIMARY KEY (date_id, item_id, customer_id),
   FOREIGN KEY (date_id)     REFERENCES mart.d_calendar(date_id)         ON UPDATE CASCADE,
   FOREIGN KEY (item_id)     REFERENCES mart.d_item(item_id)             ON UPDATE CASCADE,
   FOREIGN KEY (customer_id) REFERENCES mart.d_customer(customer_id)     ON UPDATE CASCADE
);
CREATE INDEX f_ds1  ON mart.f_daily_sales (date_id);
CREATE INDEX f_ds2  ON mart.f_daily_sales (item_id);
CREATE INDEX f_ds3  ON mart.f_daily_sales (customer_id);

INSERT INTO mart.d_calendar (day_num, month_num, month_name, year_num)
SELECT
	EXTRACT(day from full_date)   day_num,
	EXTRACT(month from full_date) month_num,
	TO_CHAR(full_date, 'month')   month_name,
	EXTRACT(year from full_date)  year_num,
	extract(week from full_date)  week_num
FROM (
		SELECT day::DATE AS full_date
		FROM GENERATE_SERIES(DATE '2020-01-01', DATE '2020-01-01' + 365*3, INTERVAL '1 day' day) day
) AS full_dates;

CREATE TABLE mart.customer_retention (
	period_id                   INT4 NOT NULL UNIQUE,
	new_customers_count         INT4 NOT NULL,
	returning_customers_count   INT4 NOT NULL,
	refunded_customer_count     INT4 NOT NULL,
	customers_refunded          INT4 NOT NULL,
	new_customers_revenue       NUMERIC(14, 2) NOT NULL,
	returning_customers_revenue NUMERIC(14, 2) NOT NULL,
	period_name                 TEXT NOT NULL,
	CONSTRAINT customer_retention_pkey PRIMARY KEY (period_id)
);
```