DROP TABLE IF EXISTS public.shipping_country_rates CASCADE;
DROP TABLE IF EXISTS public.shipping_agreement CASCADE;
DROP TABLE IF EXISTS public.shipping_transfer CASCADE;
DROP TABLE IF EXISTS public.shipping_info;
DROP TABLE IF EXISTS public.shipping_status;
DROP VIEW IF EXISTS shipping_datamart;


--shipping_country_rates
CREATE TABLE public.shipping_country_rates(
   shipping_country_id        SERIAL,
   shipping_country           TEXT,
   shipping_country_base_rate NUMERIC(14,3),
   PRIMARY KEY (shipping_country_id)
);
--shipping_agreement
CREATE TABLE public.shipping_agreement(
   agreementid 			INT8,
   agreement_number     TEXT,
   agreement_rate       NUMERIC(14,3),
   agreement_commission NUMERIC(14,3),
   PRIMARY KEY (agreementid)
);
--shipping_transfer
CREATE TABLE public.shipping_transfer(
   transfer_type_id 	  SERIAL,
   transfer_type          TEXT,
   transfer_model         TEXT,
   shipping_transfer_rate NUMERIC(14,3),
   PRIMARY KEY (transfer_type_id)
);
--shipping_info
CREATE TABLE public.shipping_info(
   shippingid 			  BIGINT,
   vendorid 			  BIGINT,
   payment_amount         NUMERIC(14,2),
   shipping_plan_datetime TIMESTAMP,
   transfer_type_id 	  INT8,
   shipping_country_id 	  INT8,
   agreementid 			  INT8,
   PRIMARY KEY (shippingid),
   CONSTRAINT fk_shipping_country_id FOREIGN KEY(shipping_country_id)
   					REFERENCES public.shipping_country_rates(shipping_country_id) ON UPDATE CASCADE,
   CONSTRAINT fk_transfer_type_id FOREIGN KEY(transfer_type_id)
   					REFERENCES public.shipping_transfer(transfer_type_id) ON UPDATE CASCADE,
   CONSTRAINT fk_agreementid FOREIGN KEY(agreementid)
   					REFERENCES public.shipping_agreement(agreementid) ON UPDATE CASCADE
);
--shipping_status
CREATE TABLE public.shipping_status(
   shippingid 					BIGINT,
   status 						TEXT,
   state 						TEXT,
   shipping_start_fact_datetime TIMESTAMP,
   shipping_end_fact_datetime   TIMESTAMP,
   PRIMARY KEY (shippingid)
);


--shipping_country_rates
INSERT INTO public.shipping_country_rates(shipping_country, shipping_country_base_rate)
SELECT
	DISTINCT
	shipping_country,
	shipping_country_base_rate AS shipping_country_base_rate
FROM
	public.shipping;

--shipping_agreement
INSERT INTO public.shipping_agreement(agreementid, agreement_number, agreement_rate, agreement_commission)
SELECT
	vendor_agreement_description[1]::INT8 AS agreementid,
	vendor_agreement_description[2]::TEXT AS agreement_number,
	vendor_agreement_description[3]::NUMERIC(14,3) AS agreement_rate,
	vendor_agreement_description[4]::NUMERIC(14,3) AS agreement_commission
FROM
	(
	SELECT
		DISTINCT REGEXP_SPLIT_TO_ARRAY(vendor_agreement_description, ':+') AS vendor_agreement_description
	FROM
		public.shipping
) AS vendor_agreement_description_subq;

--shipping_transfer
INSERT INTO public.shipping_transfer(transfer_type, transfer_model, shipping_transfer_rate)
SELECT
	shipping_transfer_description[1] AS transfer_type,
	shipping_transfer_description[2] AS transfer_model,
	shipping_transfer_rate
FROM
	(
	SELECT DISTINCT
		REGEXP_SPLIT_TO_ARRAY(shipping_transfer_description, ':+') AS shipping_transfer_description,
		shipping_transfer_rate
	FROM
		public.shipping
) AS shipping_transfer_description_subq;

--shipping_info
INSERT INTO public.shipping_info(shippingid, vendorid, payment_amount, shipping_plan_datetime,
								 transfer_type_id, shipping_country_id, agreementid)
SELECT
	DISTINCT
	shippingid,
	vendorid,
	payment_amount,
	shipping_plan_datetime,
	st.transfer_type_id,
	scr.shipping_country_id,
	sa.agreementid
FROM
	(
	SELECT
		shippingid,
		vendorid,
		payment_amount,
		shipping_plan_datetime,
		shipping_country,
		REGEXP_SPLIT_TO_ARRAY(vendor_agreement_description, ':+')  AS vendor_agreement_description,
		REGEXP_SPLIT_TO_ARRAY(shipping_transfer_description, ':+') AS shipping_transfer_description
	FROM
		public.shipping) s
JOIN public.shipping_country_rates scr USING(shipping_country)
JOIN public.shipping_agreement sa      ON sa.agreementid = vendor_agreement_description[1]::INT8
JOIN public.shipping_transfer st       ON st.transfer_type = shipping_transfer_description[1]
								       AND st.transfer_model = shipping_transfer_description[2];

--shipping_status
INSERT INTO public.shipping_status(shippingid, status, state, shipping_start_fact_datetime,
								   shipping_end_fact_datetime)
SELECT
	DISTINCT
	shippingid,
	status,
	state,
	shipping_start_fact_datetime,
	shipping_end_fact_datetime
FROM
	(
	SELECT
		shippingid,
		FIRST_VALUE(state_datetime) OVER(PARTITION BY shippingid ORDER BY state_datetime) AS shipping_start_fact_datetime,
		FIRST_VALUE(state_datetime) OVER w AS shipping_end_fact_datetime,
		FIRST_VALUE(status)         OVER w AS status,
		FIRST_VALUE(state)          OVER w AS state
	FROM
		public.shipping
	WINDOW w AS (PARTITION BY shippingid ORDER BY state_datetime DESC)
) shipping_status_subq;

CREATE VIEW shipping_datamart AS
SELECT
	ss.shippingid,
	vendorid,
	st.transfer_type,
	DATE_PART('day', AGE(shipping_end_fact_datetime, shipping_start_fact_datetime))         AS full_day_at_shipping,
	shipping_end_fact_datetime > shipping_plan_datetime                                     AS is_delay,
	status = 'finished' 																	AS is_shipping_finish,
	CASE
		WHEN shipping_end_fact_datetime > shipping_plan_datetime
		THEN DATE_PART('day', AGE(shipping_end_fact_datetime, shipping_plan_datetime))
		ELSE 0
	END 																					AS delay_day_at_shipping,
	payment_amount,
	payment_amount * (shipping_country_base_rate + agreement_rate + shipping_transfer_rate) AS vat,
	payment_amount * agreement_commission 													AS profit
FROM
	public.shipping_status ss
JOIN public.shipping_info si           ON ss.shippingid = si.shippingid
JOIN public.shipping_transfer st       ON si.transfer_type_id = st.transfer_type_id
JOIN public.shipping_country_rates scr ON scr.shipping_country_id = si.shipping_country_id
JOIN public.shipping_agreement sa      ON si.agreementid = sa.agreementid;
