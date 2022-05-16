DROP TABLE IF EXISTS public.shipping;


--shipping
CREATE TABLE public.shipping(
   ID serial ,
   shippingid                         BIGINT,
   saleid                             BIGINT,
   orderid                            BIGINT,
   clientid                           BIGINT,
   payment_amount                     NUMERIC(14,2),
   state_datetime                     TIMESTAMP,
   productid                          BIGINT,
   description                        text,
   vendorid                           BIGINT,
   namecategory                       text,
   base_country                       text,
   status                             text,
   state                              text,
   shipping_plan_datetime             TIMESTAMP,
   hours_to_plan_shipping             NUMERIC(14,2),
   shipping_transfer_description      text,
   shipping_transfer_rate             NUMERIC(14,3),
   shipping_country                   text,
   shipping_country_base_rate         NUMERIC(14,3),
   vendor_agreement_description       text,
   PRIMARY KEY (ID)
);
CREATE INDEX shippingid ON public.shipping (shippingid);
COMMENT ON COLUMN public.shipping.shippingid is 'id of shipping of sale';


COPY public.shipping (shippingid,saleid,orderid,clientid,payment_amount,state_datetime,productid,description,vendorid,namecategory,base_country,status,state,shipping_plan_datetime,hours_to_plan_shipping,shipping_transfer_description,shipping_transfer_rate,shipping_country,shipping_country_base_rate,vendor_agreement_description)
-- поля в том порядке, что в csv
FROM '/lessons/shipping.csv'
DELIMITER ','
CSV HEADER;
