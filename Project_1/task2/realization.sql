-- добавьте код сюда
CREATE OR REPLACE VIEW analysis.orders AS
select
	po.order_id,
	order_ts,
	user_id ,
	bonus_payment,
	payment,
	"cost",
	bonus_grant,
	final_status as status
from
	production.orders po
join (
	select
		order_id,
		max(status_id) as final_status
	from
		production.orderstatuslog
	group by 1
) subq on subq.order_id = po.order_id;

