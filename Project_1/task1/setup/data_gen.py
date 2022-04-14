import os
import random
from datetime import datetime, timedelta
from itertools import count
from typing import List, Tuple

from faker import Faker


class User:
    def __init__(self, id: int, name: str, login: str) -> None:
        self.id = id
        self.name = name
        self.login = login

    def __str__(self) -> str:
        return f"{self.id};{self.login};{self.name}"


class StatusLogRecord:
    def __init__(self, order_id: int, status_id: int, dt: datetime) -> None:
        self.order_id = order_id
        self.status_id = status_id
        self.dttm = dt

    def __str__(self) -> str:
        return f"{self.order_id};{self.status_id};{self.dttm}"


class OrderStatus:
    def __init__(self, id: int, key: str) -> None:
        self.id = id
        self.key = key

    def __str__(self) -> str:
        return f"{self.id};{self.key}"


class Product:
    def __init__(self, id, name, price) -> None:
        self.id = id
        self.name = name
        self.price = price

    def __str__(self) -> str:
        return f"{self.id};{self.name};{self.price}"


class OrderItem:
    def __init__(self, order_id: int, id: int, name: str, price: float, discount: float, quantity: int) -> None:
        self.id = id
        self.name = name
        self.price: float = price
        self.discount = discount
        self.quantity = quantity
        self.order_id = order_id

    def __str__(self) -> str:
        return f"{self.id};{self.order_id};{self.name};{self.price};{self.discount};{self.quantity}"


class Order:
    def __init__(self, id: int, date: datetime, user_id: int, items: List[OrderItem], bonus_payment: float, status: int) -> None:
        self.id = id
        self.date = date
        self.user_id = user_id
        self.order_items = items
        self.bonus_payment = bonus_payment
        self.cost = sum(i.price * i.quantity for i in items)
        self.payment = self.cost - self.bonus_payment
        self.bonus_grant = self.payment * 0.01
        self.status = status

    def __str__(self) -> str:
        return f"{self.id};{self.date};{self.user_id};{self.bonus_payment};{self.payment};{self.cost};{self.bonus_grant};{self.status}"


class ConsoleWriter:
    def __init__(self) -> None:
        pass

    def write(self, obj):
        print(str(obj))


class DataGen:

    def gen_users(self, fake: Faker, count: int) -> List[User]:
        user_list = []
        for i in range(count):
            user = User(i, fake.name(), fake.unique.user_name())
            user_list.append(user)
        return user_list

    def gen_statuses(self) -> List[OrderStatus]:
        return [
            OrderStatus(1, "Open"),
            OrderStatus(2, "Cooking"),
            OrderStatus(3, "Delivering"),
            OrderStatus(4, "Closed"),
            OrderStatus(5, "Cancelled")
        ]

    def gen_menu(self):
        seq = count(start=0, step=1)
        menu_list = [
            # СЭНДВИЧИ
            Product(next(seq), "ГРИЛАТА ОВОЩНАЯ ПО-МЕКСИКАНСКИ", 60),
            Product(next(seq), "ГРИЛАТА С КУРИЦЕЙ И ОВОЩАМИ", 60),

            Product(next(seq), "РОЛЛ С ВЕТЧИНОЙ И ОМЛЕТОМ", 120),
            Product(next(seq), "РОЛЛ С КУРИЦЕЙ И ПЕСТО", 120),
            Product(next(seq), "РОЛЛ С ИНДЕЙКОЙ", 120),
            Product(next(seq), "КРУАССАН С ВЕТЧИНОЙ", 120),
            Product(next(seq), "СЭНДВИЧ С ВЕТЧИНОЙ И СЫРОМ", 120),
            Product(next(seq), "ПИЦЦЕТТА С ВЕТЧИНОЙ И СЫРОМ", 120),
            Product(next(seq), "ПИЦЦЕТТА С КУРИЦЕЙ", 120),
            Product(next(seq), "КАЛЬЦОНЕ МАРГАРИТА", 120),
            Product(next(seq), "МИНИ ПИРОГ КИШ С КУРИЦЕЙ", 120),
            Product(next(seq), "КИШ С ВЕТЧИНОЙ И ГРИБАМИ", 120),

            Product(next(seq), "РОЛЛ С ТОФУ И ВЯЛЕНЫМИ ТОМАТАМИ", 180),
            Product(next(seq), "РОЛЛ С РЫБОЙ И СОУСОМ ТАРТАР", 180),
            Product(next(seq), "НОРВЕЖСКИЙ СЭНДВИЧ С РЫБОЙ И ЯЙЦОМ", 180),
            Product(next(seq), "СЭНДВИЧ С БЕКОНОМ И ОГУРЦОМ", 180),
            Product(next(seq), "CЭНДВИЧ С КУРИЦЕЙ BBQ", 180),
            Product(next(seq), "СЭНДВИЧ НА КРУАССАНЕ КУРИЦА", 180),
            Product(next(seq), "СЭНДВИЧ С ТУНЦОМ", 180),
            Product(next(seq), "СЭНДВИЧ COFIX С ВЕТЧИНОЙ И КУРИЦЕЙ", 180),
            Product(next(seq), "СЭНДВИЧ С КУРИЦЕЙ И ПЕСТО", 180),
        ]
        return menu_list

    def gen_orders(self, fake: Faker, users: List[User], statuses, menu: List[Product], count: int, max_items: int, max_item_count: int) -> Tuple[List[Order], List[OrderItem], List[StatusLogRecord]]:
        order_list: List[Order] = []
        item_list: List[OrderItem] = []
        order_status_log: List[StatusLogRecord] = []

        st_open = 1
        st_cooking = 2
        st_delivering = 3
        st_closed = 4
        st_cancelled = 5
        final_statuses: List[int] = [st_closed, st_cancelled]

        for _ in range(count):
            order_id: int = fake.random.randint(1, 10000000)
            while any(o.id for o in order_list if o.id == order_id):
                order_id = fake.random.randint(1, 10000000)

            items: List[OrderItem] = []
            for _ in range(random.randint(1, max_items)):
                prod = random.choice(menu)
                item = OrderItem(order_id, prod.id, prod.name,
                                 prod.price, 0, random.randint(1, max_item_count))

                existing_item = next(
                    (it for it in items if it.id == item.id), None)
                if existing_item:
                    existing_item.quantity += item.quantity
                else:
                    items.append(item)
                    item_list.append(item)

            order = Order(
                order_id,
                fake.past_datetime(),
                random.choice(users).id,
                items,
                0,
                random.choice(final_statuses)
            )
            order_list.append(order)

            def random_delta():
                return timedelta(minutes=random.randint(1, 59), seconds=random.randint(1, 59))

            if order.status == st_cancelled:
                order_status_log.append(StatusLogRecord(order.id, st_cancelled, order.date))
                order_status_log.append(StatusLogRecord(order.id, st_open, order.date - random_delta()))
            else:
                order_status_log.append(StatusLogRecord(order.id, st_closed, order.date))

                dt_delivering = order.date - random_delta()
                order_status_log.append(StatusLogRecord(order.id, st_delivering, dt_delivering))

                dt_cooking = dt_delivering - random_delta()
                order_status_log.append(StatusLogRecord(order.id, st_cooking, dt_cooking))

                dt_open = dt_cooking - random_delta()
                order_status_log.append(StatusLogRecord(order.id, st_open, dt_open))

        return (order_list, item_list, order_status_log)

    def write_all(self, filename: str, objs):
        strs = [str(o) + "\n" for o in objs]
        with open(filename, 'w') as out:
            out.writelines(strs)

    def run_setup(self):
        fake = Faker('ru_RU')

        # proj_path = "vsc-pg-metabase/sprint-1/project/task1/setup"
        proj_path = os.path.dirname(os.path.abspath(__file__))

        orders_limit = 10000
        user_limit = 1000
        max_unique_items_per_order = 10
        max_coint_of_item_in_order = 5

        users = self.gen_users(fake, user_limit)
        statuses = self.gen_statuses()
        menu = self.gen_menu()
        (orders, items, status_log) = self.gen_orders(fake, users, statuses,
                                                      menu, orders_limit, max_unique_items_per_order, max_coint_of_item_in_order)

        self.write_all(f"./{proj_path}/files/users.csv", users)
        self.write_all(f"./{proj_path}/files/statuses.csv", statuses)
        self.write_all(f"./{proj_path}/files/menu.csv", menu)
        self.write_all(f"./{proj_path}/files/items.csv", items)
        self.write_all(f"./{proj_path}/files/orders.csv", orders)
        self.write_all(f"./{proj_path}/files/status_log.csv", status_log)
