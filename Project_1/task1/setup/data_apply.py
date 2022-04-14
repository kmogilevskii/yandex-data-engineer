import os
import subprocess
from typing import List


class PsqlRuner:
    def __init__(self, login, password, host, port, db, path) -> None:
        self.login = login
        self.password = password
        self.host = host
        self.port = port
        self.db = db
        self.path = path

    def _connstr(self) -> str:
        return f"postgresql://{self.login}:{self.password}@{self.host}:{self.port}/{self.db}"

    def runcmd(self, params: List[str]):
        cmd = ["psql", self._connstr()]
        cmd.extend(params)
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, cwd=self.path)
        output, error = process.communicate()
        return (output, error)


class DataApply:

    def run_setup_db(self):

        login = "jovyan"
        password = "jovyan"
        host = "localhost"
        port = "5432"
        db = "de"
        # proj_path = "vsc-pg-metabase/sprint-1/project/task1/setup"
        proj_path = os.path.dirname(os.path.abspath(__file__))

        runer = PsqlRuner(login, password, host, port, db, proj_path)

        print("------------------CREATING SCHEMA------------------")
        ddl_cmd = ["-f", "setup_db.sql"]
        (output, error) = runer.runcmd(ddl_cmd)
        print(output)
        print(error)

        print("------------------LOADING Products------------------")
        menu_csv_path = "files/menu.csv"
        menu_load_cmd = [
            "--command", f"\\copy production.Products(id, name, price) FROM '{menu_csv_path}' DELIMITER ';' CSV;"]
        (output, error) = runer.runcmd(menu_load_cmd)
        print(output)
        print(error)

        print("------------------LOADING OrderStatuses------------------")
        status_csv_path = "files/statuses.csv"
        status_load_cmd = [
            "--command", f"\\copy production.OrderStatuses(id, key) FROM '{status_csv_path}' DELIMITER ';' CSV;"]
        (output, error) = runer.runcmd(status_load_cmd)
        print(output)
        print(error)

        print("------------------LOADING Users------------------")
        users_csv_path = "files/users.csv"
        status_load_cmd = [
            "--command", f"\\copy production.Users(id, name, login) FROM '{users_csv_path}' DELIMITER ';' CSV;"]
        (output, error) = runer.runcmd(status_load_cmd)
        print(output)
        print(error)

        print("------------------LOADING Orders------------------")
        orders_csv_path = "files/orders.csv"
        status_load_cmd = [
            "--command", f"\\copy production.Orders(order_id, order_ts, user_id, bonus_payment, payment, cost, bonus_grant, status) FROM '{orders_csv_path}' DELIMITER ';' CSV;"]
        (output, error) = runer.runcmd(status_load_cmd)
        print(output)
        print(error)

        print("------------------LOADING OrderItems------------------")
        items_csv_path = "files/items.csv"
        status_load_cmd = [
            "--command", f"\\copy production.OrderItems(product_id, order_id, name, price, discount, quantity) FROM '{items_csv_path}' DELIMITER ';' CSV;"]
        (output, error) = runer.runcmd(status_load_cmd)
        print(output)
        print(error)

        print("------------------LOADING OrderStatusLog------------------")
        status_log_path = "files/status_log.csv"
        status_load_cmd = [
            "--command", f"\\copy production.OrderStatusLog(order_id, status_id, dttm) FROM '{status_log_path}' DELIMITER ';' CSV;"]
        (output, error) = runer.runcmd(status_load_cmd)
        print(output)
        print(error)

        print("LOADING FINISHED SUCCESSFULLY.")
