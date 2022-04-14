import pandas as pd
import numpy as np


df = pd.read_csv('user_stats.csv')

df['recent_order'] = pd.to_datetime(df['recent_order'], format='%Y-%m-%d')
recent_orders = np.array(df['recent_order'])
qc_recent_orders = pd.qcut(recent_orders, q=5, precision=1)
print(f'Recency groups: {qc_recent_orders.categories}')

num_orders = np.array(df['num_orders'])
qc_num_orders = pd.qcut(num_orders, q=5, precision=1)
print(f'Frequency groups: {qc_num_orders.categories}')

total_spent = np.array(df['total_spent'])
qc_total_spent = pd.qcut(total_spent, q=5, precision=1)
print(f'Monetary value groups: {qc_total_spent.categories}')
