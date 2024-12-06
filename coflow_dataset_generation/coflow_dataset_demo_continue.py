import os
import pandas as pd
import numpy as np

current_file_dir = os.getcwd()
print(current_file_dir)

df = pd.read_csv(current_file_dir + '/coflow_dataset.csv')

# 前十个连续的coflow
df_demo_continue = df.head(10)
df_demo_continue.to_csv('coflow_dataset_demo_continue.csv', index=False)


