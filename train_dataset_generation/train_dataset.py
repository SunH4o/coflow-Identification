import pandas as pd
import numpy as np
import os

MEGABYTE = 1024 * 1024

current_file_dir = os.getcwd()
# print(current_file_dir)
FILE_PATH = current_file_dir + '/coflow_dataset_demo_continue.csv'

# 读取 coflow_dataset_demo 
df = pd.read_csv(FILE_PATH,
                  usecols=['coflow_id', 'start_time', 'mean_packet_size_20'])

df.astype(float)

print(df.head(10))


train_dataset = []

for i in range(len(df)):
    for j in range(i + 1, len(df)):
        feature1 = abs(df.loc[i, 'start_time'] - df.loc[j, 'start_time'])
        feature2 = abs(df.loc[i, 'mean_packet_size_20'] - df.loc[j, 'mean_packet_size_20'])
        label = 1 if df.loc[i, 'coflow_id'] == df.loc[j, 'coflow_id'] else 0
        train_dataset.append([feature1, feature2, label])

# 将新数据集转换为 DataFrame
new_df = pd.DataFrame(train_dataset, columns=['feature1', 'feature2', 'label'])
new_df.to_csv(current_file_dir+'/train_dataset_generation/train_dataset_continue.csv', index=False)
