import pandas as pd
import numpy as np
import os

MEGABYTE = 1024 * 1024

current_file_dir = os.getcwd()
# print(current_file_dir)
FILE_PATH = current_file_dir + '/flows_dataset_continue_5.csv'

# 读取 coflow_dataset_demo 
df = pd.read_csv(FILE_PATH,
                  usecols=['coflow_id', 'start_time', 'mean_packet_size_20'])

# df.astype(float)

# 抽取数据
def sample_data(group):
    if len(group) < 100:
        return group
    else:
        return group.sample(frac=0.05, random_state=1)

df_train = df.groupby('coflow_id').apply(sample_data).reset_index(drop=True)
# print(df_train)




train_dataset = []

for i in range(len(df_train)):
    for j in range(i + 1, len(df_train)):
        feature1 = abs(df_train.loc[i, 'start_time'] - df_train.loc[j, 'start_time'])
        feature2 = abs(df_train.loc[i, 'mean_packet_size_20'] - df_train.loc[j, 'mean_packet_size_20'])
        label = 1 if df_train.loc[i, 'coflow_id'] == df_train.loc[j, 'coflow_id'] else 0
        train_dataset.append([feature1, feature2, label])

# 将新数据集转换为 DataFrame
new_df = pd.DataFrame(train_dataset, columns=['feature1', 'feature2', 'label'])
new_df.to_csv(current_file_dir+'/train_dataset_generation/train_dataset_continue_5_small.csv', index=False)
