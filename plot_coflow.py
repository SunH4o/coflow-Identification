import pandas as pd
import matplotlib.pyplot as plt
import os



# 读取 CSV 文件中的数据
columns_to_read = ['start_time', 'mean_packet_size_20', 'coflow_id']
data = pd.read_csv('flows_dataset_continue_5_NEW.csv', usecols=columns_to_read)

# 打印前几行数据以确认读取成功
# print(data.head())

plt.figure(figsize=(10, 6))
scatter = plt.scatter(data['start_time'], data['mean_packet_size_20'], c=data['coflow_id'], cmap='viridis', alpha=0.6)
plt.colorbar(scatter, label='Coflow ID')
plt.xlabel('Start Time')
plt.ylabel('Mean Packet Size 20')
plt.title('Scatter Plot of Mean Packet Size 20 vs Start Time')
plt.show()