import pandas as pd
import numpy as np
import os

current_file_dir = os.getcwd()
flows_dataset = []
# 读取 coflow_dataset_demo 
df = pd.read_csv(current_file_dir+'\\coflow-identification\\coflow_dataset_demo.csv')

# 逐行处理数据
for index, row in df.iterrows():
    # 处理每一行数据
    # print(row)
    coflow_id = row['coflow_id']
    arrival_time = row['arrival_time']
    flow_num = row['flow_num']
    total_received_MB = row['received_mb']
    # 生成 flow_num 个服从均匀分布的随机数:每条 flow 的开始时间
    flow_start_times = np.random.uniform(arrival_time, arrival_time + 100, flow_num)
    # 当前coflow的平均分组大小：500-1000 bytes
    mean_packet_size = np.random.randint(500, 1001)
    std = np.random.randint(0, 101)

    # 当前coflow的总分组数
    total_packet_sum =total_received_MB / mean_packet_size

    # 生成 flow_num 个服从正态分布的整数作为每条 flow 的 mean_packet_size_perflow
    mean_packet_size_perflow = np.random.normal(mean_packet_size, std, flow_num).astype(int)

    perflow_size = np.random.normal(mean_packet_size, std, flow_num)

    for start_time, packet_size in zip(flow_start_times, mean_packet_size_perflow):
        flows_dataset.append({
            'start_time': start_time,
            'packet_size': packet_size,
            'coflow_id': coflow_id
        })
    

    #
    
    # print(f"Row {index}: Start times = {flow_start_times}")
# 如果需要将 flows_dataset 转换为 DataFrame 并保存到文件
flows_df = pd.DataFrame(flows_dataset)
flows_df.to_csv(current_file_dir+'\\coflow-identification\\flows_dataset.csv', index=False)
    


