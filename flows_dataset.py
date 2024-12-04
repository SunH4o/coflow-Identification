import pandas as pd
import numpy as np
import os

MEGABYTE = 1024 * 1024

current_file_dir = os.getcwd()

# 读取 coflow_dataset_demo 
df = pd.read_csv(current_file_dir+'\\coflow-identification\\coflow_dataset_demo.csv')


flows_dataset = []
# 逐行处理数据
for index, row in df.iterrows():
    # 处理每一行数据
    # print(row)
    coflow_id = row['coflow_id']                    # 当前coflow的ID
    arrival_time = row['arrival_time']              # 当前coflow的开始时间
    flow_num = row['flow_nums']                     # 当前coflow中flows的数量
    coflow_size = row['received_MB']*MEGABYTE       # 当前coflow的总字节数(bytes)
    # 生成 flow_num 个服从均匀分布的随机数:每条 flow 的开始时间
    # 原理：泊松分布
    flow_start_times = np.random.uniform(arrival_time, arrival_time + 100, flow_num)
    # print(type(flow_num))
#coflow-level
    # 当前coflow的平均分组大小：500-1000 bytes @PICO
    # 1*1
    coflow_mean_packet_size = np.random.randint(500, 1001)

    # 当前coflow的总分组数
    # 1*1
    coflow_total_packets_sum = coflow_size / coflow_mean_packet_size

#flow-level
    # 当前coflow中的flow的平均分组数：flow的平均长度
    # 1*1
    flow_mean_packet_sum = coflow_total_packets_sum / flow_num
    # 每条flow的长度：服从N(flow_mean_packet_sum,std)
    # 标准差
    # 共flow_num条flow
    std = np.random.randint(0, 101)
    # flow_sum*1
    flow_size = np.random.normal(flow_mean_packet_sum, std, flow_num)

#packet-level
    # 生成 flow_num 个服从正态分布(coflow_mean_packet_size,std)的整数
    # 作为每条 flow 的 平均分组大小
    flow_mean_packet_size = np.random.normal(coflow_mean_packet_size, std, flow_num).astype(int)

    ## perflow_size = np.random.normal(mean_packet_size, std, flow_num)

    for start_time, packet_size in zip(flow_start_times, flow_mean_packet_size):
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
    


