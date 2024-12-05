import pandas as pd
import numpy as np
import os

MEGABYTE = 1024 * 1024

current_file_dir = os.getcwd()

# 读取 coflow_dataset_demo 
df = pd.read_csv(current_file_dir+'\\coflow-identification\\coflow_dataset_demo.csv')


flows_dataset = []

for index, row in df.iterrows():
    # 处理每个coflow
    # print(row)
    coflow_id = row['coflow_id']                    # 当前coflow的ID
    arrival_time = row['arrival_time']              # 当前coflow的开始时间
    flow_num = row['flow_nums']                     # 当前coflow中flows的数量
    coflow_size = row['received_MB']*MEGABYTE       # 当前coflow的总字节数(bytes)
    # 生成 flow_num 个服从均匀分布的随机数:每条 flow 的开始时间
    
    flow_start_times = np.random.uniform(arrival_time, arrival_time + 100, flow_num)
    # print(type(flow_num))
#coflow-level
    # 当前coflow的平均分组大小：500-1000 bytes @PICO
    # 1*1
    coflow_mean_packet_size = np.random.randint(500, 1001)

    # 当前coflow的总分组数 = coflow的总字节数 / coflow的平均分组大小
    # 1*1
    coflow_total_packets_sum = coflow_size / coflow_mean_packet_size

#flow-level
    # 当前coflow中的flow的平均分组数 = coflow的总分组数 / flow的数量
    # 1*1
    flow_mean_packet_num = coflow_total_packets_sum / flow_num
    # 每条flow的分组数：服从N(flow_mean_packet_sum,std)
    # 标准差
    std = np.random.randint(0, 50)
    # std = 50
    
    # flow_num*1
    flow_packet_num = np.random.normal(flow_mean_packet_num, std , flow_num).astype(int)

#packet-level
    # 每条流的平均分组大小：等于coflow_mean_packet_size
    flow_mean_packet_size = coflow_mean_packet_size
    ## perflow_size = np.random.normal(mean_packet_size, std, flow_num)

    # 处理每条flow
    for start_time, packet_num in zip(flow_start_times, flow_packet_num):
        # 该条flow的分组大小：服从N(flow_mean_packet_size,std)
        packet_sizes = np.random.normal(flow_mean_packet_size, std, packet_num).astype(int)
        mean_packet_size_32 = np.mean(packet_sizes[:32]) # 前32个分组的平均大小
        mean_packet_size_20 = np.mean(packet_sizes[:20]) # 前20个分组的平均大小
        mean_packet_size_16 = np.mean(packet_sizes[:16]) # 前16个分组的平均大小
        mean_packet_size_8 = np.mean(packet_sizes[:8]) 
        mean_packet_size_4 = np.mean(packet_sizes[:4])
        
        
        flows_dataset.append({
            'start_time': start_time,
            'flow_size': packet_sizes.sum(), # 该条flow的总字节数
            'mean_packet_size_20': mean_packet_size_20,
            'mean_packet_size_16': mean_packet_size_16,
            'mean_packet_size_8': mean_packet_size_8,
            'mean_packet_size_4': mean_packet_size_4,
            'mean_packet_size_32': mean_packet_size_32,
            'coflow_id': coflow_id
        })
    

    
    
# 将 flows_dataset 转换为 DataFrame 并保存
flows_df = pd.DataFrame(flows_dataset)
flows_df.to_csv(current_file_dir+'\\coflow-identification\\flows_dataset_new_2.csv', index=False)
    


