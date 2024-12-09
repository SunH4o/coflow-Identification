import pandas as pd
import numpy as np
import os

MEGABYTE = 1024 * 1024

current_file_dir = os.getcwd()
print(current_file_dir)

# 读取 coflow_dataset_demo 
FILE_PATH = current_file_dir+'/coflow-identification/coflow_dataset_demo_continue_5.csv'

def flow_generation(FILE_PATH):
    flows_dataset = []
    df = pd.read_csv(FILE_PATH)

    for index, row in df.iterrows():
        # 处理每个coflow
        # print(row)
        coflow_id = row['coflow_id']                    # 当前coflow的ID
        arrival_time = row['arrival_time']              # 当前coflow的开始时间
        flow_num = row['flow_nums']                     # 当前coflow中flows的数量
        coflow_size = row['received_MB']*MEGABYTE       # 当前coflow的总字节数(bytes)

        # 每条 flow 的开始时间服从100ms内的均匀分布
        flow_start_times = np.round(np.random.uniform(arrival_time, arrival_time + 100, flow_num),0)
        
    #coflow-level
        # 当前coflow的平均分组大小：500-1000 bytes @PICO
        coflow_mean_packet_size = np.random.randint(500, 1000)

        # 每条flow的平均分组大小 = coflow的平均分组大小
        flow_mean_packet_size = coflow_mean_packet_size
        # 当前coflow的总分组数 = coflow的总字节数 / coflow的平均分组大小
        # 1*1
        coflow_total_packets_sum = coflow_size / coflow_mean_packet_size

    #flow-level
        # 当前coflow中的flow的平均分组数 = coflow的总分组数 / flow的数量
        # 由于识别不关心每个flow中的分组数量，因此假设所有flow的分组相同
        # 1*1
        flow_packet_num = int(coflow_total_packets_sum / flow_num)

        # 标准差
        std = np.random.randint(0, 50)

    #print('coflow_mean_packet_size:',coflow_mean_packet_size)
    #print('flow_mean_packet_size:',flow_mean_packet_size)
    #print(np.round(np.random.normal(flow_mean_packet_size, std),1))

    #packet-level
        # 每条流的平均分组大小：等于coflow_mean_packet_size
        
        ## perflow_size = np.random.normal(mean_packet_size, std, flow_num)

        # 处理每条flow
        for start_time in flow_start_times:
                # 该条flow的分组大小：服从N(flow_mean_packet_size,std)
                packet_num = flow_packet_num
                packet_sizes = np.round(np.random.normal(flow_mean_packet_size, std, packet_num),1)
                mean_packet_size_32 = np.round(np.mean(packet_sizes[:32]),1) # 前32个分组的平均大小
                mean_packet_size_20 = np.round(np.mean(packet_sizes[:20]),1) # 前20个分组的平均大小
                mean_packet_size_16 = np.round(np.mean(packet_sizes[:16]),1) # 前16个分组的平均大小
                mean_packet_size_8 = np.round(np.mean(packet_sizes[:8]),1)  # 前8个分组的平均大小   
                mean_packet_size_4 = np.round(np.mean(packet_sizes[:4]),1)   # 前4个分组的平均大小 
                
                
                flows_dataset.append({
                    'start_time': start_time,        # 该条flow的总分组数
                    'flow_packet_num':packet_num,
                    'flow_sizes': np.round(packet_sizes.sum(),1), # 该条flow的总字节数
                    'mean_packet_size_32': mean_packet_size_32,
                    'mean_packet_size_20': mean_packet_size_20,
                    'mean_packet_size_16': mean_packet_size_16,
                    'mean_packet_size_8': mean_packet_size_8,
                    'mean_packet_size_4': mean_packet_size_4,
                    'coflow_id': coflow_id
                })
        

        
        
    # 将 flows_dataset 转换为 DataFrame 并保存
    flows_df = pd.DataFrame(flows_dataset)
    flows_df.to_csv(current_file_dir+'/coflow-identification/flows_dataset_continue_5_NEW.csv', index=False)
    
    
if __name__ == '__main__':
    flow_generation(FILE_PATH)

