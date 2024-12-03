import os
import pandas as pd
import numpy as np

coflow_dataset = []
# import re
current_file_dir = os.getcwd()
file_name = "FB2010-1Hr-150-0.txt"

file_path = os.path.join(current_file_dir+"\\coflow-identification\\coflowbenchmark",file_name)

with open(file_path,'r') as file:
    #coflowTrace = file.read() #整个文件
    firstLines = file.readline() #readline():读取每一行;readlines()：读取全部
    # print(irstLines)
    splits = firstLines.split() #按空格分割
    NUM_RACKS = splits[0]
    NUM_JOBS = splits[1]
    #print(NUM_RACKS)
    #print(NUM_JOBS)
    print(f"Number of rack: {NUM_RACKS}, Number of coflows: {NUM_JOBS}")

    for line in file:
        line = line.strip() #移除末尾换行符
        if line:
            # 分割每一行的数据
            parts = line.split()
            coflow_id = int(parts[0])
            arrival_time = int(parts[1])
            num_mappers = int(parts[2])
            mapper_locations = parts[3:3 + num_mappers]
            num_reducers = int(parts[3 + num_mappers])
            reducer_info = parts[4 + num_mappers:]

            flows_num = num_mappers*num_reducers #该shuffle阶段产生的流量数
            # 处理reducer_info
            reducers = []
            for info in reducer_info:
                reducer_location, received_MB_perreducer = info.split(':')
                reducers.append((int(reducer_location), float(received_MB_perreducer)))
            total_received_MB = sum([received_MB for _, received_MB in reducers])

            # 一个coflow中的平均flow大小
            mean_size = total_received_MB/flows_num 

            # 一个coflow中每条流的到达时间和大小
            # 到达时间服从100ms内的均匀分布：by CODA
            flow_arrival_times = np.random.randint(arrival_time, arrival_time+100, flows_num)
            # 每条流大小服从均值为mean_size的正态分布，方差为0-100之间的随机数：by PICO
            flow_sizes = np.random.normal(mean_size, np.random.randint(0, 100), flows_num)


            coflow_dataset.append([coflow_id, arrival_time, num_mappers, num_reducers, total_received_MB, flows_num,mean_size])

            # 打印提取的信息
            # print(f"Coflow ID: {coflow_id}, Arrival Time: {arrival_time} ms")
            # print(f"Number of Mappers: {num_mappers}, Mapper Locations: {mapper_locations}")
            # print(f"Number of Reducers: {num_reducers}, Reducer Info: {reducer_info}")
            # print()

    # 创建一个DataFrame
    df = pd.DataFrame(coflow_dataset, columns=['coflow_id', 'arrival_time', 'num_mappers', 'num_reducers', 'received_mb', 'flow_num(i.e. coflow width)','mean_size'])

    # 打印DataFrame
    # print(df)
    # 导出DataFrame为CSV文件
    # df.to_csv('coflow_dataset.csv', index=False)

    # 从526条数据中随机抽取100条
    df_demo = df.sample(n=10, random_state=1)

    # 打印DataFrame
    print(df_demo)

    # 导出DataFrame为CSV文件
    df_demo.to_csv('coflow_dataset_demo.csv', index=False)