import pandas as pd
import numpy as np
import os

current_file_dir = os.getcwd()
flows_dataset = []
# 读取 coflow_dataset
# df = pd.read_csv(current_file_dir+'\\coflow-identification\\coflow_dataset.csv')


# flow_num_sum = df['flows_num'].sum()
# print(f"flow_num 列的总和是: {flow_num_sum}")
# 706397:符合原文描述的结果

packet_sizes = np.round(np.random.normal(0, 1, 10),1)
print(packet_sizes)
print(np.round(packet_sizes.sum(),1))

