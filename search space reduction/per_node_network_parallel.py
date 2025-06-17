from joblib import Parallel, delayed
import pandas as pd
import networkx as nx
from sklearn.metrics import f1_score
from pgmpy.estimators import PC, HillClimbSearch, ExhaustiveSearch
from pgmpy.estimators import K2Score
from pgmpy.utils import get_example_model
import numpy as np
import time
from multiprocessing import Pool,Manager
import os

global_data = None

def init_worker(data):
    global global_data
    global_data  = data

def process_node(args):
    algorithm, node, black_list = args
    
    input_data = global_data
    est = HillClimbSearch(data=input_data)
    # est = HillClimbSearch(data=hospital_data)
    estimated_model = est.estimate(
        scoring_method=algorithm,
        max_indegree=None,
        max_iter=int(1e4),
        black_list=black_list
    )
    tmp_corr = estimated_model.get_markov_blanket(node)
    tmp_corr.append(node)
    return tmp_corr



if __name__ == '__main__':

    ALGORITHMS = ["aicscore"]
    # ALGORITHMS = ["k2score","bdeuscore","bicscore"]

    hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_child25w_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_child50w_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_child75w_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_child100w_less.csv")

    #flights
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_flight8_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_flight12_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_flight16_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_flight8_less.csv")



    #mildew
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_mildew35_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_mildew31_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_mildew27_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_mildew23_less.csv")


    #alarm
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_alarm22_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_alarm27_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_alarm32_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_alarm37_less.csv")


    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_adult_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_hosp_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_food_less.csv")
    #hospital_data = pd.read_csv(r"./datasets/studentfull_26.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_flight_less.csv")
    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_statlog_german_less.csv")

    #hospital_data = pd.read_csv(r"./sample/sampled_data/sampled_amazon_less.csv")
    #print(hospital_data.isnull().any().any())  # 如果存在空值，返回 True；否则返回 False
    # 删除所有包含空值的行
    #hospital_data.dropna(inplace=True)
    # est = HillClimbSearch(data=hospital_data)
    nodes = list(hospital_data.columns)
    unique_nodes = []
    non_unique_nodes = []
    for node in nodes:
        val_class = hospital_data[node].value_counts()
        val_class_counts = len(val_class)
        if val_class_counts > 1:
            non_unique_nodes.append(node)
        else:
            unique_nodes.append(node)
 
    black_lists_each_node = {}
    for target_feature in non_unique_nodes:
        black_list = []
        white_list = []
        for x in nodes:
            for y in nodes:
                if x != y:
                    if x != target_feature and y != target_feature:
                        black_list.append((x,y))
                    else:
                        white_list.append((x,y))
        # print(black_list)
        # print(target_feature,white_list)
        black_lists_each_node[target_feature] = black_list
    # print(black_lists_each_node)
    
    corr_set = []
    task_args = [
        (algorithm, node, black_lists_each_node[node])
        for algorithm in ALGORITHMS
        for node in non_unique_nodes
    ]
    print("length of args:", len(task_args))
    start_time = time.time()
    n_cores = os.cpu_count() - 1
    # 使用进程池并行处理
    with Pool(processes = 17, initializer=init_worker, initargs=(hospital_data,)) as pool:  # 根据CPU核心数调整
        results = pool.map(process_node, task_args)

    # 合并结果
    corr_set = [result + unique_nodes for result in results]

    unique_sets = [set(s) for s in {frozenset(s) for s in corr_set}]
    # 按集合大小降序排序
    sorted_sets = sorted(unique_sets, key=lambda x: len(x), reverse=True)
    filtered_sets = []
    for current_set in sorted_sets:
        # 检查当前集合是否是结果中已有集合的子集
        if len(nodes)-len(current_set)>= 3 and not any(current_set.issubset(existing) for existing in filtered_sets):
            filtered_sets.append(current_set)
    
    end_time = time.time()
    order_dict = {char: idx for idx, char in enumerate(list(nodes))}
    count = 0
    #child 
    original_data = pd.read_csv(r"./datasets/scalability/synthetic_child_25W.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/synthetic_child_50W.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/synthetic_child_75W.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/synthetic_child_100W.csv")


    #flight
    # original_data = pd.read_csv(r"./datasets/scalability/flight_8_clean.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/flight_12_clean.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/flight_16_clean.csv")
        
    #mildew
    #original_data = pd.read_csv(r"./datasets/scalability/synthetic_mildew_23.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/synthetic_mildew_27.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/synthetic_mildew_31.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/synthetic_mildew_35.csv")

    #alarm
    #original_data = pd.read_csv(r"./datasets/synthetic_alarm.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/synthetic_alarm_22.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/synthetic_alarm_27.csv")
    #original_data = pd.read_csv(r"./datasets/scalability/synthetic_alarm_32.csv")
    #original_data = pd.read_csv(r"./datasets/studentfull_26.csv")
    #original_data = pd.read_csv(r"./datasets/flights_20_500k_clean.csv")
    #original_data = pd.read_csv(r"./datasets/adult_clean.csv")
    #original_data = pd.read_csv(r"./datasets/hospital_clean.csv")
    #original_data = pd.read_csv(r"./datasets/statlog_german_credit_data.csv")
    # original_data = pd.read_csv(r"D:\CodeWork\python\data_dependency\PGM_dp\data\sampled_adult_maxclass.csv")
    #original_data = pd.read_csv(r"./datasets/Amazon-sale-report-clean.csv")
    for sets in filtered_sets:
        if len(sets) >= 2 and len(sets) <= 20:
            count +=1
            print(len(sets))
            sub_data = original_data[sets]
            sub_data.to_csv(r'./datasets/subsets/subflight8/subtable' + str(count) + '.csv', index=False)

            #sub_data.to_csv(r'./datasets/subsets/submildew23/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/submildew27/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/submildew31/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/submildew35/subtable' + str(count) + '.csv', index=False)
            
            #sub_data.to_csv(r'./datasets/subsets/subalarm37/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/subalarm32/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/subalarm27/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/subalarm22/subtable' + str(count) + '.csv', index=False)

            #sub_data.to_csv(r'./datasets/subsets/substatalog/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/subamazon/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/subflight/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/subhosp/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/subhospaic/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/subfood/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/subadult/subtable' + str(count) + '.csv', index=False)
            #sub_data.to_csv(r'./datasets/subsets/substu/subtable' + str(count) + '.csv', index=False)
        # sub_data = hospital_data[list(sets)] 
    print(count)

    print("Time elapsed: ", end_time - start_time)



































# from sklearn.feature_selection import mutual_info_classif
# import pickle
# import pandas as pd
# import numpy as np
# from sklearn.preprocessing import LabelEncoder
# hospital_data = pd.read_csv(r"D:\CodeWork\python\data_dependency\PGM_dp\data\sampled_hospital_17_maxclass.csv")
# nodes = list(hospital_data.columns)

# read_path = 'D:\CodeWork\python\data_dependency\PGM_dp\data\pickle\hospital_corr.pkl'
# with open(read_path,'rb') as f:
# 	result = pickle.load(f)
# print(result)


# enc = LabelEncoder()

# # 原始数据集D对应的Dataframe
# # 检测非数值列
# non_numeric_columns = hospital_data.select_dtypes(exclude=[np.number]).columns

# # 为每个非数值列创建一个 LabelEncoder 实例
# encoders = {}
# for column in non_numeric_columns:
#     encoder = LabelEncoder()
#     hospital_data[column] = encoder.fit_transform(hospital_data[column])
#     encoders[column] = encoder  # 保存每个列的编码器，以便将来可能需要解码


# # from sklearn.feature_selection import mutual_info_classif

# for target in nodes:
#     candidates = result[target]  # 假设 candidates 是一个列表，包含多个候选变量
    
#     # 提取目标变量并转换为一维数组
#     target_num = hospital_data[target].values.ravel()  # 目标变量，形状为 (n_samples,)
    
#     # 提取候选变量并转换为二维数组
#     candidate_num = hospital_data[candidates].values  # 特征矩阵，形状为 (n_samples, n_features)
    
#     # 检查形状是否一致
#     print(f"Target shape: {target_num.shape}, Candidates shape: {candidate_num.shape}")
    
#     # 计算互信息
#     mi_values = mutual_info_classif(candidate_num, target_num)
    
#     # 将候选变量和互信息值组合成字典
#     mi_dict = dict(zip(candidates, mi_values))
    
#     # 按互信息值从高到低排序
#     sorted_mi = sorted(mi_dict.items(), key=lambda x: x[1], reverse=True)
    
#     # 输出排序后的结果
#     print(f"Sorted mutual information for target '{target}':")
#     for feature, mi in sorted_mi:
#         print(f"{feature}: {mi:.4f}")
#     print("=============================================")


