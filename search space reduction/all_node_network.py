from itertools import combinations

import networkx as nx
from sklearn.metrics import f1_score
import time
from pgmpy.estimators import PC, HillClimbSearch, ExhaustiveSearch
from pgmpy.estimators import K2Score
from pgmpy.utils import get_example_model
import numpy as np
from pgmpy.readwrite import BIFReader
# from pgmpy.estimators import BIC
import pandas as pd


def partition(original_path, less_path, subtable_path):
    input_data = pd.read_csv(less_path)
    original_data = pd.read_csv(original_path)
    nodes = list(input_data.columns)
    start_time = time.time()
    est = HillClimbSearch(data=input_data)
    estimated_model = est.estimate(
        scoring_method='aicscore', max_indegree=None, max_iter=int(1e4)
    )
    # print("Graph type:", type(estimated_model))
    print("Number of nodes:", estimated_model.number_of_nodes())
    print("Number of edges:", estimated_model.number_of_edges())
    # print("Edges:", estimated_model.edges())
    # get_f1_score(estimated_model, model)
    # edges = estimated_model.edges()
    corr_attrs = {}
    count = 0
    order_dict = {char: idx for idx, char in enumerate(list(nodes))}
    for node in nodes:
        corr_attrs[node] = estimated_model.get_markov_blanket(node)
        corr_attrs[node].append(node)
        tmp_subsets = corr_attrs[node]
        
        if len(tmp_subsets) >= 2:
            count +=1
            print(len(tmp_subsets))
            print(tmp_subsets)
            sub_data = original_data[tmp_subsets]
            sub_data.to_csv(subtable_path + str(count) + '.csv', index=False)
    end_time = time.time()
    print(count)
    
    print("elapsed time:", end_time -start_time)

if __name__ == '__main__':

    original_data_paths = [
        #r"./sample/sampled_data/studentfull_26.csv",
        r"./datasets/synthetic_alarm.csv"
        #r"./datasets/flights_20_500k_clean.csv",
        #r"./datasets/Amazon-sale-report-clean.csv"
        #r"./datasets/scalability/synthetic_mildew_23.csv",
        #r"./datasets/scalability/synthetic_mildew_27.csv",
        #r"./datasets/scalability/synthetic_mildew_31.csv",
        #r"./datasets/scalability/synthetic_mildew_35.csv"

        #r"./datasets/scalability/flight_8_clean.csv",
        #r"./datasets/scalability/flight_12_clean.csv",
        #r"./datasets/scalability/flight_16_clean.csv",
        #r"./datasets/scalability/synthetic_child_25W.csv",
        #r"./datasets/scalability/synthetic_child_50W.csv",
        #r"./datasets/scalability/synthetic_child_75W.csv",
        #r"./datasets/scalability/synthetic_child_100W.csv"
    ]


    less_data_paths = [
        #r"./sample/sampled_data/studentfull_26.csv",
        r"./sample/sampled_data/sampled_alarm37_less.csv"
        #r"./sample/sampled_data/sampled_flight_less.csv",
        #r"./sample/sampled_data/sampled_amazon_less.csv"
        #r"./sample/sampled_data/sampled_mildew23_less.csv",
        #r"./sample/sampled_data/sampled_mildew27_less.csv",
        #r"./sample/sampled_data/sampled_mildew31_less.csv",
        #r"./sample/sampled_data/sampled_mildew35_less.csv"

        #r"./sample/sampled_data/sampled_flight8_less.csv",
        #r"./sample/sampled_data/sampled_flight12_less.csv",
        #r"./sample/sampled_data/sampled_flight16_less.csv",
        #r"./sample/sampled_data/sampled_flight_less.csv",

        #r"./sample/sampled_data/sampled_child25w_less.csv",
        #r"./sample/sampled_data/sampled_child50w_less.csv",
        #r"./sample/sampled_data/sampled_child75w_less.csv",
        #r"./sample/sampled_data/sampled_child100w_less.csv"
    ]

    subtable_paths = [
        r"./datasets/subsets/subalarmallnode/subtable"
        #r"./datasets/subsets/submildew23allnode/subtable",
        #r"./datasets/subsets/submildew27allnode/subtable",
        #r"./datasets/subsets/submildew31allnode/subtable",
        #r"./datasets/subsets/submildew35allnode/subtable",

        #r"./datasets/subsets/subflight8allnode/subtable",
        #r"./datasets/subsets/subflight12allnode/subtable",
        #r"./datasets/subsets/subflight16allnode/subtable",
        #r"./datasets/subsets/subchild25allnode/subtable",
        #r"./datasets/subsets/subchild50allnode/subtable",
        #r"./datasets/subsets/subchild75allnode/subtable",
        #r"./datasets/subsets/subchild100allnode/subtable"
    ]

    for i in range(len(original_data_paths)):
        partition(original_data_paths[i], less_data_paths[i], subtable_paths[i])