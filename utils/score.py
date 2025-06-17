import pandas as pd
import os
import time
from multiprocessing import Pool
from pgmpy.estimators import HillClimbSearch, AICScore
from pgmpy.utils import get_example_model
from pgmpy.estimators import BaseEstimator
import numpy as np
global_data = None
def compute_entropy(estimator,variable,parents):
    # tight_part = pd.DataFrame(data)
    # estimator = BaseEstimator(tight_part)
    state_counts = estimator.state_counts(variable, parents, reindex=False)

    #转换为numpy数组
    counts = np.asarray(state_counts)
    # print("counts:", counts)
    log_likelihoods = np.zeros_like(counts, dtype=float)

    # Compute the log-counts
    np.log(counts, out=log_likelihoods, where=counts > 0)
    # print("log_likelihoods:", log_likelihoods)
    # Compute the log-conditional sample size
    log_conditionals = np.sum(counts, axis=0, dtype=float)
    np.log(log_conditionals, out=log_conditionals, where=log_conditionals > 0)
    # print("log_conditionals:", log_conditionals)
    # Compute the log-likelihoods
    log_likelihoods -= log_conditionals  # log(p(x|parents))
    log_likelihoods *= counts
    score = np.sum(log_likelihoods)
    return score
def init_worker(data):
    global global_data
    global_data = data


def process_node(args):
    algorithm, node, black_list = args
    input_data = global_data

    est = HillClimbSearch(data=input_data)
    estimated_model = est.estimate(
        scoring_method=algorithm,
        max_indegree=None,
        max_iter=int(1e4),
        black_list=black_list
    )
    tight_part = pd.DataFrame(input_data)
    estimator = BaseEstimator(tight_part)
    tmp_corr = estimated_model.get_markov_blanket(node)
    blanket_score = compute_entropy(estimator,node,tmp_corr)
        # total_scores.append(blanket_score)  
    # tmp_corr.append(node)

    # AIC score calculation
    # scorer = AICScore(data=input_data)
    # total_score = scorer.score(estimated_model)
    # node_scores = [scorer.local_score(n, estimated_model.get_parents(n)) for n in estimated_model.nodes()]
    # min_score = min(node_scores)
    # max_score = max(node_scores)
    # mean_score = sum(node_scores) / len(node_scores)

    # return tmp_corr, total_score, min_score, mean_score, max_score
    return blanket_score


def parallel_learn(less_data_path):
    ALGORITHMS = ["aicscore"]

    hospital_data = pd.read_csv(less_data_path)
    nodes = list(hospital_data.columns)

    # unique_nodes = []
    # non_unique_nodes = []

    # for node in nodes:
    #     if hospital_data[node].nunique() > 1:
    #         non_unique_nodes.append(node)
    #     else:
    #         unique_nodes.append(node)

    black_lists_each_node = {}
    for target_feature in nodes:
        black_list = []
        for x in nodes:
            for y in nodes:
                if x != y and x != target_feature and y != target_feature:
                    black_list.append((x, y))
        black_lists_each_node[target_feature] = black_list

    task_args = [
        (algorithm, node, black_lists_each_node[node])
        for algorithm in ALGORITHMS
        for node in nodes
    ]

    print("length of args:", len(task_args))

    start_time = time.time()
    with Pool(processes=os.cpu_count()-1, initializer=init_worker, initargs=(hospital_data,)) as pool:
        results = pool.map(process_node, task_args)

    end_time = time.time()
    print("Time elapsed: ", end_time - start_time)

    # corr_sets, total_scores, min_scores, mean_scores, max_scores = zip(*results)
    total_scores = results

    # Save score table
    table_name = os.path.basename(less_data_path)
    print(f"Table name: {table_name}")
    # AIC score calculation
    print("Total AIC scores:", sum(total_scores),total_scores)
    print("Min AIC score:", min(total_scores))
    print("Mean AIC score:", sum(total_scores) / len(total_scores))
    print("Max AIC score:", max(total_scores))
    print("===================================")

if __name__ == '__main__':
    less_data_paths = [
        r"./sample/sampled_data/sampled_hosp_less.csv",
        r"./sample/sampled_data/sampled_amazon_less.csv",
        r"./sample/sampled_data/sampled_flight_less.csv",
        r"./sample/sampled_data/studentfull_26.csv",
        r"./sample/sampled_data/sampled_adult_less.csv",
        r"./sample/sampled_data/sampled_statlog_german_less.csv"
    ]


    for less_data_path in less_data_paths:
        parallel_learn(less_data_path)
