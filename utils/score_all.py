import pandas as pd
import os
import numpy as np
import time
from multiprocessing import Pool
from pgmpy.estimators import HillClimbSearch, AICScore
from pgmpy.estimators import BaseEstimator
from pgmpy.utils import get_example_model
import json
global_data = None

def init_worker(data):
    global global_data
    global_data = data


def compute_entropy(estimator,variable,parents):
    # tight_part = pd.DataFrame(data)
    # estimator = BaseEstimator(tight_part)
    state_counts = estimator.state_counts(variable, parents, reindex=False)

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

def parallel_learn(less_data_path):
    ALGORITHMS = ["aicscore"]

    input_data = pd.read_csv(less_data_path)
    nodes = list(input_data.columns)

    est = HillClimbSearch(data=input_data)
    estimated_model = est.estimate(
        scoring_method='aicscore',
        max_indegree=None,
        max_iter=int(1e4),
    )

    scorer = AICScore(data=input_data)

    total_scores = []
    min_scores = []
    mean_scores = []
    max_scores = []
    tight_part = pd.DataFrame(input_data)
    estimator = BaseEstimator(tight_part)

    for node in nodes:
        blanket = estimated_model.get_markov_blanket(node)
        blanket_score = compute_entropy(estimator,node,blanket)
        total_scores.append(blanket_score)  
        # # blanket_score = scorer.local_score(node, blanket)
        # blanket_score = [scorer.local_score(n, estimated_model.get_parents(n)) for n in estimated_model.nodes()]
        # total_scores.append(blanket_score)
        # min_scores.append(min(blanket_score))
        # mean_scores.append(sum(blanket_score)/len(blanket_score))
        # max_scores.append(max(blanket_score))

     # Global (table-level) score
    table_name = os.path.basename(less_data_path)
    print(f"Table name: {table_name}")
    # AIC score calculation
    print("Total AIC scores:", sum(total_scores),total_scores)
    print("Min AIC score:", min(total_scores))
    print("Mean AIC score:", sum(total_scores) / len(total_scores))
    print("Max AIC score:", max(total_scores))

    # Save score table

    print("===================================")
    import json

    # Save node-level scores
    # df = pd.DataFrame({
    #     'Node': nodes,
    #     'Total_AIC': total_scores,
    #     'Min_Node_AIC': min_scores,
    #     'Mean_Node_AIC': mean_scores,
    #     'Max_Node_AIC': max_scores
    # })
    # print(df)
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
