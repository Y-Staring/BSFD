## Fast Discovery of Functional Dependencies
via Bayesian Network Learning

> Under this project, implementations are provided for the representative sampling method,  the correlated attribute extraction method, the search space construction algorithm SubLearner,  the parallel discovery method PFMiner, as well as code for testing precision and recall, involving corresponding implementation algorithms.

## Quick Start Guide

The code compiles with Python 3.10. We have set default parameters in the algorithm. Below are the environments and their respective version numbers that you need to configure:

```
# Configure the environment with one click
pip install -r requirements.txt
# Run the code with one click
python xx.py
```

## Project structure

To make it easier for readers to understand the roles and execution of each algorithm in the article, we have divided the main functional modules into sub folders and placed independently executable Python files within them.

Specifically, the folder divisions are as follows:

> 1. The `baselines` folder implements baseline methods: TANE and DFD. Support and confidence thresholds are set in the source before discovery.
> 2. The `sample` folder implements corresponding algorithms for a stratified sample.
> 3. The `search space reduction` implements correlated attributes extraction, sub-table partition(per-node) ,  the global BN variant(all_node), and reduced search space ratio calculation.
> 4. The `parallel discovery` folder implements parallel discovery across multiple sub-tables.
> 5. The `datasets` folder provides synthetic data and variants of real-life data for scalability tests. And a clean version of real-life data and corresponding sub-tables under each experiment setting.
> 6. The `utils` folder contains implementations of precision and recall calculation, network scoring, data synthetic methods, and example results.
