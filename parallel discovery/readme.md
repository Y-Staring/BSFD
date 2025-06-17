## Fast Discovery of Functional Dependencies via Bayesian Network Learning

> This code correspond to algorithm PFMiner.

## Quick Start Guide
The code compiles with cmake. The program takes three arguments:

1. The first parameter is specified in input.txt (names of CSV sub-tables).
2. The remaining two parameters (number of files and baseline variant) are specified in the source. Support and confidence threshold are set in the  `algorithmsClassic` folder.

**OpenMP parallel command：**

> 1.compile CMakeLists.txt
> 2.**Change the number of files and input directories** in input.txt and main.cpp
> 3.Go to the **cmake-build-debug directory** and run the command **cmake../**
> 3.# Start 30 threads (for example) with the following command:
> ulimit -s unlimited && mpiexec -n 30 -mca btl ^openib BSFD

