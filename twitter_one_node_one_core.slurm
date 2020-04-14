#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=0-1:00:00
#SBATCH --partition=physical

# Purge old modules
module purge

# Load required modules
module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9

# Change these as appropriate
INPUT_FILE=smallTwitter.json

# Launch multiple process python code
time mpiexec -n 1 python3 parallel_main.py $INPUT_FILE
