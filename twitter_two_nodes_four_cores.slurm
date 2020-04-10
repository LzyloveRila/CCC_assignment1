#!/bin/bash
# 2 nodes, 4 tasks per node = 8 cores
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --time=0-1:00:00
#SBATCH --partition=physical

# Send yourself an email when the job:
# aborts abnormally (fails)
#SBATCH --mail-type=FAIL
# begins
#SBATCH --mail-type=BEGIN
# ends successfully
#SBATCH --mail-type=END

# Load required modules
module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9

# Launch multiple process python code
mpiexec -n 8 python3 parallel_main.py 