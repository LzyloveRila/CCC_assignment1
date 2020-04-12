#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=0-1:00:00
#SBATCH --partition=physical

#SBATCH --job-name="process_twitter_1019332"
#SBATCH --output=result_one_node_eight_cores.out
#SBATCH --mail-user=mkarimov@student.unimelb.edu.au
#SBATCH --mail-type=ALL


# Load required modules
module load Python/3.6.4-intel-2017.u2-GCC-6.2.0-CUDA9

# Launch multiple process python code
mpiexec -n 8 python3 parallel_main.py 
