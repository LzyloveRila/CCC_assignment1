#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --time=0-12:00:00
#SBATCH --partition=physical

# Send yourself an email when the job:
# aborts abnormally (fails)
#SBATCH --mail-type=FAIL
# begins
#SBATCH --mail-type=BEGIN
# ends successfully
#SBATCH --mail-type=END

# Load required modules
module load Python/3.7.3-spartan_gcc-8.1.0

# Launch multiple process python code
time mpiexec -n 4 python3 parallel_main.py 
