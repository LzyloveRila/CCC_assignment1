#!/bin/bash
#SBATCH --ntasks=1
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
echo "Testing"
time mpiexec -n 1 python3 main.py 
# echo "Searching for topics"
# time mpiexec -n 1 python3 twitter_search_541635.py -i minitwitter.csv
# echo "Searching for the keyword 'jumping'"
# time mpiexec -n 1 python3 twitter_search_541635.py -i minitwitter.csv
