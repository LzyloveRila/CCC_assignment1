from mpi4py import MPI

import sys
size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()
print(f"Hello world! I am a process {rank} of {size}.")