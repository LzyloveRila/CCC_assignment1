from mpi4py import MPI
import numpy as np

import json
import sys

size = MPI.COMM_WORLD.Get_size()
rank = MPI.COMM_WORLD.Get_rank()

print("Hello world! I am process %d of %d.\n" % (rank, size))

# from parutils import pprint
# comm = MPI.COMM_WORLD

# print("Running on %d cores" % comm.size)

# comm.Barrier()

# N = 5
# if comm.rank == 0:
# 	A = np.arange(N,dtype=np.float64)
# else:
# 	A = np.empty(N,dtype=np.float64)

# comm.Bcast([A,MPI.DOUBLE])
# print("[%02d] %s" % (comm.rank,A) 

with open("/Users/lizhengyang/Desktop/Study in unimelb/2020 S1/COMP90024 CCC/TinyTwitter.json",'r') as f:
    load = json.load(f)
    # print(load)

# for line in load['rows']:
# 	text = line['doc']['text']
# 	language = line['doc']['metadata']['iso_language_code']
# 	if language != "" and text:
# 		print(language,' - ',text)
		
print(json.dumps(load['rows'][2],indent=4))

