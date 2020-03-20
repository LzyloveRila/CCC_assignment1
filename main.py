from mpi4py import MPI
# import numpy as np

import json
import sys

# size = MPI.COMM_WORLD.Get_size()
# rank = MPI.COMM_WORLD.Get_rank()

# print("Hello world! I am process %d of %d.\n" % (rank, size))

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

twitter_data = "tinyTwitter.json"

def read_twitter_data():
    with open(twitter_data, "r") as f:
        text = f.read()
        text = text[:-2]
        text += "]}"
        data = json.loads(text)

    return data
    

def language_frequency():
    for line in load['rows']:
        text = line['doc']['text']
        language = line['doc']['metadata']['iso_language_code']
        if language != "" and text:
            print(language,' - ',text)
            
    print(json.dumps(load['rows'][2],indent=4))


def hashtag_frequency():
    data = read_twitter_data()
    rows = data["rows"]

    for row in rows:
        hashtags = row["doc"]["entities"]["hashtags"]

        for hashtag in hashtags:
            print(hashtag["text"])


if __name__ == '__main__':
    hashtag_frequency()
    language_frequency()
