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

twitter_data = "TinyTwitter.json"

def read_twitter_data():
    with open(twitter_data, "r") as f:
        text = f.read()
        text = text[:-2]
        text += "]}"
        data = json.loads(text)

    return data
    

def language_frequency(data):
    lang_freq = {}
 
    for line in data['rows']:
        text = line['doc']['text']
        language = line['doc']['metadata']['iso_language_code']
        if language != "":
            if language in lang_freq.keys():
                lang_freq[language] += 1
            else: 
                lang_freq[language] = 1

    #sort the dictionary by Descending order 
    freq_rank = sorted(lang_freq.items(), key=lambda item: item[1],reverse=True)
    
    # print(freq_rank)
    # print(json.dumps(data['rows'][2],indent=4))
    top_10 = freq_rank[:10]
    with open("./country_code.json",'r') as cc:
        country_code = json.load(cc)
        # print(country_code)

    # print(top_10)
    top_10_list = []
    for item in top_10:
        if item[0] in country_code.keys():
            top_10_list.append(country_code[item[0]]+"("+item[0]+"), "+str(item[1]))
        else:
            top_10_list.append(item[0]+", "+str(item[1]))
    print(top_10_list)

    return top_10_list


def hashtag_frequency():
    data = read_twitter_data()
    rows = data["rows"]

    for row in rows:
        hashtags = row["doc"]["entities"]["hashtags"]

        for hashtag in hashtags:
            print(hashtag["text"])


if __name__ == '__main__':
    data = read_twitter_data()
    # hashtag_frequency()
    top10_lang = language_frequency(data)

