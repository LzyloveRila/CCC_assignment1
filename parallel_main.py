# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
import json
import sys
import time
from json.decoder import JSONDecodeError

# Constants
twitter_data = "smallTwitter.json"
MASTER_RANK = 0

def read_twitter_data():
    #load json data
    with open(twitter_data, "r") as f:
        text = f.read()
        try:
            data = json.loads(text)
        except JSONDecodeError:
            text = text[:-2]
            text += "]}"
            data = json.loads(text)

    return data

# def read_twitter_lines():
#     with open(twitter_data, "r") as f:
#         for i,line in enumerate(f):
#             try:
#                 line = line[:-2]
#                 data = json.loads(line)
#             except JSONDecodeError:
#                 print(JSONDecodeError)
#                 continue
#             except Exception as e:
#                 print(e)
    
#     return data

def merge_dict(x, y):
    for k,v in x.items():
        if k in y.keys():
            y[k] += v
        else:
            y[k] = v

def language_frequency(rank, processes):
    lang_freq = {}
    # data = read_twitter_data()

    with open(twitter_data, "r") as f:
        for i,line in enumerate(f):
            if i%processes == rank:
                try:
                    line = line[:-2]
                    line = json.loads(line)
                except JSONDecodeError:
                    print(JSONDecodeError)
                    continue
                except Exception as e:
                    print(e)

            # get a line now (data)
                try:
                    text = line['doc']['text']
                    language = line['doc']['metadata']['iso_language_code']
                    if language != "":
                        if language in lang_freq.keys():
                            lang_freq[language] += 1
                        else: 
                            lang_freq[language] = 1
                except:
                    print("could not interpret json")
    # try:
    #     for i, line in enumerate(data['rows']):
    #         if i%processes == rank:
    #             text = line['doc']['text']
    #             language = line['doc']['metadata']['iso_language_code']
                # if language != "":
                #     if language in lang_freq.keys():
                #         lang_freq[language] += 1
                #     else: 
                #         lang_freq[language] = 1
    # except:
    #     print("could not read data in line")
    # print(lang_freq)

    return lang_freq           

def hashtag_frequency(rank, processes):
    pass

def marshall_freq(comm, lang_freq):
    size = comm.Get_size()
    #send data request
    for i in range(size-1):
        comm.send('return_data', dest=(i+1), tag=(i+1))
    for i in range(size-1):
        #receive frequency count dict from slaves
        recv_dict = comm.recv(source=(i+1), tag=MASTER_RANK)
        #marshall the data
        merge_dict(recv_dict, lang_freq)
    #sort the dictionary by Descending order 
    top_10 = sorted(lang_freq.items(), key=lambda item: item[1],reverse=True)[:10]
    top_10_list = match_country(top_10)

    return top_10_list  

def match_country(top_10):
    with open("./country_code.json",'r') as cc:
        country_code = json.load(cc)

    #search country code to get country name
    top_10_list = []
    for item in top_10:
        if item[0] in country_code.keys():
            top_10_list.append(country_code[item[0]]+"("+item[0]+"), "+str(item[1]))
        else:
            top_10_list.append(item[0]+", "+str(item[1]))
    
    return top_10_list


def master_tweet_processor(comm):
    # Read our tweets
    rank = comm.Get_rank()
    size = comm.Get_size()

    lang_freq = language_frequency(rank, size)
    print("master:", lang_freq)
    hashtag_freq = hashtag_frequency(rank, size)
    top_10_lang = []

    if size > 1:
        top_10_lang = marshall_freq(comm, lang_freq)

        for i in range(size-1):
            comm.send('exit', dest=(i+1), tag=(i+1))
    
    print("total: ", top_10_lang)

def slave_tweet_processor(comm):
    # process all relevant tweets and send our counts back
    # to master when asked
    rank = comm.Get_rank()
    size = comm.Get_size()

    lang_freq = language_frequency(rank, size)
    #start to listen from master 
    while True:
        in_comm = comm.recv(source=MASTER_RANK, tag=rank)
        if isinstance(in_comm, str):
            if in_comm == "return_data":
                #send back data
                print("lang_freq",lang_freq)
                comm.send(lang_freq, dest=MASTER_RANK, tag=MASTER_RANK)
            elif in_comm in ("exit"):
                exit(0)

def main():
    start_time = time.time()    
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0 :
    #one master to gather results
        print("--------------------------------")
        print("I am master process %d of %d.\n" % (rank, size))
        master_tweet_processor(comm)
    else:
    # slaves work on different part of data
        print("--------------------------------")
        print("I am slave process %d of %d.\n" % (rank, size))
        slave_tweet_processor(comm)

    end_time = time.time()
    time_cost = end_time - start_time
    print("time_cost:",round(time_cost,4),"seconds")
        

if __name__ == "__main__":
    main()

