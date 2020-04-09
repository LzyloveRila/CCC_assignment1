# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
import json
import sys
import time
from json.decoder import JSONDecodeError

# Constants
twitter_data = "smallTwitter.json"
MASTER_RANK = 0

# def read_twitter_data():
#     #load json data
#     with open(twitter_data, "r") as f:
#         text = f.read()
#         try:
#             data = json.loads(text)
#         except JSONDecodeError:
#             text = text[:-2]
#             text += "]}"
#             data = json.loads(text)

#     return data

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
    hashtag_frequency = {}

    with open(twitter_data, "r") as f:
        for i,line in enumerate(f):
            if i%processes == rank:
                try:
                    line = line[:-2]
                    line = json.loads(line)
                except JSONDecodeError:
                    print("A line cannot be read ocurred, skip it")
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
                    
                    hashtags = line["doc"]["entities"]["hashtags"]
                    for hashtag in hashtags:
                        hash_t = hashtag["text"].lower()

                        if hash_t in hashtag_frequency:
                            hashtag_frequency[hash_t] += 1
                        else:
                            hashtag_frequency[hash_t] = 1                   
                except:
                    print("could not interpret json")

    return lang_freq, hashtag_frequency          

# def hashtag_frequency(rank, processes):
#     hashtag_frequency = {}

#     with open(twitter_data, "r") as f:
#         for i,line in enumerate(f):
#             if i%processes == rank:
#                 try:
#                     line = line[:-2]
#                     line = json.loads(line)
#                 except JSONDecodeError:
#                     print("A line cannot be read ocurred, skip it")
#                     continue
#                 except Exception as e:
#                     print(e)

#                 try:
#                     hashtags = line["doc"]["entities"]["hashtags"]
#                     for hashtag in hashtags:
#                         text = hashtag["text"].lower()

#                         if text in hashtag_frequency:
#                             hashtag_frequency[text] += 1
#                         else:
#                             hashtag_frequency[text] = 1
#                 except:
#                     print("could not interpret json")
    
#     return hashtag_frequency

    

def marshall_freq(comm, lang_freq, htag_freq):
    size = comm.Get_size()
    #send data request
    for i in range(size-1):
        comm.send('return_data', dest=(i+1), tag=(i+1))
    for i in range(size-1):
        #receive frequency count dict from slaves
        recv_dict_list = comm.recv(source=(i+1), tag=MASTER_RANK)
        #marshall the data
        merge_dict(recv_dict_list[0], lang_freq)
        merge_dict(recv_dict_list[1], htag_freq)
    #sort the dictionary by Descending order 
    lang_freq = sorted(lang_freq.items(), key=lambda item: item[1],reverse=True)[:10]
    top_10_htag = sorted(htag_freq.items(), key=lambda item: item[1],reverse=True)[:10]
    
    top_10_lang = match_country(lang_freq)

    return top_10_lang, top_10_htag

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

    lang_freq, hashtag_frequency = language_frequency(rank, size)
    # print("master:", lang_freq)
    # hashtag_freq = hashtag_frequency(rank, size)
    top_10_lang = []

    if size > 1:
        # multi process
        top_10_lang, top_10_htag = marshall_freq(comm, lang_freq, hashtag_frequency)

        for i in range(size-1):
            comm.send('exit', dest=(i+1), tag=(i+1))
    elif size == 1:
        # 1 node 1 core 1 process
        top_10 = sorted(lang_freq.items(), key=lambda item: item[1],reverse=True)[:10]
        top_10_lang = match_country(top_10)
        top_10_htag = sorted(hashtag_frequency.items(), key=lambda item: item[1],reverse=True)[:10]
    
    print("----------------------------------")
    print("-------top 10 language used-------")
    print("----------------------------------")
    for i in range(len(top_10_lang)):
        print(i+1," : ",top_10_lang[i])
    print("----------------------------------")
    print("--------top 10 hashtags-----------")
    print("----------------------------------")
    for i in range(len(top_10_htag)):
        print(i+1," : ",top_10_htag[i][0],top_10_htag[i][1])
    print("----------------------------------")

def slave_tweet_processor(comm):
    # process all relevant tweets and send our counts back
    # to master when asked
    rank = comm.Get_rank()
    size = comm.Get_size()

    lang_freq, hashtag_frequency = language_frequency(rank, size)
    data = [lang_freq, hashtag_frequency]
    #start to listen from master 
    while True:
        in_comm = comm.recv(source=MASTER_RANK, tag=rank)
        if isinstance(in_comm, str):
            if in_comm == "return_data":
                #send back data
                # print("lang_freq",lang_freq)
                comm.send(data, dest=MASTER_RANK, tag=MASTER_RANK)
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
        print("I am master process, total number of process is %d.\n" % (size))
        master_tweet_processor(comm)
    else:
    # slaves work on different part of data
        # print("--------------------------------")
        # print("I am slave process %d of %d.\n" % (rank, size))
        slave_tweet_processor(comm)

    end_time = time.time()
    time_cost = end_time - start_time
    print("time_cost:",round(time_cost,4),"seconds")
        

if __name__ == "__main__":
    main()

