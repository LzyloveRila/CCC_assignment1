from mpi4py import MPI
import json
import sys
import time
from json.decoder import JSONDecodeError


# comm = MPI.COMM_WORLD
# size = comm.Get_size()
# rank = comm.Get_rank()

# print("I am process %d of %d.\n" % (rank, size))

twitter_data = "smallTwitter.json"

def read_twitter_data():
    with open(twitter_data, "r") as f:
        for i,line in enumerate(f):
            try:
                line = line[:-2]
                data = json.loads(line)
            except JSONDecodeError:
                print(JSONDecodeError)
                continue
            except Exception as e:
                print(e)

            # do something with each line

    # with open(twitter_data, "r") as f:
    #     text = f.read()
    #     try:
    #         data = json.loads(text)
    #     except JSONDecodeError:
    #         text = text[:-2]
    #         text += "]}"
    #         data = json.loads(text)

    return data  

def language_frequency(data):
    lang_freq = {}
 
    #count frequency
    for line in data['rows']:
        text = line['doc']['text']
        language = line['doc']['metadata']['iso_language_code']
        if language != "":
            if language in lang_freq.keys():
                lang_freq[language] += 1
            else: 
                lang_freq[language] = 1

    #sort the dictionary by Descending order 
    top_10 = sorted(lang_freq.items(), key=lambda item: item[1],reverse=True)[:10]
    with open("./country_code.json",'r') as cc:
        country_code = json.load(cc)

    #search country code to get country name
    top_10_list = []
    for item in top_10:
        if item[0] in country_code.keys():
            top_10_list.append(country_code[item[0]]+"("+item[0]+"), "+str(item[1]))
        else:
            top_10_list.append(item[0]+", "+str(item[1]))
    print(top_10_list)

    return top_10_list


def hashtag_frequency(data):
    rows = data["rows"]
    hashtag_frequency = {}

    # count frequency
    for row in rows:
        hashtags = row["doc"]["entities"]["hashtags"]

        for hashtag in hashtags:
            text = hashtag["text"].lower()

            if text in hashtag_frequency:
                hashtag_frequency[text] += 1
            else:
                hashtag_frequency[text] = 1

    # sort by most used
    top_hashtags = sorted(hashtag_frequency.items(), key=lambda item: item[1], reverse=True)[:10]
    print(top_hashtags)

    return top_hashtags


if __name__ == '__main__':
    start_time = time.time()
    data = read_twitter_data()

    # if rank == 0:
    #     data1 = data
    #     print(data1)
    # else:
    #     data1 = None
    # local_data = comm.scatter(data, root=0)
    # print('rank %d, got %s:' % (rank,local_data))

    top10_lang = language_frequency(data)
    top10_hashtags = hashtag_frequency(data)

    end_time = time.time()
    time_cost = end_time - start_time
    print("time_cost:",round(time_cost,4),"seconds")