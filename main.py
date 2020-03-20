from mpi4py import MPI
import json
import sys
import time
from json.decoder import JSONDecodeError


comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# print("I am process %d of %d.\n" % (rank, size))

twitter_data = "TinyTwitter.json"

def read_twitter_data():
    with open(twitter_data, "r") as f:
        try:
            data = json.load(f)
        except JSONDecodeError:
            text = f.read()
            text = text[:-2]
            text += "]}"
            data = json.loads(text)

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
    freq_rank = sorted(lang_freq.items(), key=lambda item: item[1],reverse=True)
    top_10 = freq_rank[:10]
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
    top10_lang = language_frequency(data)
    top10_hashtags = hashtag_frequency(data)
    end_time = time.time()
    time_cost = end_time - start_time
    print("time_cost:",round(time_cost,4),"seconds")