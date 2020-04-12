# Import MPI so we can run on more than one node and processor
import functools
import json
import sys
import time
from json.decoder import JSONDecodeError

from mpi4py import MPI

# Constants

DEFAULT_FILE = "smallTwitter.json"
COUNTRY_CODE_FILE = "country_code.json"
MASTER_RANK = 0
HORIZONTAL_LINE = "----------------------------------"


def merge_dict(x, y):
    for k, v in x.items():
        if k in y.keys():
            y[k] += v
        else:
            y[k] = v


def get_input_file(argv):
    """Read input file name from arguments or return default one"""

    if len(argv) > 0:
        return argv[0]

    return DEFAULT_FILE


def tweet_processor(rank, processes, file):
    lang_freq = {}
    hashtag_frequency = {}

    with open(file, "r") as f:
        for i, line in enumerate(f):
            if i % processes == rank:
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
                    text = line["doc"]["text"]
                    language = line["doc"]["metadata"]["iso_language_code"]
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


def marshall_freq(comm, lang_freq, htag_freq):
    size = comm.Get_size()
    # send data request
    for i in range(size - 1):
        comm.send("return_data", dest=(i + 1), tag=(i + 1))
    for i in range(size - 1):
        # receive frequency count dict from slaves
        recv_dict_list = comm.recv(source=(i + 1), tag=MASTER_RANK)
        # marshall the data
        merge_dict(recv_dict_list[0], lang_freq)
        merge_dict(recv_dict_list[1], htag_freq)
    # sort the dictionary by Descending order
    lang_freq = sorted(lang_freq.items(), key=lambda item: item[1], reverse=True)[:10]
    top_10_htag = sorted(htag_freq.items(), key=lambda item: item[1], reverse=True)[:10]

    top_10_lang = match_country(lang_freq)

    return top_10_lang, top_10_htag


def match_country(top_10):
    with open(COUNTRY_CODE_FILE, "r") as f:
        country_code = json.load(f)

    # search country code to get country name
    top_10_list = []
    for item in top_10:
        if item[0] in country_code.keys():
            top_10_list.append(
                country_code[item[0]] + "(" + item[0] + "), " + str(item[1])
            )
        else:
            top_10_list.append(item[0] + ", " + str(item[1]))

    return top_10_list


def master_tweet_processor(comm, file):
    # Read our tweets
    rank = comm.Get_rank()
    size = comm.Get_size()

    lang_freq, hashtag_frequency = tweet_processor(rank, size, file)
    top_10_lang = []

    if size > 1:
        # multi process
        top_10_lang, top_10_htag = marshall_freq(comm, lang_freq, hashtag_frequency)

        for i in range(size - 1):
            comm.send("exit", dest=(i + 1), tag=(i + 1))
    elif size == 1:
        # 1 node 1 core 1 process
        top_10 = sorted(lang_freq.items(), key=lambda item: item[1], reverse=True)[:10]
        top_10_lang = match_country(top_10)
        top_10_htag = sorted(
            hashtag_frequency.items(), key=lambda item: item[1], reverse=True
        )[:10]

    print(HORIZONTAL_LINE)
    print("-------Top 10 Languages-------")
    print(HORIZONTAL_LINE)
    for i in range(len(top_10_lang)):
        print(i + 1, " : ", top_10_lang[i])
    print(HORIZONTAL_LINE)
    print("--------Top 10 Hashtags-----------")
    print(HORIZONTAL_LINE)
    for i in range(len(top_10_htag)):
        print(i + 1, " : ", top_10_htag[i][0], top_10_htag[i][1])
    print(HORIZONTAL_LINE)


def slave_tweet_processor(comm, file):
    # process all relevant tweets and send our counts back
    # to master when asked
    rank = comm.Get_rank()
    size = comm.Get_size()

    lang_freq, hashtag_frequency = tweet_processor(rank, size, file)
    data = [lang_freq, hashtag_frequency]
    # start to listen from master
    while True:
        in_comm = comm.recv(source=MASTER_RANK, tag=rank)
        if isinstance(in_comm, str):
            if in_comm == "return_data":
                # send back data
                comm.send(data, dest=MASTER_RANK, tag=MASTER_RANK)
            elif in_comm in ("exit"):
                exit(0)


def timer(func):
    """Print the runtime of the decorated function"""

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        started_at = time.perf_counter()
        value = func(*args, **kwargs)
        ended_at = time.perf_counter()
        run_time = ended_at - started_at
        print(f"Finished {func.__name__!r} in {run_time:.4f} seconds")
        return value

    return wrapper_timer


@timer
def main(argv):
    # Get input file to work on
    input_file = get_input_file(argv)
    # Identify processes' rank and size
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        # a master to gather results
        print(HORIZONTAL_LINE)
        print(f"I am master process, processing {input_file}")
        print(f"Total number of processes is {size}.")
        master_tweet_processor(comm, input_file)
    else:
        # slaves work on different part of data
        slave_tweet_processor(comm, input_file)


if __name__ == "__main__":
    main(sys.argv[1:])
