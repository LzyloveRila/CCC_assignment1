# Import functools to use writing decoretor functions
import functools

# Import json to be able to read JSON formatted data
import json

# Import sys to read CLI arguments
import sys

# Import time to measure runtime of the program
import time

# Import MPI so we can run on more than one node and processor
from mpi4py import MPI


# CONSTANTS

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


def timer(func):
    """Measure and print the runtime of the decorated function"""

    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        started_at = time.perf_counter()
        value = func(*args, **kwargs)
        ended_at = time.perf_counter()
        run_time = ended_at - started_at
        print(f"Finished {func.__name__!r} in {run_time:.4f} seconds")
        return value

    return wrapper_timer


def print_top_hashtags(hashtags):
    """Display top hashtags in nicely formatted way."""

    print(HORIZONTAL_LINE)
    print("--------TOP 10 HASHTAGS-----------")
    print(HORIZONTAL_LINE)
    for index, hashtag in enumerate(hashtags, start=1):
        print(f"{index} : #{hashtag[0]}, {hashtag[1]}")


def print_top_languages(languages):
    """Display top languages in nicely formatted way."""

    print(HORIZONTAL_LINE)
    print("-------TOP 10 LANGUAGES-----------")
    print(HORIZONTAL_LINE)

    for index, hashtag in enumerate(hashtags, start=1):
        print(f"{index} : {hashtag[0]} {hashtag[1]}")


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


def tweet_processor(rank, processes, file):
    language_occurences = {}
    hashtag_occurences = {}

    with open(file, "r") as f:
        for i, line in enumerate(f):
            if i % processes == rank:
                try:
                    line = line[:-2]
                    line = json.loads(line)
                except json.decoder.JSONDecodeError:
                    print("Malformed JSON. Cannot read the line.")
                    continue
                except Exception as e:
                    print(e)

                try:
                    text = line["doc"]["text"]
                    language = line["doc"]["metadata"]["iso_language_code"]
                    if language != "":
                        if language in language_occurences.keys():
                            language_occurences[language] += 1
                        else:
                            language_occurences[language] = 1

                    hashtags = line["doc"]["entities"]["hashtags"]
                    for h in hashtags:
                        hashtag = h["text"].lower()

                        if hashtag in hashtag_occurences:
                            hashtag_occurences[hashtag] += 1
                        else:
                            hashtag_occurences[hashtag] = 1
                except:
                    print("Failed to process tweet.")

    return language_occurences, hashtag_occurences


def master_tweet_processor(comm, file):
    rank = comm.Get_rank()
    size = comm.Get_size()

    lang_frequency, hashtag_frequency = tweet_processor(rank, size, file)

    if size > 1:
        # multi process
        languages, hashtags = marshall_freq(comm, lang_frequency, hashtag_frequency)

        for i in range(size - 1):
            comm.send("exit", dest=(i + 1), tag=(i + 1))
    elif size == 1:
        # 1 node 1 core 1 process
        top_10 = sorted(lang_frequency.items(), key=lambda item: item[1], reverse=True)[
            :10
        ]
        languages = match_country(top_10)
        hashtags = sorted(
            hashtag_frequency.items(), key=lambda item: item[1], reverse=True
        )[:10]

    print_top_languages(languages)
    print_top_hashtags(hashtags)
    print(HORIZONTAL_LINE)


def slave_tweet_processor(comm, file):
    # process all relevant tweets and send our counts back
    # to master when asked
    rank = comm.Get_rank()
    size = comm.Get_size()

    lang_frequency, hashtag_frequency = tweet_processor(rank, size, file)
    data_to_send = [lang_frequency, hashtag_frequency]
    # start to listen from master
    while True:
        in_comm = comm.recv(source=MASTER_RANK, tag=rank)
        if isinstance(in_comm, str):
            if in_comm == "return_data":
                # send back data
                comm.send(data_to_send, dest=MASTER_RANK, tag=MASTER_RANK)
            elif in_comm in ("exit"):
                exit(0)


@timer
def main(argv):
    # Get input file to work on
    input_file = get_input_file(argv)
    # Identify processes' rank and size
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        # Master process enters here.
        print(HORIZONTAL_LINE)
        print(f"Master process started processing {input_file}.")
        print(f"Total number of processes is {size}.")
        master_tweet_processor(comm, input_file)
    else:
        # Slaves processes comes here.
        slave_tweet_processor(comm, input_file)


if __name__ == "__main__":
    main(sys.argv[1:])
