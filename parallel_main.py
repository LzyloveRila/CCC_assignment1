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


def print_top_languages(languages):
    print(HORIZONTAL_LINE)
    print("-------Top 10 Languages-----------")
    print(HORIZONTAL_LINE)
    for index, language in enumerate(languages, start=1):
        print(f"{index} : {language}")


def print_top_hashtags(hashtags):
    print(HORIZONTAL_LINE)
    print("--------Top 10 Hashtags-----------")
    print(HORIZONTAL_LINE)
    for index, hashtag in enumerate(hashtags, start=1):
        print(f"{index} : #{hashtag[0]} {hashtag[1]}")


def match_country(languages):
    with open(COUNTRY_CODE_FILE, "r") as f:
        country_code = json.load(f)

    # search country code to get country name
    top_languages = []
    for item in languages:
        if item[0] in country_code.keys():
            top_languages.append(
                country_code[item[0]] + "(" + item[0] + "), " + str(item[1])
            )
        else:
            top_languages.append(item[0] + ", " + str(item[1]))

    return top_languages


def marshall_freq(comm, language_frequency, hashtag_frequency):
    size = comm.Get_size()
    # send data request
    for i in range(size - 1):
        comm.send("return_data", dest=(i + 1), tag=(i + 1))
    for i in range(size - 1):
        # receive frequency count dict from slaves
        recv_dict_list = comm.recv(source=(i + 1), tag=MASTER_RANK)
        # marshall the data
        merge_dict(recv_dict_list[0], language_frequency)
        merge_dict(recv_dict_list[1], hashtag_frequency)

    # sort the dictionary by descending order
    language_frequency = sorted(
        language_frequency.items(), key=lambda item: item[1], reverse=True
    )[:10]
    top_hashtags = sorted(
        hashtag_frequency.items(), key=lambda item: item[1], reverse=True
    )[:10]

    top_languages = match_country(language_frequency)

    return top_languages, top_hashtags


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
                        language_occurences[language] = (
                            language_occurences.setdefault(language, 0) + 1
                        )

                    hashtags = line["doc"]["entities"]["hashtags"]
                    for h in hashtags:
                        hashtag = h["text"].lower()
                        hashtag_occurences[hashtag] = (
                            hashtag_occurences.setdefault(hashtag, 0) + 1
                        )
                except:
                    print("Failed to process tweet.")

    return language_occurences, hashtag_occurences


def master_tweet_processor(comm, file):
    # Read tweets
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

    # Print results
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
