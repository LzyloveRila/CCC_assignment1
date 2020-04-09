from json.decoder import JSONDecodeError
import json
import time


twitter_data = "tinyTwitter.json"

# def read_twitter_data():
#     with open(twitter_data, "r") as f:
#         text = f.readline()
#         try:
#             data = json.loads(text)
#         except JSONDecodeError:
#             text = text[:-2]
#             text += "]}"
#             data = json.loads(text)

#     return data

# data = read_twitter_data()
# print("1")
start_time = time.time()
with open(twitter_data, "r") as f:
    for i,line in enumerate(f):
        try:
            line = line[:-2]
            line = json.loads(line)
        except JSONDecodeError:
            print(JSONDecodeError)
            continue
        except Exception as e:
            print(e)

        # print(line)
end_time = time.time()
print("total time cost:", round(end_time-start_time,4))
