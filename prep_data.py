import os
import pickle
import pandas as pd
from collections import defaultdict
from typing import *
import base64

import csv
import sys
csv.field_size_limit(sys.maxsize)
from itertools import dropwhile, takewhile

DATA_DIR = "/om/user/evan_kim/ebay-comp/data"

description_paths = [os.path.join(DATA_DIR, filename) \
                     for filename in ("desc_0.csv", "desc_1.csv", "desc_2.csv", "desc_3.csv", "desc_4.csv")]
fitments_path = os.path.join(DATA_DIR, "ftmnt_train.csv")
items_path = os.path.join(DATA_DIR, "items.csv")
tags_path = os.path.join(DATA_DIR, "tags.csv")

# Utils ####################################################################################
def negate(f):
    return lambda *args, **kwargs: not f(*args, **kwargs)


def row_generator(filename, predicate):
    with open(filename, 'r', newline='\n', encoding='utf-8') as csvfile:
        datareader = csv.reader(csvfile)
        yield next(datareader)
        yield from takewhile(predicate, dropwhile(negate(predicate), datareader))
        return
    

def getdata(filename, predicate):
    for row in row_generator(filename, predicate):
        yield row


def decode_base64_html(encoded_string):
    try:
        decoded_bytes = base64.b64decode(encoded_string)
        decoded_html = decoded_bytes.decode('utf-8')
        return decoded_html
    
    except Exception as e:
        print(f"Error decoding string: {e}")
        return None

##########################################################################################

# with open('cache/train_item_to_fitments.pkl', 'rb') as f:
#     train_item_to_fitments = pickle.load(f)


first_30k_items = []
for category_id, description_path in enumerate(description_paths):
    id_category_desc: List[Tuple[str, str, str]] = []
    for row in getdata(description_path, lambda x: int(x[0]) < 30000):
        id, desc = row[0], row[1]
        id_category_desc.append((id, category_id, desc))

    id_category_desc.pop(0)  # remove the header
    first_30k_items.extend(id_category_desc)
    print(id_category_desc[0][0], id_category_desc[0][1], len(id_category_desc[0][2]))
    
    # save the decoded description to an html file
    # with open(f"cache/desc_{category_id}_example.html", 'w', encoding='utf-8') as f:
    #     f.write(decoded_desc)

assert len(first_30k_items) == 30000, "Expected 30000 items, got {len(first_30k_items)}"
first_30k_items.sort(key=lambda x: int(x[0]))  # sort by record_id


# for the first 30k items in items.csv, add the desc to a new 4th column and save that to a new file
items = pd.read_csv(items_path)
items = items.iloc[:30000]
for i in range(30000):
    assert items.iloc[i]["RECORD_ID"] == int(first_30k_items[i][0]), f"Expected {type(items.iloc[i]['RECORD_ID'])} == {(first_30k_items[i][0])}"
    assert items.iloc[i]["CATEGORY"] == int(first_30k_items[i][1]), f"Expected {type(items.iloc[i]['CATEGORY'])} == {(first_30k_items[i][0])}"
    
items["ITEM_DESC"] = [desc for _, _, desc in first_30k_items]

print(items.head())
items.to_csv("cache/items_with_desc.csv", index=False)
