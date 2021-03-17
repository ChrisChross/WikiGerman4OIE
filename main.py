import os
import sys
import re
import json
import pprint
# import wptools
import logging
import wikipedia
import pandas as pd
import urllib.request
from time import time
import streamlit as st
import itertools as it
from datasets import load_dataset
import pickle
import itertools
from itertools import islice, repeat
from functools import partial
import multiprocessing as mp
from multiprocessing.pool import Pool
from typing import List, Tuple, Dict, NamedTuple, TypedDict
import utils
import preprocessing as prep
import matcher

### Global Variables ###
N_CORES = mp.cpu_count()


WikiDict = Dict[str,str]
def preprocess_wiki_text(data:Dict[str,str]) -> WikiDict:
    """
    Args:
        :param data:
    Return
        :return:
    """
    out_dict = {}
    for title, text in data.items():
        text = prep.prep_text(data[title]) # main cleaning function
        if (len(text.split()) > 100):
            out_dict[title] = text
    return out_dict
    # save_json(out_dict, path="data/data_small_clean.json") # text_middle_preped

##### 2. INFOBOX CLEANING - OPTION2: Multi Process #######
def get_infobox_data(data):
    """
    :param data:
    :return:
    """
    dict_list = utils.split_dict_equally(data, N_CORES) # out_dict , preped_data
    ts = time()
    with Pool(N_CORES) as pool:
       train_data = pool.map(prep.prep_data, dict_list)
    duration = (time() - ts)  # / 60
    st.write(str(duration) + " sec" )
    preped_data = {}
    for d in train_data:
        preped_data.update(d)
    st.write(f"Pages stored: {len(preped_data)}")
    triple_count = 0
    for key, value in preped_data.items():
        for triple in value[0]["triples"]:
            triple_count+=1
    st.write(f"Triples found: {triple_count}")
    return preped_data

def main():
    st.title("Dataset")
    data, titles, texts = utils.load_data() # careful, loads 12 GB of data
    preped_wiki_texts = preprocess_wiki_text(data) # Prep text from Wiki
    preped_infobox_texts = get_infobox_data(preped_wiki_texts) # Get Articles and Infoboxs mapped
    matcher.match(preped_infobox_texts) #match sentences and triple (infobox values) => saving in data as jsonl

if __name__ == "__main__":
    main()