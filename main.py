import os
import sys
import re
import json
from time import time
import streamlit as st
import multiprocessing as mp
from multiprocessing.pool import Pool
from typing import List, Tuple, Dict, NamedTuple, TypedDict
import utils
import preprocessing as prep
import matcher

### Global Variables ###
N_CORES = mp.cpu_count()


WikiDict = Dict[str,str]
def preprocess_wiki_text(data: Dict[str, str], filename:str): # -> WikiDict
    """ Cleans the text to only keep complete sentences.
    Args:
        :param data: the Wikipeida article_title, and it's text
    Return
        :return: None => (saves the data in jsonl file.)
    """
    out_dict = {}
    for title, text in data.items():
        text = prep.prep_text(data[title]) # main cleaning function
        if (len(text.split()) > 100): # Only take texts with more than 100 words.
            out_dict["title"] = title
            out_dict["text"] = text
            utils.save_json_line(out_dict, filename)
    # return out_dict
    # save_json(out_dict, path="data/data_small_clean.json") # text_middle_preped


def get_infobox_data(in_file, out_file):
    """ INFOBOX CLEANING - OPTION1: Single Processing #######
    :param in_file: file from where to take the titles and texts for crawling and mapping.
    :param out_file: Filename/path where to save the created mapped texts and triples.
    :return: None
    """
    train_exs_count = 0
    ts = time()
    train_exs_keys = []
    with open(in_file) as data:
        for json_line in data:
            line = json.loads(json_line)
            print(line)
            train_example = prep.prep_example(line)
            if len(train_example)>0:
                first_key = list(train_example.keys())[0]
                if first_key not in train_exs_keys:
                    train_exs_keys.append(first_key)
                    utils.save_json_line(train_example, out_file)
                    train_exs_count+=1
                    print(train_exs_count)
    duration = (time() - ts)  # / 60
    st.write(str(duration) + " sec" )


def process_batches(batch):
    """ Takes a batch and extracts all matches.
    Args
        :param batch: a dict holding a bunch of items.
    Results
        :return: the training examples from the batch.
    """
    ts = time()
    train_exls = []
    exs = len(batch)
    for json_line in batch.items():
        exs -= 1
        train_exl = prep.prep_example(json_line)
        if len(train_exl)>0:
            train_exls.append(train_exl)
    duration = time() - ts  / 60
    print(f"Left in batch: {exs}")
    print(str(duration) + " sec" )
    return train_exls

def get_infobox_data_multi(in_file, out_file):
    """ Same as get_infobox_data - only utilizing multiprocessing.
    Args
        :param in_file: File from where to take the titles and texts for crawling and mapping.
        :param out_file: Filename/path where to save the created mapped texts and triples.
    Results
        :return: None
    """
    data = utils.load_jsonl(in_file)
    data_dict = utils.split_dict_equally(data, N_CORES) # out_dict , preped_data
    dict_length = len(data_dict)
    pool = Pool(N_CORES)
    ts = time()
    with open(out_file, 'a', encoding="utf8") as outfile:
        for idx, dlist in enumerate(data_dict):
            if idx == dict_length-1:
                print("stop")
            dict_list = utils.split_dict_equally(dlist, N_CORES)
            for train_exls in pool.map(process_batches, dict_list):
                if len(train_exls) > 0:
                    for train_exl in train_exls:
                        json.dump(train_exl, outfile)
                        outfile.write('\n')
    duration = time() - ts / 60
    print(str(duration) + " sec")
    utils.count_json_lines(out_file)

def main():
    st.title("Dataset")
    data, titles, texts = utils.load_data() # careful, loads 12 GB of data
    preprocess_wiki_text(data, "preped_wikitexts.jsonl" ) # Prep text from Wiki
    get_infobox_data("./data/preped_wikitexts.jsonl", "./data/matched_texts.jsonl") # Get Articles and Infoboxs mapped
    # or use for speedup => get_infobox_data_multi("./data/preped_wikitexts.jsonl", "./data/matched_texts.jsonl")
    matcher.match("./data/matched_texts.jsonl", "/data/train_data.jsonl") # match sentences and triple (infobox values) => saving in data as jsonl

if __name__ == "__main__":
    main()