import os
import re
import json
# import wptools
import wikipedia
import pandas as pd
import urllib.request
import streamlit as st
from datasets import load_dataset
import pickle
import itertools
from itertools import islice


def load_data(rel_path=None):
    if (rel_path==None):
        dataset = load_dataset("wikipedia", '20200501.de', split='train')
        titles = dataset["title"]
        texts = dataset["text"]
        glitter = zip(titles, texts)
        data = dict(glitter)
        return data, titles, texts
    elif rel_path:
        work_dir = os.path.dirname(os.path.realpath(__file__))
        file_dir = os.path.join(work_dir, rel_path)
        with open(file_dir, 'rb') as handle:
            data = pickle.load(handle)
            # return dataset
    return data


def save_data(data, filename):
    work_dir = os.path.dirname(os.path.realpath(__file__))
    file_dir = os.path.join(work_dir, filename)
    with open(f'{file_dir}', 'wb') as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)


def split_dict_equally(input_dict, chunks=2):
    """
    Splits dict by keys. Returns a list of dictionaries.
    """
    return_list = [dict() for idx in range(chunks)]
    idx = 0
    for k,v in input_dict.items():
        return_list[idx][k] = v
        if idx < chunks-1:
            idx += 1
        else:
            idx = 0
    return return_list