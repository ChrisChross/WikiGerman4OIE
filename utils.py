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
from jsonslicer import JsonSlicer
from time import time

def load_data(rel_path=None):
    """ Loads the data from hugginface or from local file.
    Args:
        :param rel_path: in case its loaded from locally, specify path to file and name.
    Return
        :return: the loaded data e.i. {"title":"Germany","text":"Germany is a great nation. ..."}
    """
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
    return data


def save_data(data, filename):
    """ Saves the data to a pickle file."""
    work_dir = os.path.dirname(os.path.realpath(__file__))
    file_dir = os.path.join(work_dir, filename)
    with open(f'{file_dir}', 'wb') as handle:
        pickle.dump(data, handle, protocol=pickle.HIGHEST_PROTOCOL)


def save_json_line(line, filename):
    """ Saves a single json line to a file. """
    if not os.path.exists("./data"):
        os.mkdir("./data")
    with open(filename, 'a', encoding="utf8") as outfile:
        if len(line) > 0:
            json.dump(line, outfile)
            outfile.write('\n')


def load_jsonl(filename, chunk_size=None):
    """ Loads a jsonl file, parses it to a dict and svaes it. """
    data_dict = {}
    with open(filename) as data:
        for json_line in data:
            line = json.loads(json_line)
            data_dict[line["title"]] = line["text"]
        return data_dict


def load_jsonl2(filename, chunk_size=None):
    data_dict = {}
    with open(filename) as data:
        for json_line in data:
            line = json.loads(json_line)
            for key, values in line.items():
                data_dict[key] = values
        return data_dict

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

def count_json_lines(in_file):
    ''' process each line of jsonl file and count absolute amount of examples/sentences
    :param in_file: File to count the lines in.
    :return: number of lines in jsonl-file.
    '''
    line_counter = 0
    ts = time()
    with open(in_file, encoding="utf8") as jsonl_file:
        for line in jsonl_file:
            line_counter += 1
    duration = (time() - ts)  # / 60
    print(f"It took: {duration} seconds to count the lines in the {in_file}.")
    return line_counter