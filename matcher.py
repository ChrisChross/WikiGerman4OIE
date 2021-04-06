import re
import os
import json
import spacy
import utils
import synonyms
from time import time
import multiprocessing as mp
from synonyms import get_syns, permuts
from multiprocessing.pool import Pool

N_CORES = mp.cpu_count()
nlp = spacy.load('de_core_news_lg')

def find_pos(sent, phrase):
    """ find the start end end pos."""
    start_pos = sent.find(phrase)
    end_pos = start_pos + len(phrase)
    return start_pos, end_pos, phrase

def add_triple(sent_triples, sdoc, subj, pred, obj): # is_not_synonym=True
    """ Adds triple to sentence."""
    subjPos = find_pos(sdoc.text, subj)
    predPos = find_pos(sdoc.text, pred)
    objPos = find_pos(sdoc.text, obj)
    sent_triples.append((subjPos, predPos, objPos))
    return sent_triples

def match2(data: dict)->dict:
    """ Does perform the real matching. """
    train_exls = {}
    for wikibase, content in data.items():
        page_triples = content[0]["triples"]
        page_text = content[1]["text"]
        doc = nlp(page_text)
        sent_trip_list = []
        for sent in doc.sents:
            sdoc = sent.as_doc()
            sent_triples = []
            for triple in page_triples:
                subj, pred, obj = triple
                if subj != obj:
                    permuts = synonyms.permuts(subj,pred, obj) # List of Tuples
                    for tupl in permuts:
                        if all(tok in sdoc.text for tok in tupl):
                            sub, pre, ob = tupl
                            sent_triples = add_triple(sent_triples, sdoc, sub, pre, ob)
                            break
            if len(sent_triples)>0:
                regexp = re.compile(r"\s$")
                sentext = re.sub(regexp, '', str(sdoc.text))
                train_exl = {"sentence": sentext, "triples": sent_triples}
                sent_trip_list.append(train_exl)
        if len(sent_trip_list)>0:
            train_exls.update({wikibase: sent_trip_list})
    return train_exls

def match(preped_data_file, out_file):
    """ Matches a sentence with a triple if all its words or synoynms are entailed in the sentence it.
    Args:
        :param preped_data_file: Filename/path to mapped texts.
        :param out_file: Filename/path where file should be saved to.
    """
    preped_data = utils.load_jsonl2(preped_data_file)
    double_dict_list = utils.split_dict_equally(preped_data, N_CORES)
    pool = Pool(N_CORES)
    work_dir = os.path.dirname(os.path.realpath(__file__))
    file_dir = os.path.join(work_dir, out_file)
    ts = time()
    with open(file_dir, 'a', encoding="utf8") as outfile:
        for dlist in double_dict_list:
            dict_list = utils.split_dict_equally(dlist, 16)
            for entry in pool.map(match2, dict_list): # calls lower function with multiprocessing.
                 if len(entry)>0:
                     for key, train_exls in entry.items():
                         for train_exl in train_exls:
                            json.dump(train_exl, outfile)
                            outfile.write('\n')
    duration = (time() - ts) / 60
    print(duration)