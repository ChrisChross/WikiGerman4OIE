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
    start_pos = sent.find(phrase)
    end_pos = start_pos + len(phrase)
    return start_pos, end_pos, phrase

def add_triple(sent_triples, sdoc, subj, pred, obj): # is_not_synonym=True
    subjPos = find_pos(sdoc.text, subj)
    predPos = find_pos(sdoc.text, pred)
    objPos = find_pos(sdoc.text, obj)
    sent_triples.append((subjPos, predPos, objPos))
    return sent_triples

def match2(data: dict)->dict:
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
                tupdict = {"triples": sent_triples}
                tup = (sentext, tupdict)
                sent_trip_list.append(tup)
        if len(sent_trip_list)>0:
            train_exls.update({wikibase: sent_trip_list})
    return train_exls

def match(preped_data):
    double_dict_list = utils.split_dict_equally(preped_data, N_CORES)
    pool = Pool(N_CORES)
    work_dir = os.path.dirname(os.path.realpath(__file__))
    file_dir = os.path.join(work_dir, "data/train_data.jsonl") # data_train
    ts = time()
    with open(f"{file_dir}", 'w', encoding="utf8") as outfile:
        for dlist in double_dict_list:
            dict_list = utils.split_dict_equally(dlist, 16)
            for entry in pool.map(match2, dict_list):
                 if len(entry)>0:
                    json.dump(entry, outfile)
                    outfile.write('\n')
    duration = (time() - ts) / 60
    print(duration)