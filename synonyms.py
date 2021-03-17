import itertools
from typing import List
from py_openthesaurus import OpenThesaurusDb

open_thesaurus = OpenThesaurusDb(host="localhost", user="chris", passwd="123", db_name="database_name")

def get_syns(word:str)->List[str]:
    """Get all synonyms for a word.
    Args:
        :param: word: the word to get the synonyms for.
    Returns:
        A list of all found synonyms.
    """
    synonyms = []
    synonyms = open_thesaurus.get_synonyms(word=word)
    return synonyms

def permuts(subj, pred, obj):
    syns_subj = get_syns(subj)
    syns_pred = get_syns(pred)
    syns_obj = get_syns(obj)
    if len(syns_subj)<=0:
        syns_subj.append(subj)
    if len(syns_pred)<=0:
        syns_pred.append(pred)
    if len(syns_obj)<=0:
        syns_obj.append(obj)
    permuts = [permu for permu in itertools.product(syns_subj, syns_pred, syns_obj)]
    return permuts