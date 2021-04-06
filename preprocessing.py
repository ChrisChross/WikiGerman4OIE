import re
import wptools
import utils
from typing import List, Dict, Tuple
from time import time

########################
#### Prep WIKI Text ####
########################

def prep_text(text):
    """ Removes unwanted parts from wikipedia's article texts.
    Args:
        :param text: to remove antwanted parts from.
    Returns
        :return: clean text.
    """
    if "\n" in text:
        output = text.split('\n')
    else:
        output = text
    new_out = []
    for sent_block in output:
        new_pice = sent_block.rstrip()
        new_pice = re.sub(r"[\n\t]*", "", str(new_pice))
        substrings = ["mini",
                      "WEITERLEITUNG"
                      "Kategorie",
                      "Sekundärliteratur",
                      "Literatur",
                      "ISBN",
                      "Band",
                      "In: Journal of",
                      "In:",
                      "(Hrsg.):",
                      "Weblinks",
                      "Schriften"]
        for phrase in substrings:
            if phrase in sent_block:
                new_pice = ""
        if not( len(new_pice.split())>10):
            new_pice = ""
        if not new_pice.endswith("."):
            new_pice = ""
        if new_pice != "" :
            new_out.append(new_pice)
    new_text = " ".join(new_out)
    return new_text

########################
#### Prep IBOX DATA ####
########################

def find_http(string):
    # findall() has been used
    # with valid conditions for urls in string
    regex = r"(?i)\b((?:https?://|www\d{0,3}[.]|[a-z0-9.\-]+[.][a-z]{2,4}/)(?:[^\s()<>]+|\(([^\s()<>]+|(\([^\s()<>]+\)))*\))+(?:\(([^\s()<>]+|(\([^\s()<>]+\)))*\)|[^\s`!()\[\]{};:'\".,<>?«»“”‘’]))"
    url = re.findall(regex, string)
    return [x[0] for x in url]


def remove_html_tags(text):
    """ Remove html tags from a string. """
    patterns = [('<.*?>(.*?)<.*?>', ''),
                ('<.*?>.*', ''),
                ('\[\[', ''),
                 ('\[', ''),
                ('\]', ''),
                ('\]\]', ''),
                ('[(]\w.*[)]', ''),
                ('&nbsp;', ' '),
                ('\|([a-zA-Z]+).*', ''),
                ('\#([a-zA-Z]+).*',''),
                ('{{.*?}}', ''),
                ('{{', ''),
                ('(^\*(.|\n)*)',''),
                ('\s$', ''),
                ('\s\n$',''),
                ('\s,$','')
                 # ('\d.+(\s)', '$1')
                ] #'\s.*'
    for patt in patterns:
        repl, mit = patt
        clean = re.compile(repl)
        text =  re.sub(clean, mit, str(text))
        links = find_http(text)
        if len(links)>0:
            text = links[0]
    return text


def wiki_infobox_extractor(page_title=None):
    """ Crawls the infoboxs values by title and returns all triples.
    Args:
        :param page_title: title to crawl.
    Returns:
        :return: dict with triples and wikibase as key.
    """
    triples = []
    wikipage_triples = {}
    wikibase = None
    ibox = None
    try:
        ibox_json = wptools.page(page_title, lang='de').get_parse()
        #ibox_json.data => shows all available data
        if ((ibox_json.data['wikibase'] != None) and (ibox_json.data['infobox'] != None)):
            wikibase = ibox_json.data["wikibase"]
            ibox = ibox_json.data["infobox"]
            triples = []
            for key, value in ibox.items():
                val = remove_html_tags(value)
                if not "_tabelle" in key:
                    pred = re.sub("_", " ", str(key))
                    pred = re.sub("-", " ", str(pred))
                    if val != "":
                        triples.append( (page_title, pred, val) )
            for triple in triples:
                if ", " in triple[2]:
                    pairs = triple[2].split(", ")
                    for value in pairs:
                        triples.append( (triple[0],triple[1], value) )
                    triples.remove(triple)
    except:
        return wikibase, triples
    return wikibase, triples

##################################################
### FINIAL PREP - Combine WIKI-TEXT and IBOX  ####
##################################################

def prep_example(data:dict): # -> dict
    """ Maps the infoxes with the corresponding article text.
    :param data: cleanted wikipedia title and text.
    :return: mapped triples (ibox values) and texts.
    """
    # {wikibase: {"triples":triples}} oder {} in case no ibox exists
    train_data = {}
    page_title = data["title"] # data[0]
    wikipage_text = data["text"] # data[1]
    wikibase = None
    triples = []
    # print(f"Processing page: {page_title}")
    wikibase, triples = wiki_infobox_extractor(page_title)
    if wikibase != None:
        train_data[wikibase] = [{"text": wikipage_text}, {"triples": triples}]
    return train_data

def prep_examples(batch:dict)->list:
    """ Maps the infoxes with the corresponding article text.
    :param json_line: cleanted wikipedia title and text.
    :return: mapped triples (ibox values) and texts.
    """
    train_exls = []
    ts = time()
    exs = len(batch)
    for json_line in batch.items():
        exs-=1
        print(exs)
        train_data = {}
        page_title = json_line[0]
        wikipage_text = json_line[1]
        wikibase = None
        triples = []
        wikibase, triples = wiki_infobox_extractor(page_title)
        if wikibase != None:
            train_data[wikibase] = [{"text": wikipage_text}, {"triples": triples}]
            train_exls.append(train_data)
    duration = (time() - ts)  / 60
    print(str(duration) + " minutes for prep_examples.")
    return train_exls