import sys, requests, requests_cache, json, urllib.parse, xmltodict, csv, time
from secrets import ncbo_apikey
from xml.etree import ElementTree as ET

requests_cache.install_cache('api_cache', backend='sqlite', expire_after=604800)

# some static globals for now
ONTOLOGY = 'ICD10CM'
LEVEL = 3
VERSION = '1.0'
STOP_WORDS = ['condition', 'unspecified', 'injury', 'complaint']

def granulized_csv_from_mmo(xmlfile):
    with open(xmlfile, 'r') as data:
        tree = ET.fromstring(data.read())

    n = 0
    for mmo in tree.iter('MMO'):
        mmo_dict = xmltodict.parse(ET.tostring(mmo))
        item, concepts, labels, vocabs, sem_types, is_granulized = process_mmo(mmo_dict)
        n += 1
        if n > 30:
            break

def process_mmo(mmo_dict):
    item_string = mmo_dict['MMO']['Utterances']['Utterance']['UttText']
    # this will break if > 1 phrase (i.e. not term-processing mode)
    mappings = dict(mmo_dict['MMO']['Utterances']['Utterance']['Phrases']['Phrase']['Mappings'])
    concepts, labels, vocabs, sem_types, is_granulized = process_mappings(mappings)
    print(item_string, concepts, labels, vocabs)
    return item_string, concepts, labels, vocabs, sem_types, is_granulized

def process_mappings(mappings):
    """
    Reduce and/or granulize mapped concepts for this utterance.
    """
    if int(mappings['@Count']) == 1:
        candidates = mappings['Mapping']['MappingCandidates']
    elif int(mappings['@Count']) > 1:
        # take the first mapping
        candidates = mappings['Mapping'][0]['MappingCandidates']
    else:
        return (None, None, None, None)

    # if more than one candidate, try to simplify
    concepts, labels, vocabs, sem_types = reduce_candidates(candidates)
    if concepts and len(concepts) == 1 and vocabs:
        return granulize_concept(concepts, vocabs)
    else:    
        return concepts, labels, vocabs, sem_types, False

def reduce_candidates(candidates):
    """
    Attempt to reduce the number of candidate concepts to a single CUI which
    can then be granulized.
    """

    concepts = labels = vocabs = []
    if int(candidates['@Total']) == 1:
        # return cui, label and sources
        concepts.append(candidates['Candidate']['CandidateCUI'])
        labels.append(candidates['Candidate']['CandidatePreferred'])
        if any(ONTOLOGY in s for s in candidates['Candidate']['Sources']['Source']):
            vocabs.append(ONTOLOGY)
    else:
        for candidate in candidates['Candidate']:
            # is this a noisy candidate?
            if any(candidate['CandidatePreferred'].lower() in w for w in STOP_WORDS):
                continue
            else:
                concepts.append(candidate['CandidateCUI'])
                labels.append(candidate['CandidatePreferred'])
                if any(ONTOLOGY in s for s in candidate['Sources']['Source']):
                    vocabs.append(ONTOLOGY)
    return (concepts, labels, vocabs, None)
    
def granulize_concept(concept, vocab):
    # None or granularized cui, label pair
    return (None, None, None, None, True)

if __name__ == '__main__':
    granulized_csv_from_mmo(sys.argv[-1])
    exit()
