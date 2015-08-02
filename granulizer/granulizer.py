import sys, requests, requests_cache, json, urllib.parse, xmltodict, csv, time
from secrets import ncbo_apikey
from xml.etree import ElementTree as ET

requests_cache.install_cache('api_cache', backend='sqlite', expire_after=604800)

# some static globals for now
ONTOLOGY = 'ICD10CM'
LEVEL = 2
VERSION = '1.0'
STOP_WORDS = ['condition', 'unspecified', 'injury', 'complaint', 'unspecified state of consciousness']

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

    # if we have succeeded, granulize
    if concepts and len(concepts) == 1 and vocabs:
        return granulize_concept(concepts[0], vocabs[0])
    else:    
        return concepts, labels, vocabs, sem_types, False

def reduce_candidates(candidates):
    """
    Attempt to reduce the number of candidate concepts to a single CUI which
    can then be granulized.
    """

    concepts = []
    labels = []
    vocabs = []
    if int(candidates['@Total']) == 1:
        # return cui, label and sources
        concepts.append(candidates['Candidate']['CandidateCUI'])
        labels.append(candidates['Candidate']['CandidatePreferred'])
        if ONTOLOGY in candidates['Candidate']['Sources']['Source']:
            vocabs.append(ONTOLOGY)
    else:
        for candidate in candidates['Candidate']:
            # is this a noisy candidate?
            if candidate['CandidatePreferred'].lower() in STOP_WORDS:
                continue
            else:
                concepts.append(candidate['CandidateCUI'])
                labels.append(candidate['CandidatePreferred'])
                if ONTOLOGY in candidate['Sources']['Source']:
                    vocabs.append(ONTOLOGY)
    return (concepts, labels, vocabs, None)
    
def granulize_concept(concept, vocab):

    # first, retrieve the @id (url) for concept
    url = "http://data.bioontology.org/search"
    params =  {'apikey': ncbo_apikey, 'include': 'cui', 'display_context': 'false', 'display_links': 'false', 'require_exact_match': 'false', 'ontologies' : vocab, 'q': concept }
    r = requests.get(url, params=params)
    search = r.json()

    # search may return multiple concepts...
    # select only the first @id, the broadest concept and remove leading and trailing quotes from string
    id = urllib.parse.quote_plus(json.dumps(search['collection'][0]['@id'])[1:-1])
    
    # use @id to retrieve anestors
    url = "http://data.bioontology.org/ontologies/{}/classes/{}/ancestors".format(vocab, id)
    params = {'apikey': ncbo_apikey, 'include': 'prefLabel,cui', 'display_context': 'false', 'display_links': 'false'}
    r = requests.get(url, params=params)
    ancestors = r.json()

    if len(ancestors) > LEVEL:
        # reverse the matches and take the @id of node at level X (0 = level 1, 1 = level 2, 2 = level 3, etc.)
        id = urllib.parse.quote_plus(json.loads(json.dumps(ancestors[::-1][LEVEL]))['@id'])
        # finally, get CUI and prefLabel for degranularized class
        url = "http://data.bioontology.org/ontologies/{}/classes/{}".format(vocab, id)
        params = {'apikey': ncbo_apikey, 'include': 'prefLabel,cui', 'display_context': 'false', 'display_links': 'false'}
        r = requests.get(url, params=params)
        simplified_icd_class = r.json()
        try:
            simplified_icd_cui = simplified_icd_class['cui'][0]
            simplified_icd_label = simplified_icd_class['prefLabel']
        except KeyError:
            print('++++++++++++++++++++++++++++++')
            print('got key error, r.text was' + r.text)
            print('++++++++++++++++++++++++++++++')

        print(' +++++ got ' + simplified_icd_cui + ' and label of ' + simplified_icd_label)
        return (simplified_icd_cui, simplified_icd_label, ONTOLOGY, None, True)
    else:
        print(' +-+-+ already at level ' + str(LEVEL) + ' so goin with ' + str(concept))
        return (concept, None, ONTOLOGY, None, True)


def main():
    with open(sys.argv[-1], 'r') as data:
        tree = ET.fromstring(data.read())

    n = 0
    for mmo in tree.iter('MMO'):
        mmo_dict = xmltodict.parse(ET.tostring(mmo))
        item, concepts, labels, vocabs, sem_types, is_granulized = process_mmo(mmo_dict)
        n += 1
        if n > 30:
            break

if __name__ == '__main__':
    main()
    exit()
