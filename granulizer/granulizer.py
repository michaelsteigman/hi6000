import sys, requests, requests_cache, json, urllib.parse, xmltodict, csv, time, array, re
from secrets import ncbo_apikey
from xml.etree import ElementTree as ET

requests_cache.install_cache('api_cache', backend='sqlite', expire_after=604800)

# some static globals for now
ONTOLOGY = 'ICD10CM'
LEVEL = 2
VERSION = '1.0'
STOP_WORDS = ['condition', 'unspecified', 'injury', 'complaint', 'unspecified state of consciousness']

def mmo_mapping(mmo_dict):

    try:
        utterance = mmo_dict['MMO']['Utterances']['Utterance']['UttText']
        mappings = mmo_dict['MMO']['Utterances']['Utterance']['Phrases']['Phrase']['Mappings']
    except TypeError:
        # huh? term processing mode still does sentence tokenizing?? guess so. for the few cases with periods..
        utterance = mmo_dict['MMO']['Utterances']['Utterance'][0]['UttText']
        mappings = mmo_dict['MMO']['Utterances']['Utterance'][0]['Phrases']['Phrase']['Mappings']

    yield from mapping_candidates(utterance, mappings)
    #concepts, labels, vocabs, sem_types, is_granulized = process_mappings(mappings)
    #print(item_string, concepts, labels, vocabs, sem_types)
    #return item_string, concepts, labels, vocabs, sem_types, is_granulized

def mapping_candidates(utterance, mappings):
    """
    Pull out the candidates appropriately
    """
    if int(mappings['@Count']) == 1:
        candidates = mappings['Mapping']['MappingCandidates']
    elif int(mappings['@Count']) > 1:
        # take the first (highest scored) mapping
        candidates = mappings['Mapping'][0]['MappingCandidates']
    else:
        return []

    yield from unpack_candidates(utterance, candidates)


def unpack_candidates(utterance, candidates):
    """
    Pull out the elements of the MMO and return granulized concept if possible, else return
    all the candidates elements
    """

    if int(candidates['@Total']) == 1:
        # return cui, label and sources
        concept = candidates['Candidate']['CandidateCUI']
        label = candidates['Candidate']['CandidatePreferred']
        vocabs = normalize_list(candidates['Candidate']['Sources']['Source'])
        sem_types = normalize_list(candidates['Candidate']['SemTypes']['SemType'])
        yield granulize_concept(utterance, concept, label, vocabs, sem_types)
    else:
        concepts = []
        for candidate in candidates['Candidate']:
            # (TODO - use functional approach here, passing candidate to
            # function or multiple functions that attempt to reduce number
            # of candidates)
            # is this a noisy candidate?
            # if candidate['CandidatePreferred'].lower() in STOP_WORDS:
            #     print('&' * 50, candidate['CandidatePreferred'])
            #     continue
            concept = candidate['CandidateCUI']
            label = candidate['CandidatePreferred']
            vocabs = normalize_list(candidate['Sources']['Source'])
            sem_types = normalize_list(candidate['SemTypes']['SemType'])
            # there are cases where MMO contains multiple mappings to same concept (?)
            if concept not in concepts:
                concepts.append(concept)
                yield utterance, concept, label, vocabs, sem_types, 'multiple'

def normalize_list(elements):
    if isinstance(elements,str):
        return [elements]
    else:
        return elements
    
def granulize_concept(utterance, concept, label, vocabs, sem_types):

    if ONTOLOGY not in vocabs:
        return utterance, concept, label, vocabs, sem_types, 'as_is'
    else:
        # first, retrieve the @id (url) for concept
        url = "http://data.bioontology.org/search"
        params =  {'apikey': ncbo_apikey, 'include': 'cui', 'display_context': 'false', 'display_links': 'false', 'require_exact_match': 'false', 'ontologies' : ONTOLOGY, 'q': concept }
        r = requests.get(url, params=params)
        results = r.json()

        # search may return multiple concepts...
        # select only the first @id, the broadest concept and remove leading and trailing quotes from string
        id = urllib.parse.quote_plus(json.dumps(results['collection'][0]['@id'])[1:-1])
    
        # use @id to retrieve anestors
        url = "http://data.bioontology.org/ontologies/{}/classes/{}/ancestors".format(ONTOLOGY, id)
        params = {'apikey': ncbo_apikey, 'include': 'prefLabel,cui', 'display_context': 'false', 'display_links': 'false'}
        r = requests.get(url, params=params)
        ancestors = r.json()

        if len(ancestors) > LEVEL:
            # reverse the matches and take the @id of node at level X (0 = level 1, 1 = level 2, 2 = level 3, etc.)
            id = urllib.parse.quote_plus(json.loads(json.dumps(ancestors[::-1][LEVEL]))['@id'])
            # finally, get CUI and prefLabel for granulized class
            url = "http://data.bioontology.org/ontologies/{}/classes/{}".format(ONTOLOGY, id)
            params = {'apikey': ncbo_apikey, 'include': 'prefLabel,cui', 'display_context': 'false', 'display_links': 'false'}
            r = requests.get(url, params=params)
            simplified_icd_class = r.json()
            try:
                simplified_icd_cui = simplified_icd_class['cui'][0]
                simplified_icd_label = simplified_icd_class['prefLabel']
            except KeyError:
                print('KeyError: r.text was' + r.text)
            #print(' +++++ got ' + simplified_icd_cui + ' and label of ' + simplified_icd_label)
            return utterance, simplified_icd_cui, simplified_icd_label, [ONTOLOGY], [], 'granulized'
        else:
            #print(' +-+-+ already at level ' + str(LEVEL) + ' so returning ' + str(concept) + ' - ' + str(label))
            return utterance, concept, label, vocabs, sem_types, 'as_is'

def pack_array(list):
    a = str(list).replace('[', '{')
    a = a.replace(']', '}')
    return a.replace('\'', '')

def main():
    datafile = sys.argv[-1]
    with open(datafile, 'r') as data:
        tree = ET.fromstring(data.read())

    n = 1
    o = 1
    p = 1
    q = 1
    mappings = {}
    for mmo in tree.iter('MMO'):
        mmo_dict = xmltodict.parse(ET.tostring(mmo))
        for utterance, concept, label, vocabs, sem_types, status in mmo_mapping(mmo_dict):
            mappings[n] = { 'item_string': utterance, 'concept': concept, 'label': label, 'vocabs': pack_array(vocabs), 'sem_types': pack_array(sem_types), 'version': VERSION, 'status': status}
            n += 1
            if status == 'granulized':
                o += 1
            elif status == 'as_is':
                p += 1
            else:
                q += 1

    outfile = re.match('[A-Za-z0-9-_]*',datafile).group(0) + '_mappings.csv'
    with open(outfile, 'w') as csvfile:
        fieldnames = list(mappings[1].keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
        writer.writeheader()
        writer.writerows(mappings.values())

    print('=' * 40)
    print('wrote output to ' + outfile)
    print('total = ', n, 'granulized = ', o, 'as is = ', p, 'multiple = ', q)

if __name__ == '__main__':
    main()
    exit()
