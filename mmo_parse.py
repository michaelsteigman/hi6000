import sys, requests, requests_cache, json, urllib.parse, csv, time
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import ParseError
from secrets import ncbo_apikey, dbname, dbuser, dbpasswd

requests_cache.install_cache('api_cache', backend='sqlite', expire_after=604800)

i = open(sys.argv[1], 'r')
tree = ET.fromstring(i.read())
i.close()

n_total = 0
n_mapped = 0
n_singles_icdx = 0
classes = {}

for mmo in tree.iter('MMO'):
    n_total += 1

    candidate_uis = []
    candidate_labels = []

    #
    # taking only the first phrase's mapping for now
    #

    item_string = mmo.find('./Utterances/Utterance/UttText').text
    print('\n')
    print('=======================================================')
    print(item_string)
    print('-------------------------------------------------------')
    print(mmo.find('./Utterances/Utterance/Phrases').attrib.get('Count') + ' phrase(s), using ' + mmo.find('./Utterances/Utterance/Phrases/Phrase/PhraseText').text)
    print('-------------------------------------------------------')
    mapping = mmo.find('./Utterances/Utterance/Phrases/Phrase/Mappings/Mapping')
    if mapping and mapping.find('./MappingCandidates').attrib.get('Total') == "1":
        candidate = mapping.find('./MappingCandidates/Candidate')
        candidate_ui = candidate.find('./CandidateCUI').text
        candidate_label = candidate.find('./CandidateMatched').text
        candidate_uis.append(candidate_ui)
        candidate_labels.append(candidate_label)
        
        # check to see if we've got an ICDx mapping
        sources = [source.text for source in candidate.findall('./Sources/Source')]
        sem_types = [sem_type.text for sem_type in candidate.findall('./SemTypes/SemType')]
        print(candidate_labels, candidate_uis, sources, sem_types)
        
        ontology = None
        if any('ICD9CM' in s for s in sources):
            ontology = 'ICD9CM'
        elif any('ICD10CM' in s for s in sources):
            ontology = 'ICD10CM'
            
        #if ontology and not any('sosy' in t for t in sem_types):
        if ontology:
            ## get @id for cui via search
            url = "http://data.bioontology.org/search"
            params =  {'apikey': ncbo_apikey, 'include': 'cui', 'display_context': 'false', 'display_links': 'false', 'require_exact_match': 'false', 'ontologies' : ontology, 'q': candidate_ui }
            r = requests.get(url, params=params)
            search = json.loads(r.text)
            # select only the first @id, the broadest concept and remove leading and trailing quotes from string
            id = urllib.parse.quote_plus(json.dumps(search['collection'][0]['@id'])[1:-1])
        
            ## now grab anestors
            url = "http://data.bioontology.org/ontologies/{}/classes/{}/ancestors".format(ontology, id)
            params = {'apikey': ncbo_apikey, 'include': 'prefLabel,cui', 'display_context': 'false', 'display_links': 'false'}
            r = requests.get(url, params=params)
            ancestors = json.loads(r.text)

            if len(ancestors) > 2:
                # reverse the matches and take the @id of node at level X (0 = 1, 1 = 2, 2 = 3, etc.)
                id = urllib.parse.quote_plus(json.loads(json.dumps(ancestors[::-1][2]))['@id'])
                # finally, get CUI and prefLabel for degranularized class
                url = "http://data.bioontology.org/ontologies/{}/classes/{}".format(ontology, id)
                params = {'apikey': ncbo_apikey, 'include': 'prefLabel,cui', 'display_context': 'false', 'display_links': 'false'}
                r = requests.get(url, params=params)
                simplified_icd_class = json.loads(r.text)
                try:
                    simplified_icd_cui = simplified_icd_class['cui'][0]
                    simplified_icd_label = simplified_icd_class['prefLabel']
                except KeyError:
                    print('++++++++++++++++++++++++++++++')
                    print('got key error, r.text was' + r.text)
                    print('++++++++++++++++++++++++++++++')
                    continue
                print(' +++++ got ' + simplified_icd_cui + ' and label of ' + simplified_icd_label)
                classes[n_total] = {'item_string': item_string, 'item_type': 'problem', 'ui': simplified_icd_cui, 'label': simplified_icd_label, 'vocabs': ontology, 'granularityreduced': 't', 'version': '1' }
            else:
                print(' +-+-+ already at level 3 so goin with ' + candidate_ui + ' and ' + candidate_label)
                classes[n_total] = {'item_string': item_string, 'item_type': 'problem', 'ui': candidate_ui, 'label': candidate_label, 'vocabs': ontology, 'granularityreduced': 'f', 'version': '1' }

            n_singles_icdx += 1
    elif mapping:
        for candidate in mapping.findall('./MappingCandidates/Candidate'):
            candidate_uis.append(candidate.find('./CandidateCUI').text)
            candidate_labels.append(candidate.find('./CandidateMatched').text)
            classes[n_total] = {'item_string': item_string, 'item_type': 'problem', 'ui': ', '.join(candidate_uis), 'label': ', '.join(candidate_labels), 'vocabs': ', '.join(sources), 'granularityreduced': 'f', 'version': '1' }
        print(' ----- got ' + str(candidate_labels) + str(candidate_uis))

    if len(candidate_uis) > 0:
        n_mapped += 1
    time.sleep(1.5)
    

with open('classes.csv', 'w') as csvfile:
    fieldnames = list(classes[1].keys())
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=',')
    writer.writeheader()
    writer.writerows(classes.values())
        
print(n_total, n_mapped, n_singles_icdx)
