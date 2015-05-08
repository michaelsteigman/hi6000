import sys
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import ParseError

i = open(sys.argv[1], 'r')
tree = ET.fromstring(i.read())
i.close()

n_total = 0
n_mapped = 0
n_singles_icdx = 0

for mmo in tree.iter('MMO'):
    n_total += 1

    #print(mmo.find('./Utterances/Utterance/UttText').text)
    candidate_cuis = []
    candidate_names = []

    #
    # taking only the first (highest scoring) mapping
    #
    
    mapping = mmo.find('./Utterances/Utterance/Phrases/Phrase/Mappings/Mapping')
    if mapping and mapping.find('./MappingCandidates').attrib.get('Total') == "1":
        candidate = mapping.find('./MappingCandidates/Candidate')
        candidate_cuis.append(candidate.find('./CandidateCUI').text)
        candidate_names.append(candidate.find('./CandidateMatched').text)
        # check to see if we've got an ICDx mapping
        sources = [source.text for source in candidate.findall('./Sources/Source')]
        if any('ICD9CM' in s for s in sources) or any('ICD10CM' in s for s in sources):
            print(mmo.find('./Utterances/Utterance/UttText').text, candidate_names, candidate_cuis, sources)
            n_singles_icdx += 1
    elif mapping:
        for candidate in mapping.findall('./MappingCandidates/Candidate'):
            candidate_cuis.append(candidate.find('./CandidateCUI').text)
            candidate_names.append(candidate.find('./CandidateMatched').text)
            

    if len(candidate_cuis) > 0:
        n_mapped += 1

print(n_total, n_mapped, n_singles_icdx)
