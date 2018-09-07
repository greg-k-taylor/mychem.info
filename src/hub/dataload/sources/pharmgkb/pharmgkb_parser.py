import csv
import sys
from biothings.utils.dataload import dict_sweep, unlist

csv.field_size_limit(sys.maxsize)

def load_data(tsv_file, drugbank_col=None, pubchem_col=None, chembl_col=None, chebi_col=None):
    _file = open(tsv_file)
    reader = csv.DictReader(_file,delimiter='\t')
    _dict = {}
    drug_list = []
    for row in reader:
        _id = row["PharmGKB Accession Id"]
        _d = restr_dict(row)
        _d = clean_up(_d)
        _d = unlist(dict_sweep(_d))
        _d = prefix_chebi(_d)
        _dict = {'_id':_id,'pharmgkb':_d}
        yield _dict

def restr_dict(d):
    _d = {}
    _li2 = ["External Vocabulary","Trade Names","Generic Names","Brand Mixtures","Dosing Guideline","Cross-references"]
    _li1 = ["SMILES","Name","Type","InChI"]
    for key, val in iter(d.items()):
        if key in _li1:
            _d.update({key.lower():val})
        elif key in _li2:
            val = val.split(',"')
            val = list(map(lambda each:each.strip('"'), val))  #python 3 compatible
            k = key.lower().replace(" ","_").replace('-','_').replace(".","_")
            _d.update({k:val})
        elif key == "PharmGKB Accession Id":
            k = key.lower().replace(" ","_").replace(".","_")
            _d.update({k:val})
    return _d

def clean_up(d):
    _li = ['cross_references','external_vocabulary']
    for key, val in iter(d.items()):
        if key in _li:
            _d= {}
            for ele in val:
                idx = ele.find(':')
                k = ele[0:idx].lower().replace(' ','_').replace('-','_').replace(".","_")
                v = ele[idx+1:]
                _d.update({k:v})
            d.update({key:_d})
    return d

def prefix_chebi(_d):
    if 'cross_references' in _d.keys():
        if 'chebi' in _d['cross_references'].keys():
            _d['cross_references']['chebi'] = 'CHEBI:' + _d['cross_references']['chebi']
    return _d
