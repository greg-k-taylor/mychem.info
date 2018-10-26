import csv
import re
import sys
from biothings.utils.dataload import dict_sweep, unlist

csv.field_size_limit(sys.maxsize)

def load_data(tsv_file):
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
    def _restr_xref(xref):
        """Restructure field names related to the pharmgkb.xref field"""
        # Rename fields
        rename_fields = [
            ('National Drug Code Directory', 'ndc'),
            ('Drugs Product Database (DPD)', 'dpd'),
            ('FDA Drug Label at DailyMed', 'dailymed.setid'),
            ]
        res = []
        for v in xref:
            for rf_orig, rf_new in rename_fields:
                if rf_orig in v:
                    v = v.replace(rf_orig, rf_new)
            # Multiple replacements on the 'Web Resource' field
            if 'Web Resource' in v:
                if 'http://en.wikipedia.org/wiki/' in v:
                    v = v.replace('Web Resource', 'wikipedia.url_stub')
                    v = v.replace('http://en.wikipedia.org/wiki/', '')
            # Add 'CHEBI:' prefix if not there already
            elif 'ChEBI:' in v:
                if 'ChEBI:CHEBI' not in v:
                    v = v.replace('ChEBI:', 'ChEBI:CHEBI:')
            res.append(v)
        return res
    _d = {}
    _li2 = ["Trade Names","Generic Names","Brand Mixtures","Dosing Guideline"]
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
            k = 'id'
            _d.update({k:val})
        elif key == "Cross-references":
            k = "xref"
            val = val.split(',"')
            val = list(map(lambda each:each.strip('"'), val))  #python 3 compatible
            val = _restr_xref(val)
            _d.update({k:val})
        elif key == "External Vocabulary":
            # external_vocabulary - remove parenthesis and text within
            k = "external_vocabulary"
            # note:  regular expressions appear to be causing an error
            # val = re.sub('\([^)]*\)', '', val)
            val = val.split(',"')
            val = list(map(lambda each:remove_paren(each.strip('"')), val))  #python 3 compatible
            _d.update({k:val})
    return _d

def clean_up(d):
    _li = ['xref','external_vocabulary']
    _d= {}
    for key, val in iter(d.items()):
        if key in _li:
            for ele in val:
                idx = ele.find(':')
                # Note:  original pharmgkb keys do not have '.'
                k = transform_xref_fieldnames(ele[0:idx])
                v = ele[idx+1:]
                if k in ["pubchem.cid","pubchem.sid"]:
                    v = int(v)
                # Handle nested elements (ex: 'wikipedia.url_stub') here
                sub_d = sub_field(k, v)
                _d.update(sub_d)
    # 'xref' and 'external_vocabulary' are merged
    if 'external_vocabulary' in d.keys():
        d.pop('external_vocabulary')
    d.update({'xref':_d})
    return d

def prefix_chebi(_d):
    if 'cross_references' in _d.keys():
        if 'chebi' in _d['cross_references'].keys():
            _d['cross_references']['chebi'] = 'CHEBI:' + _d['cross_references']['chebi']
    return _d

def sub_field(k, v):
    """Return a nested dictionary with field keys k and value v."""
    res = {}
    field_d = res
    fields = k.split('.')
    for f in fields[:-1]:
        field_d[f] = {}
        field_d = field_d[f]
    field_d[fields[-1]] = v
    return res

def remove_paren(v):
    """remove first occurance of trailing parentheses from a string"""
    idx = v.find('(')
    if idx != -1:
        return v[0:idx]
    return v

def transform_xref_fieldnames(k):
    fields = [
        ('Chemical Abstracts Service', 'cas'),
        ('Therapeutic Targets Database', 'ttd'),
        ('PubChem Substance', 'pubchem.sid'),
        ('PubChem Compound', 'pubchem.cid')
        ]
    for orig_f, new_f in fields:
        if orig_f in k:
            k = k.replace(orig_f, new_f)
            break
    k = k.lower().replace(' ','_').replace('-','_')
    return k
