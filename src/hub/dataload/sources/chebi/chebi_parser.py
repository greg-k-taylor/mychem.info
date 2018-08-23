import pickle
from zlib import compress
from biothings.utils.dataload import dict_sweep, unlist, value_convert_to_number


def load_data(sdf_file, drugbank_col=None, chembl_col=None):
    import biothings.utils.mongo as mongo
    f = open(sdf_file,'r').read()
    comp_list = f.split("$$$$") #split the compounds and list
    comp_list = [ele.split("\n> <") for ele in comp_list] #split from \n> <
    comp_list = list(map(lambda x:[ele.strip("\n") for ele in x],comp_list))
    comp_list = list(map(lambda x: [ele.split('>\n',1) for ele in x],comp_list))
    for item in comp_list:
        del item[0] #remove molecule structure - Marvin
    for element in comp_list:
        element = map(lambda x: [ele.split('\n') for ele in x],element)
    comp_list = list(map(lambda x: dict([ ele for ele in x]),comp_list)) #python 3 compatible
    del comp_list[-1]
    for compound in comp_list:
        restr_dict = restructure_dict(compound)
        restr_dict["_id"] = find_inchikey(restr_dict,drugbank_col,chembl_col)
        restr_dict = truncate(restr_dict)
        yield restr_dict

def clean_up(_dict):
    _temp = dict()
    for key, value in iter(_dict.items()):
        key = key.lower().replace(' ','_').replace('-','_')
        value = value.split('\n')
        if key == "definition":
            value[0] = value[0].replace('<stereo>','').replace('<ital>','')
            value[0] = value[0].replace('</stereo>','').replace('</ital>','')
        # restructure the pubchem_database_links field
        if key == 'pubchem_database_links':
            new_pubchem_dict = {}
            if type(value) == list:
                for _value in value:
                    splitted_results = _value.split(':')
                    if len(splitted_results) == 2:
                        new_pubchem_dict[splitted_results[0]] = splitted_results[1][1:]
            value = new_pubchem_dict
        _temp[key] = value
    return _temp

def restructure_dict(dictionary):
    restr_dict = dict()
    restr_dict['_id'] = dictionary['ChEBI ID']
    restr_dict['chebi']= dictionary
    restr_dict['chebi'] = clean_up(restr_dict['chebi'])
    restr_dict = dict_sweep(restr_dict,vals=[None,".", "-", "", "NA", "none", " ", "Not Available",
        "unknown","null","None","NaN"])
    restr_dict = value_convert_to_number(unlist(restr_dict),skipped_keys=["beilstein_registry_numbers","pubmed_citation_links","sabio_rk_database_links","gmelin_registry_numbers","molbase_database_links"])
    return restr_dict

def find_inchikey(doc, drugbank_col, chembl_col):
    _id = doc["_id"] # default if we can't find anything

    if 'inchikey' in doc["chebi"]:
        _id = doc["chebi"]['inchikey']

    return _id


def truncate(d):
    max_values = 1000

    fields = [
        'intez_database_links',
        'reactome_database_links',
        'rhea_database_links',
        'sabio_rk_database_links',
        'uniprot_database_links',
        ]
    
    def truncate_field(field_name, field):
        if isinstance(field, list) and len(field) >= max_values:
            compressed = compress(pickle.dumps(field))
            res = {
                '_truncated': {
                    '_readme': "the following fields are truncated for top ten {}".format(field_name),
                    'total': len(field),
                    'kept': len(field[:max_values]),
                    '_raw': compressed,
                    field_name: field[:max_values],
                    }
                }
            return res
        else:
            return field

    for field in fields:
        if 'chebi' in d.keys():
            if field in d['chebi'].keys():
                d['chebi'][field] = truncate_field(field, d['chebi'][field])

    return d
