import xmltodict
import json, math
import collections
import logging
from biothings.utils.dataload import dict_sweep, unlist, value_convert_to_number
from biothings.utils.dataload import boolean_convert


def load_data(xml_file):
    drug_list = []
    def handle(path,item):  #streaming mode of xmltodict
        doc = restructure_dict(item)
        # try to normalize to inchi key
        try:
            _id = doc["drugbank"]['inchi_key']
            doc["_id"] = _id
        except KeyError:
            pass
        drug_list.append(doc)
        return True
    with open(xml_file,'rb') as f:
        xmltodict.parse(f,item_depth=2,item_callback=handle,xml_attribs=True)
    for doc in drug_list:
        yield doc

def restr_protein_dict(dictionary):
    _li1 = ['id', 'name', 'organism']
    _dict = {}
    for x,y in iter(dictionary.items()):
        if x in _li1:
            _dict.update({x:y})
        elif x == 'actions' and y:
            for z in y:
                _dict.update({x:y[z]})
        elif x == 'known-action':
            x = x.replace('-','_')
            _dict.update({x:dictionary['known-action']})
        elif x == 'polypeptide':
            _li2 = ['general-function','specific-function']
            for i in y:
                if i ==  "@id":
                    _dict.update({'uniprot':y[i]})
                elif i == "@source":
                    _dict.update({'source':y[i]})
                elif i in _li2:
                    j = i.replace('-','_')
                    _dict.update({j:y[i]})
    return _dict

def restructure_dict(dictionary):
    restr_dict = dict()
    d1 = dict()
    pred_properties_dict = {}
    products_list = []
    categories_list = []
    enzymes_list = []
    targets_list = []
    carriers_list = []
    transporters_list = []
    atccode_list = []

    for key,value in iter(dictionary.items()):
        if key == 'name' and value:
            d1[key] = value

        elif key == 'drugbank-id' and value:
            id_list = []
            if isinstance(value,list):
                for ele in value:
                    if isinstance(ele,collections.OrderedDict):
                        assert "@primary" in ele
                        for x,y in iter(ele.items()):
                            if x == '#text':
                                # make sure we always have DB ID as drugbank_id
                                d1.update({'drugbank_id' : y})
                                restr_dict['_id'] = y

                    if isinstance(ele,str):
                        key = key.replace('-','_')
                        id_list.append(ele)
                        d1.update({'accession_number':id_list})

            elif isinstance(value,dict) or isinstance(value,collections.OrderedDict):
                for x,y in iter(value.items()):
                    if x == '#text':
                        key = key.replace('-','_')
                        id_list.append(y)
                        d1.update({key:id_list})
                        restr_dict['_id'] = y


        elif key == 'description':
            d1.update({'pharmacology':{key:value}})

        elif key == 'groups':
            for i,j in iter(value.items()):
                d1[key] = j

        elif key == 'indication':
            d1['pharmacology'].update({key:value})

        elif key == 'pharmacodynamics':
            d1['pharmacology'].update({key:value})

        elif key == 'mechanism-of-action':
            key = key.replace('-','_')
            d1['pharmacology'].update({key:value})

        elif key == 'toxicity':
            d1['pharmacology'].update({key:value})

        elif key == 'metabolism':
            d1['pharmacology'].update({key:value})

        elif key == 'absorption':
            d1['pharmacology'].update({key:value})

        elif key == 'half-life':
            key = key.replace('-','_')
            d1['pharmacology'].update({key:value})

        elif key == 'protein-binding':
            key = key.replace('-','_')
            d1['pharmacology'].update({key:value})

        elif key == 'route-of-elimination':
            key = key.replace('-','_')
            d1['pharmacology'].update({key:value})

        elif key == 'volume-of-distribution':
            key = key.replace('-','_')
            d1['pharmacology'].update({key:value})

        elif key == 'clearance':
            d1['pharmacology'].update({key:value})

        elif key == 'classification' and value:
            for m,n in iter(value.items()):
                m = m.lower().replace('-','_')
                d1.update({'taxonomy':value})

        elif key == 'salts'and value:
            salts_list = []

            for m,n in iter(value.items()):
                if isinstance(n,list):
                    for ele in n:
                        for k in ele:
                            if k == 'name':
                                salts_list.append(ele[k])
                                d1.update({key:salts_list})

                elif isinstance(n,dict) or isinstance(n,collections.OrderedDict):
                    d1.update({key:n['name']})

        elif key == 'synonyms' and value:
            synonym_list = []
            if isinstance(value,collections.OrderedDict):
                for x,y in iter(value.items()):
                    for ele in y:
                        for name in ele:
                            if name == '#text':
                                synonym_list.append(ele[name])
                                d1.update({key:synonym_list})

        elif key == 'products'and value:
            def restr_product_dict(dictionary):
                products_dict = {}
                for x in dictionary:
                    if x == 'name':
                        products_dict[x] = dictionary[x]
                    elif x == 'dosage-form':
                        products_dict['dosage_form'] = dictionary[x]
                    elif x == 'strength':
                        products_dict[x] = dictionary[x]
                    elif x == 'route':
                        products_dict[x] = dictionary[x]
                    elif x == 'over-the-counter':
                        products_dict['otc'] = dictionary[x]
                    elif x == 'generic':
                        products_dict[x] = dictionary[x]
                    elif x == 'ndc-id':
                        products_dict['ndc_id'] = dictionary[x]
                    elif x == 'ndc-product-code':
                        products_dict['ndc_product_code'] = dictionary[x]
                    elif x == 'dpd-id':
                        products_dict['dpd'] = dictionary[x]
                    elif x == 'started-marketing-on':
                        products_dict[x.replace('-','_')] = dictionary[x]
                    elif x == 'ended-marketing-on':
                        products_dict[x.replace('-','_')] = dictionary[x]
                    elif x == 'fda-application-number':
                        products_dict[x.replace('-','_')] = dictionary[x]
                    elif x == 'approved':
                        products_dict[x] = dictionary[x]
                    elif x == 'country':
                        products_dict[x] = dictionary[x]
                    elif x == 'source':
                        products_dict[x] = dictionary[x]
                return products_dict

            for x,y in iter(value.items()):
                if isinstance(y,dict) or isinstance(y,collections.OrderedDict):
                    _d = restr_product_dict(y)
                    products_list.append(_d)

                elif isinstance(y,list):
                    for _d in y:
                        products_list.append(restr_product_dict(_d))

        elif key == 'packagers' and value:
            pack_list = []
            for pack in value:
                for pack1 in value[pack]:
                    for s in pack1:
                        if s == 'name' and pack1[s]:
                            pack_list.append(pack1[s])
                            d1.update({key:pack_list})

        elif key == 'manufacturers' and value:
            manuf_list = []
            for x,y in iter(value.items()):
                if isinstance(y,dict) or isinstance(y,collections.OrderedDict):
                    for i in y:
                        if i == '#text':
                            manuf_list.append(y[i])
                            d1.update({key:manuf_list})

                if isinstance(y,list):
                    for i in y:
                        for m,n in iter(i.items()):
                            if m == '#text':
                                manuf_list.append(n)
                                d1.update({key:manuf_list})

        elif key == 'categories' and value:
            for x,y in iter(value.items()):
                d1.update({key:y})

        elif key == "snp-effects" and value:
            key = key.replace('-','_')
            d1['pharmacology'].update({key:value})
        elif key == "snp-adverse-drug-reactions" and value:
            key = key.replace('-','_')
            d1['pharmacology'].update({key:value})

        elif key == 'affected-organisms' and value:
            for x,y in iter(value.items()):
                key = key.replace('-','_')
                d1['pharmacology'].update({key:value["affected-organism"]})

        elif key == 'ahfs-codes' and value:
            for x in value:
                key = key.replace('-','_')
                d1.update({key:value[x]})

        elif key == 'food-interactions' and value:
            food_interaction_list = []
            for x,y in iter(value.items()):
                if isinstance(y,list):
                    key = key.replace('-','_')
                    for i in y:
                        food_interaction_list.append(i)
                        d1.update({key:food_interaction_list})
                else:
                    d1.update({key:y})

        elif key == 'drug-interactions' and value:
            key = key.replace('-','_')
            for x,y in iter(value.items()):
                d1.update({key:y})

        elif key == 'sequences'and value:
            for x,y in iter(value.items()):
                for i in y:
                    if i == '@format':
                        str1 = y[i]+'_sequences'
                        d1[str1] = y['#text'].replace('\n',' ')

        elif key == 'experimental-properties' and value:
            d1_exp_properties = {}
            def restr_properties_dict(dictionary):
                for x,y in iter(dictionary.items()):
                    k1 = dictionary['kind']
                    k1 = k1.lower().replace(' ','_').replace('-','_')
                    if k1 == "isoelectric_point":
                        # make sure value are floats, if intervals, then list(float)
                        try:
                            d1_exp_properties[k1] = float(dictionary['value'])
                        except ValueError:
                            # not a float, maybe a range ? "5.6 - 7.6"
                            vals = dictionary['value'].split("-")
                            try:
                                for i,val in enumerate([v for v in vals]):
                                    vals[i] = float(val)
                                logging.info("Document ID '%s' has a range " % restr_dict["_id"] + \
                                             "as isoelectric_point: %s" % vals)
                                d1_exp_properties[k1] = vals
                            except ValueError as e:
                                # not something we can handle, skip it
                                logging.warning("Document ID '%s' has non-convertible " % restr_dict["_id"] + \
                                                " value for isoelectric_point, field ignored: %s" % dictionary['value'])
                                continue
                    else:
                        d1_exp_properties[k1] = dictionary['value']
                return d1_exp_properties

            for ele in value:
                key = key.replace('-','_')
                if isinstance(value[ele],list):
                    for _d in value[ele]:
                        _d = restr_properties_dict(_d)
                        d1.update({key:_d})

                if isinstance(value[ele],dict) or isinstance(value[ele],collections.OrderedDict):
                    _d = restr_properties_dict(value[ele])
                    d1.update({key:_d})

        elif key == 'calculated-properties' and value:
            def restr_properties_dict(dictionary):
                for x in dictionary:
                    k = dictionary['kind']
                    k = k.lower().replace(' ','_').replace('-','_')
                    pred_properties_dict[k] = dictionary['value']

                    if dictionary['kind'] == "IUPAC Name":
                        d1.update({'iupac':dictionary['value']})
                    elif dictionary['kind'] == "SMILES":
                        d1.update({'smiles':dictionary['value']})
                    elif dictionary['kind'] == "Molecular Formula":
                        d1.update({'formula':dictionary['value']})
                    elif dictionary['kind'] == "InChI":
                        d1.update({'inchi':dictionary['value']})
                    elif dictionary['kind'] == "InChIKey":
                        if dictionary['value'][0:9] == 'InChIKey=':
                            d1.update({'inchi_key':dictionary['value'][9:]})
                        else:
                            d1.update({'inchi_key':dictionary['value']})
                    elif dictionary['kind'] == "Molecular Weight":
                        d1.update({'weight':{'average':dictionary['value']}})
                    elif dictionary['kind'] == "Monoisotopic Weight":
                        d1['weight'].update({'monoisotopic':dictionary['value']})

            for x,y in iter(value.items()):
                if isinstance(y,list):
                    for _d in y:
                        _d = restr_properties_dict(_d)

                if isinstance(y,dict) or isinstance(y,collections.OrderedDict):
                    _d = restr_properties_dict(y)

        elif key == 'external-identifiers' and value:
            for ele in value['external-identifier']:
                for x in ele:
                    if x == 'resource':
                        if ele[x] == "Drugs Product Database (DPD)":
                            d1['dpd'] = ele['identifier']
                        elif ele[x] == "KEGG Drug":
                            d1['kegg_drug'] = ele['identifier']
                        elif ele[x] == "KEGG Compound":
                            d1['kegg_compound'] = ele['identifier']
                        elif ele[x] == "National Drug Code Directory":
                            d1['ndc_directory'] = ele['identifier']
                        elif ele[x] == "PharmGKB":
                            d1['pharmgkb'] = ele['identifier']
                        elif ele[x] == "UniProtKB":
                            d1['uniprotkb'] = ele['identifier']
                        elif ele[x] == "Wikipedia":
                                d1['wikipedia'] = ele['identifier']
                        elif ele[x] == "ChemSpider":
                                d1['chemspider'] = ele['identifier']
                        elif ele[x] == "ChEBI":
                                d1['chebi'] = 'CHEBI:' + str(ele['identifier'])
                        elif ele[x] == "PubChem Compound":
                                d1['pubchem_compound'] = ele['identifier']
                        elif ele[x] == "PubChem Substance":
                                d1['pubchem_substance'] = ele['identifier']
                        elif ele[x] == "UniProtKB":
                                d1['uniprotkb'] = ele['identifier']
                        elif ele[x] == "GenBank":
                                d1['genbank'] = ele['identifier']
                        else:
                            source = ele[x].lower().replace('-','_').replace(' ','_')
                            d1[source]=ele['identifier']

        elif key == 'external-links' and value:
            if isinstance(value['external-link'],list):
                for ele in value['external-link']:
                    for x in ele:
                        try:
                            resource = ele['resource']
                            d1[resource.lower().replace('.','_')] = ele['url']
                        except:
                            pass
            else:
                try:
                    resource = ele['resource']
                    d1[resource.lower().replace('.','_')] = ele['url']
                except:
                    pass



        elif key == 'patents'and value:
            if isinstance(value,dict):
                for x in value:
                    d1.update({key:value[x]})

        elif key == 'international-brands' and value:
            key = key.lower().replace('-','_')
            d1.update({key:value['international-brand']})

        elif key == 'mixtures' and value:
            d1.update({key:value['mixture']})

        elif key == 'pathways' and value:
            _li = []
            def restr_pathway_dict(dictionary):
                _dict = {}
                for x,y in iter(dictionary.items()):
                    if x == 'smpdb-id':
                        _dict.update({'smpdb_id':y})
                    elif x == 'name':
                        _dict.update({x:y})
                    elif x == 'drugs':
                        _dict.update({x:y['drug']})
                    elif x == 'enzymes':
                        _dict.update({x:y})
                return _dict

            if isinstance(value['pathway'],list):
                for ele in value['pathway']:
                    _dict = restr_pathway_dict(ele)
                    _li.append(_dict)
                    d1.update({key:_li})

            elif isinstance(value['pathway'],dict) or isinstance(value['pathway'],OrderedDict):
                _dict = restr_pathway_dict(value['pathway'])
                d1.update({key:_dict})

        elif key == 'targets' and value:
            if isinstance(value['target'],list):
                for dictionary in value['target']:
                    _dict = restr_protein_dict(dictionary)
                    targets_list.append(_dict)

            elif isinstance(value['target'],dict) or isinstance(value['target'],OrderedDict):
                _dict = restr_protein_dict(value['target'])
                targets_list.append(_dict)

        elif key == 'enzymes' and value:
            if isinstance(value['enzyme'],list):
                for dictionary in value['enzyme']:
                    _dict = restr_protein_dict(dictionary)
                    enzymes_list.append(_dict)

            elif isinstance(value['enzyme'],dict) or isinstance(value['enzyme'],OrderedDict):
                _dict = restr_protein_dict(value['enzyme'])
                enzymes_list.append(_dict)

        elif key == 'transporters' and value:
            if isinstance(value['transporter'],list):
                for dictionary in value['transporter']:
                    _dict = restr_protein_dict(dictionary)
                    transporters_list.append(_dict)

            elif isinstance(value['transporter'],dict) or isinstance(value['transporter'],OrderedDict):
                _dict = restr_protein_dict(value['transporter'])
                transporters_list.append(_dict)

        elif key == 'carriers' and value:
            if isinstance(value['carrier'],list):
                for dictionary in value['carrier']:
                    _dict = restr_protein_dict(dictionary)
                    carriers_list.append(_dict)

            elif isinstance(value['carrier'],dict) or isinstance(value['carrier'],OrderedDict):
                _dict = restr_protein_dict(value['carrier'])
                carriers_list.append(_dict)

        elif key == 'atc-codes' and value:
            def restr_atccode_dict(dictionary):
                for x in dictionary:
                    if x == '@code':
                        atccode_list.append(dictionary[x])
                return atccode_list

            if isinstance(value['atc-code'], list):
                for _d in value['atc-code']:
                    restr_atccode_dict(_d)

            elif isinstance(value['atc-code'], dict) or isinstance(value['atc-code'], OrderedDict):
                restr_atccode_dict(value['atc-code'])


    d1['atc_codes'] = atccode_list
    d1['targets'] = targets_list
    d1['carriers'] = carriers_list
    d1['enzymes'] = enzymes_list
    d1['transporters'] = transporters_list
    d1['predicted_properties'] = pred_properties_dict
    d1['products'] = products_list
    restr_dict['drugbank'] = d1
    restr_dict = unlist(restr_dict)
    restr_dict = dict_sweep(restr_dict,vals=[None,math.inf,"INF",".", "-", "", "NA", "none", " ",
        "Not Available", "unknown","null","None"])
    restr_dict = boolean_convert(restr_dict,["predicted_properties.mddr_like_rule",
        "predicted_properties.bioavailability","predicted_properties.ghose_filter",
        "predicted_properties.rule_of_five","products.generic","products.otc",
        "products.approved","products.pediatric-extension"])
    restr_dict = value_convert_to_number(restr_dict,
            skipped_keys=["dpd","chemspider","chebi","pubchem_compound","pubchem_substance","bindingdb",
                          "pka","boiling_point","melting_point","water_solubility","number","half_life",
                          "pdb","name"])
    return restr_dict

