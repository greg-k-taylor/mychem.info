import xmltodict
import re
import gzip
import os
from biothings.utils.dataload import value_convert_to_number

def load_data(input_file):
    compound_list = []
    def handle(path,item):
        item = restructure_dict(item)
        compound_list.append(item)
        return True

    _f = gzip.open(input_file,'rb').read()
    xmltodict.parse(_f,item_depth=2,item_callback=handle,xml_attribs=True)  #parse the xml file to dictionary
    for compound in compound_list:
        try:
            _id = compound["pubchem"]['inchi_key']
            compound["_id"] = _id
        except KeyError:
            pass

        yield compound

def restructure_dict(dictionary):
    smile_dict = dict()
    iupac_dict = dict()
    d = dict()

    for key,value in iter(dictionary.items()):
        if key == "PC-Compound_id":
            for cnt in value:
                for m,n in iter(value[cnt].items()):
                    for x,y in iter(n.items()):
                        d["cid"] = y

        elif key == "PC-Compound_charge":
            d["formal_charge"] = dictionary[key]

        elif key == "PC-Compound_props":
            for cnt in value:
                for ele in value[cnt]:
                    for x,y in iter(ele.items()):
                        if x == "PC-InfoData_urn":
                            for i,j in iter(y.items()):
                                if i == "PC-Urn":
                                    val = ele["PC-InfoData_value"]
                                    for z in val:
                                        val1 = val[z]
                                    for k,l in iter(j.items()):
                                        if l == "Hydrogen Bond Acceptor":
                                            d["hydrogen_bond_acceptor_count"] = val1

                                        elif l == "Hydrogen Bond Donor":
                                            d["hydrogen_bond_donor_count"] = val1

                                        elif l == "Rotatable Bond":
                                            d["rotatable_bond_count"] = val1

                                        elif l == "IUPAC Name":
                                            IUPAC = j["PC-Urn_name"]
                                            IUPAC = IUPAC.lower()
                                            iupac_dict[IUPAC] = val1
                                            d["iupac"] = iupac_dict
                                            iupac_dict = {}

                                        elif l == "InChI":
                                            d["inchi"] = val1
                                            break

                                        elif l == "InChIKey":
                                            d["inchi_key"] = val1
                                            break

                                        elif l == "Log P":
                                            d["xlogp"] = val1

                                        elif l == "Mass":
                                            d["exact_mass"] = val1

                                        elif l == "Molecular Formula":
                                            d["molecular_formula"] = val1

                                        elif l == "Molecular Weight":
                                            d["molecular_weight"] = val1

                                        elif l == "SMILES":
                                            smiles = j["PC-Urn_name"]
                                            smiles = smiles.lower()
                                            smile_dict[smiles] = val1
                                            d["smiles"] = smile_dict
                                            smile_dict = {}

                                        elif l == "Topological":
                                            d["topological_polar_surface_area"] = val1

                                        elif l == "Weight":
                                            d["monoisotopic_weight"] = val1

                                        elif l == "Compound Complexity":
                                            d["complexity"] = val1

        elif key == "PC-Compound_count":
            for cnt in value:
                for x,y in iter(value[cnt].items()):
                    if x == "PC-Count_heavy-atom":
                        d["heavy_atom_count"] = y

                    elif x == "PC-Count_atom-chiral":
                        d["chiral_atom_count"] = y

                    elif x == "PC-Count_atom-chiral-def":
                        d["defined_atom_stereocenter_count"] = y

                    elif x == "PC-Count_atom-chiral-undef":
                        d["undefined_atom_stereocenter_count"] = y

                    elif x == "PC-Count_bond-chiral":
                        d["chiral_bond_count"] = y

                    elif x == "PC-Count_bond-chiral-def":
                        d["defined_bond_stereocenter_count"] = y

                    elif x == "PC-Count_bond-chiral-undef":
                        d["undefined_bond_stereocenter_count"] = y

                    elif x == "PC-Count_isotope-atom":
                        d["isotope_atom_count"] = y

                    elif x == "PC-Count_covalent-unit":
                        d["covalently-bonded_unit_count"] = y

                    elif x == "PC-Count_tautomers":
                        d["tautomers_count"] = y

    restr_dict = {}
    restr_dict['_id'] = str(d["cid"])
    restr_dict["pubchem"] = d
    restr_dict = value_convert_to_number(restr_dict,skipped_keys=["_id"])
    return restr_dict

