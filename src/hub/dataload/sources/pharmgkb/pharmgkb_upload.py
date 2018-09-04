import os
import glob

import biothings.hub.dataload.storage as storage
from biothings.utils.mongo import get_src_db

from .pharmgkb_parser import load_data
from hub.dataload.uploader import BaseDrugUploader
from biothings.hub.datatransform import DataTransformMDB
from hub.dataload.graph_mychem import graph_mychem as G


SRC_META = {
        "url": 'https://www.pharmgkb.org/',
        "license_url" : "https://www.pharmgkb.org/page/dataUsagePolicy",
        "license_url_short" : "https://goo.gl/6ZW4iX"
        }


class PharmGkbUploader(BaseDrugUploader):

    name = "pharmgkb"
    storage_class = storage.IgnoreDuplicatedStorage
    __metadata__ = {"src_meta" : SRC_META}

    @DataTransformMDB(G, [('inchi', 'pharmgkb.inchi'), ('drugbank', 'pharmgkb.cross_references.drugbank'), ('pubchem', 'pharmgkb.cross_references.pubchem_compound'), ('drugbank', 'pharmgkb.cross_references.drugbank'), ('chebi-short', 'pharmgkb.cross_references.chebi')], ['inchikey'])
    def load_data(self,data_folder):
        self.logger.info("Load data from '%s'" % data_folder)
        input_file = os.path.join(data_folder,"drugs.tsv")
        assert os.path.exists(input_file), "Can't find input file '%s'" % input_file
        # get others source collection for inchi key conversion
        drugbank_col = get_src_db()["drugbank"]
        assert drugbank_col.count() > 0, "'drugbank' collection is empty (required for inchikey " + \
                "conversion). Please run 'drugbank' uploader first"
        pubchem_col = get_src_db()["pubchem"]
        assert pubchem_col.count() > 0, "'pubchem' collection is empty (required for inchikey " + \
                "conversion). Please run 'pubchem' uploader first"
        chembl_col = get_src_db()["chembl"]
        assert chembl_col.count() > 0, "'chembl' collection is empty (required for inchikey " + \
                "conversion). Please run 'chembl' uploader first"
        chebi_col = get_src_db()["chebi"]
        assert chebi_col.count() > 0, "'chebi' collection is empty (required for inchikey " + \
                "conversion). Please run 'chebi' uploader first"
        return load_data(input_file,drugbank_col,pubchem_col,chembl_col,chebi_col)

    @classmethod
    def get_mapping(klass):
        mapping = {
            "pharmgkb" : {
                "properties" : {
                    "pharmgkb_accession_id" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "name" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "generic_names" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "trade_names" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "brand_mixtures" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "type" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "cross_references" : {
                        "properties" : {
                            "chebi" : {
                                "type":"string"
                                },
                            "chemspider" : {
                                "type":"string"
                                },
                            "therapeutic_targets_database" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "pubchem_substance" : {
                                "type":"string"
                                },
                            "web_resource" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "drugbank" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "drugs_product_database" : {
                                "type":"string"
                                },
                            "pubchem_compound" : {
                                "type":"string"
                                },
                            "bindingdb" : {
                                "type":"string"
                                },
                            "kegg_drug" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "fda_drug_label_at_dailymed" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "national_drug_code_directory" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "kegg_compound" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "pdb" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "iuphar_ligand" : {
                                "type":"string"
                                },
                            "clinicaltrials_gov" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "het" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "genbank" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "uniprotkb" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                }
                            }
                        },
                    "smiles" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "inchi" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "dosing_guideline" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "external_vocabulary" : {
                        "properties" : {
                            "umls" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "rxnorm" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "ndfrt" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "atc" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                },
                            "mesh" : {
                                "type":"string",
                                "analyzer":"string_lowercase"
                                }
                        }
                    }
                }
            }
        }
        return mapping

