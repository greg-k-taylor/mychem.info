import os
import glob

import biothings.hub.dataload.storage as storage
from biothings.utils.mongo import get_src_db

from .pharmgkb_parser import load_data
from hub.dataload.uploader import BaseDrugUploader
from biothings.hub.datatransform import DataTransformMDB
from hub.dataload.graph_mychem import graph_mychem as G

from hub.datatransform.keylookup import MyChemKeyLookup


SRC_META = {
        "url": 'https://www.pharmgkb.org/',
        "license_url": "https://www.pharmgkb.org/page/dataUsagePolicy",
        "license_url_short": "https://goo.gl/6ZW4iX",
        "license": "CC BY-SA 4.0"
        }


class PharmGkbUploader(BaseDrugUploader):

    name = "pharmgkb"
    storage_class = storage.RootKeyMergerStorage
    __metadata__ = {"src_meta" : SRC_META}
    keylookup = MyChemKeyLookup(
            [('inchi', 'pharmgkb.inchi'),
             ('pubchem', 'pharmgkb.xref.pubchem.cid'),
             ('drugbank', 'pharmgkb.xref.drugbank'),
             ('chebi', 'pharmgkb.xref.chebi')])

    @DataTransformMDB(G, [('inchi', 'pharmgkb.inchi'), ('pubchem', 'pharmgkb.cross_references.pubchem_compound'), ('drugbank', 'pharmgkb.cross_references.drugbank'), ('chebi-short', 'pharmgkb.cross_references.chebi')], ['inchikey', 'unii', 'rxnorm', 'drugbank', 'chebi', 'chembl', 'pubchem', 'drugname'] )
    def load_data(self,data_folder):
        self.logger.info("Load data from '%s'" % data_folder)
        input_file = os.path.join(data_folder,"drugs.tsv")
        assert os.path.exists(input_file), "Can't find input file '%s'" % input_file
        return self.keylookup(load_data)(input_file)

    @classmethod
    def get_mapping(klass):
        mapping = {
                "pharmgkb": {
                    "properties": {
                        "id": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            'copy_to': ['all'],
                            },
                        "dosing_guideline": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "inchi": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "name": {
                            "type": "text",
                            'copy_to': ['all'],
                            },
                        "smiles": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "generic_names": {
                            "type": "text",
                            'copy_to': ['all'],
                            },
                        "brand_mixtures": {
                            "type": "text"
                            },
                        "trade_names": {
                            "type": "text"
                            },
                        "type": {
                            "type": "text"
                            },
                        "xref": {
                            "properties": {
                                "web_resource": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "uniprotkb": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "pubchem": {
                                    "properties": {
                                        "sid": {
                                            "normalizer": "keyword_lowercase_normalizer",
                                            "type": "keyword",
                                            },
                                        "cid": {
                                            "normalizer": "keyword_lowercase_normalizer",
                                            "type": "keyword",
                                            }
                                        }
                                    },
                                "het": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "wikipedia": {
                                    "properties": {
                                        "url_stub": {
                                            "normalizer": "keyword_lowercase_normalizer",
                                            "type": "keyword",
                                            }
                                        }
                                    },
                                "iuphar_ligand": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "meddra": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "atc": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "kegg_compound": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "umls": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "clinicaltrials": {
                                        "properties": {
                                            "gov": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword",
                                                }
                                            }
                                        },
                                "genbank": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "rxnorm": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "chebi": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "cas": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "ttd": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "kegg_drug": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "mesh": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "ndc": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "chemspider": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "hmdb": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "dailymed": {
                                        "properties": {
                                            "setid": {
                                                "normalizer": "keyword_lowercase_normalizer",
                                                "type": "keyword",
                                                }
                                            }
                                        },
                                "ndfrt": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "bindingdb": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "drugbank": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "pdb": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "dpd": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        }
                                }
                        }
                    }
                }
            }
        return mapping

