import os
import glob
import zipfile
import pymongo

from .chebi_parser import load_data
from hub.dataload.uploader import BaseDrugUploader
from biothings.utils.mongo import get_src_db
import biothings.hub.dataload.storage as storage
from biothings.hub.datatransform import DataTransformMDB
from hub.dataload.graph_mychem import graph_mychem as G

from hub.datatransform.keylookup import MyChemKeyLookup


SRC_META = {
        "url": 'https://www.ebi.ac.uk/chebi/',
        "license_url" : "https://www.ebi.ac.uk/about/terms-of-use",
        "license_url_short" : "https://goo.gl/FJpLMf"
        }


class ChebiUploader(BaseDrugUploader):

    name = "chebi"
    #storage_class = storage.IgnoreDuplicatedStorage
    storage_class = storage.RootKeyMergerStorage
    __metadata__ = {"src_meta" : SRC_META}
    keylookup = MyChemKeyLookup(
            [('inchikey','chebi.inchikey'),
             ('chebi','chebi.id'),
             ('drugbank','chebi.xref.drugbank'),
             ],
            copy_from_doc=True)

    @DataTransformMDB(G, ['chebi', ('drugbank', 'chebi.drugbank_database_links')], ['inchikey', 'drugbank'], skip_w_regex="^[A-Z]{14}\-[A-Z]{10}(\-[A-Z])?")
    def load_data(self,data_folder):
        self.logger.info("Load data from '%s'" % data_folder)
        input_file = os.path.join(data_folder,"ChEBI_complete.sdf")
        assert os.path.exists(input_file), "Can't find input file '%s'" % input_file
        return self.keylookup(load_data)(input_file)

    def post_update_data(self, *args, **kwargs):
        for idxname in ["chebi.id"]:
            self.logger.info("Indexing '%s'" % idxname)
            # background=true or it'll lock the whole database...
            self.collection.create_index([(idxname,pymongo.ASCENDING)],background=True)

    @classmethod
    def get_mapping(klass):
        mapping = {
                "chebi": {
                    "properties": {
                        "brand_names": {
                            "type": "text",
                            'copy_to': ['all'],
                            },
                        "id": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            'copy_to': ['all'],
                            },
                        "iupac": {
                            "type": "text"
                            },
                        "inchi": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "definition": {
                            "type": "text"
                            },
                        "star": {
                            "type": "integer"
                            },
                        "smiles": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "last_modified": {
                            "type": "text"
                            },
                        "inn": {
                            "type": "text"
                            },
                        "xref": {
                            "properties": {
                                "molbase": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "resid": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "come": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "pubchem": {
                                    "properties": {
                                        "sid": {
                                            "type": "integer"
                                            },
                                        "cid": {
                                            "type": "integer"
                                            }
                                        }
                                    },
                                "beilstein": {
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
                                "metacyc": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "biomodels": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "reactome": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "um_bbd_compid": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "lincs": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "uniprot": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "sabio_rk": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "patent": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "pdbechem": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "arrayexpress": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "cas": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "lipid_maps_class": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "kegg_drug": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "knapsack": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "lipid_maps_instance": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "intenz": {
                                        "type": "text"
                                        },
                                "kegg_glycan": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "ecmdb": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "hmdb": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "kegg_compound": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "ymdb": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "drugbank": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "rhea": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "gmelin": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                "intact": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        }
                                }
                        },
                        "monoisotopic_mass": {
                                "type": "float"
                                },
                        "mass": {
                                "type": "float"
                                },
                        "secondary_chebi_id": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword",
                                'copy_to': ['all'],
                                },
                        "formulae": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword",
                                },
                        "inchikey": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword",
                                },
                        "name": {
                                "type": "text",
                                'copy_to': ['all'],
                                },
                        "charge": {
                                "type": "integer"
                                },
                        "synonyms": {
                                "type": "text"
                                },
                        "citation": {
                                "properties": {
                                    "pubmed": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                    "agricola": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                    "pmc": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                    "chinese_abstracts": {
                                        "type": "integer"
                                        },
                                    "citexplore": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        }
                                    }
                                }
                        }
            }
        }
        return mapping


