import os
import glob
import pymongo

from .drugbank_parser import load_data
from hub.dataload.uploader import BaseDrugUploader
import biothings.hub.dataload.storage as storage
from biothings.utils.common import unzipall


SRC_META = {
        "url" : "http://www.drugbank.ca",
        "license_url" : "https://www.drugbank.ca/releases/latest",
        "license_url_short" : "https://goo.gl/kvVASD",
        "license" : "CC BY-NC 4.0",
        }


class DrugBankUploader(BaseDrugUploader):

    name = "drugbank"
    storage_class = storage.IgnoreDuplicatedStorage
    __metadata__ = {"src_meta" : SRC_META}

    def load_data(self,data_folder):
        xmlfiles = glob.glob(os.path.join(data_folder,"*.xml"))
        if not xmlfiles:
            self.logger.info("Unzipping drugbank archive")
            unzipall(data_folder)
            self.logger.info("Load data from '%s'" % data_folder)
            xmlfiles = glob.glob(os.path.join(data_folder,"*.xml"))
        assert len(xmlfiles) == 1, "Expecting one xml file, got %s" % repr(xmlfiles)
        input_file = xmlfiles.pop()
        assert os.path.exists(input_file), "Can't find input file '%s'" % input_file
        return load_data(input_file)

    def post_update_data(self, *args, **kwargs):
        for idxname in ["drugbank.drugbank_id","drugbank.chebi","drugbank.inchi"]:
            self.logger.info("Indexing '%s'" % idxname)
            # background=true or it'll lock the whole database...
            self.collection.create_index([(idxname,pymongo.HASHED)],background=True)
        # hashed index won"t support arrays, values are small enough to standard
        self.collection.create_index("drugbank.products.ndc_product_code")

    @classmethod
    def get_mapping(klass):
        mapping = {
            "drugbank": {
                "properties": {
                    "name": {
                        "type":"string"
                    },
                    "ndc_directory": {
                        "type":"string"
                    },
                    "kegg": {
                        "type":"string",
                        "analyzer":"string_lowercase"
                    },
                    "uniprotkb": {
                        "type":"string",
                        "analyzer":"string_lowercase"
                    },
                    "pharmgkb": {
                        "type":"string",
                        "analyzer":"string_lowercase"
                    },
                    "wikipedia": {
                        "type":"string"
                    },
                    "dpd": {
                        "type":"string"
                    },
                    "groups": {
                        "type":"string",
                        "analyzer":"string_lowercase"
                    },
                    "fasta_sequences": {
                        "type":"string"
                    },
                    "ahfs_code": {
                        "type":"string"
                    },
                    "smiles": {
                        "type":"string",
                        "analyzer":"string_lowercase"
                    },
                    "inchi_key": {
                        "type":"string",
                        "analyzer":"string_lowercase"
                    },
                    "inchi": {
                        "type":"string",
                        "analyzer":"string_lowercase"
                    },
                    "iupac": {
                        "type":"string"
                    },
                    "synonyms": {
                        "type":"string"
                    },
                   "weight": {
                        "properties": {
                            "average": {
                                "type":"float"
                            },
                            "monoisotopic": {
                                "type":"float"
                            }
                        }
                    },
                    "accession_number": {
                        "type":"string",
                        "analyzer":"string_lowercase"
                    },
                    "formula": {
                        "type":"string",
                        "analyzer":"string_lowercase"
                    },
                    "drug_interaction": {
                        "properties": {
                            "description": {
                                "type":"string",
                            },
                            "drugbank-id": {
                                "type":"string",
                                "analyzer":"string_lowercase"
                            },
                            "name": {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                            }
                        }
                    },

                    "food_interaction": {
                        "type":"string"
                    },
                    "international_brands" : {
                        "properties" : {
                            "company" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "name" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                }
                            }
                        },
                    "drugbank_id": {
                        "analyzer": "string_lowercase",
                        "type": "string"
                        },
                    "targets" : {
                        "properties" : {
                            "organism" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "id" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "name" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "uniprot" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "source" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "specific_function" : {
                                "type":"string"
                                },
                            "general_function" : {
                                "type":"string"
                                },
                            "actions" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "known_action" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                }
                            }
                        },
                    "transporters" : {
                        "properties" : {
                            "organism" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "id" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "name" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "uniprot" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "source" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "specific_function" : {
                                "type":"string"
                                },
                            "general_function" : {
                                "type":"string"
                                },
                            "actions" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "known_action" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                }
                            }
                        },
                    "enzymes" : {
                        "properties" : {
                            "organism" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "id" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "name" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "uniprot" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "source" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "specific_function" : {
                                "type":"string"
                                },
                            "general_function" : {
                                "type":"string"
                                },
                            "actions" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "known_action" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                }
                            }
                        },
                    "carriers" : {
                        "properties" : {
                            "organism" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "id" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "name" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "uniprot" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "source" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "specific_function" : {
                                "type":"string"
                                },
                            "general_function" : {
                                "type":"string"
                                },
                            "actions" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "known_action" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                }
                            }
                        },
                    "mixtures" : {
                        "properties" : {
                            "name" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "ingredients" : {
                                "type":"string"
                                }
                            }
                        },
                    "pathways" : {
                        "properties" : {
                            "drugs" : {
                                "properties" : {
                                    "drugbank-id" : {
                                        "type":"string" ,
                                        "analyzer":"string_lowercase"
                                        },
                                    "name" : {
                                        "type":"string" ,
                                        "analyzer":"string_lowercase"
                                        }
                                    }
                                },
                            "enzymes" : {
                                "properties" : {
                                    "uniprot-id" : {
                                        "type":"string" ,
                                        "analyzer":"string_lowercase"
                                        }
                                    }
                                },
                            "smpdb_id" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "name" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                }
                            }
                        },
                    "categories" : {
                        "properties" : {
                            "category" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "mesh-id" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                }
                            }
                        },
                    "atc_codes" : {
                        "type":"string" ,
                        "analyzer":"string_lowercase"
                        },
                    "salts" : {
                        "type":"string" ,
                        "analyzer":"string_lowercase"
                        },
                    "patents" : {
                        "properties" : {
                            "country" : {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                                },
                            "expires" : {
                                "type":"string"
                                },
                            "number" : {
                                "type":"string"
                                },
                            "approved" : {
                                "type":"string"
                                }
                            }
                        },
                    "pharmacology": {
                        "properties": {
                            "toxicity": {
                                "type":"string"
                            },
                            "protein_binding": {
                                "type":"string"
                            },
                            "description": {
                                "type":"string"
                            },
                            "absorption": {
                                "type":"string"
                            },
                            "pharmacodynamics": {
                                "type":"string"
                            },
                            "affected_organisms": {
                                "type":"string"
                            },
                            "mechanism_of_action": {
                                "type":"string"
                            },
                            "route_of_elimination": {
                                "type":"string"
                            },
                            "half_life": {
                                "type":"string"
                            },
                            "indication": {
                                "type":"string"
                            },
                            "volume_of_distribution": {
                                "type":"string"
                            },
                            "clearance": {
                                "type":"string"
                            },
                            "metabolism": {
                                "type":"string"
                            },
                            "snp_adverse_drug_reactions" : {
                                "properties" : {
                                    "reaction" : {
                                        "properties" : {
                                            "protein-name" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "description" : {
                                                "type":"string"
                                                },
                                            "gene-symbol" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "rs-id" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "pubmed-id" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "adverse-reaction" : {
                                                "type":"string"
                                                },
                                            "uniprot-id" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "allele" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                }
                                            }
                                        }
                                    }
                                },
                            "snp_effects" : {
                                "properties" : {
                                    "effect" : {
                                        "properties" : {
                                            "defining-change" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "protein-name" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "description" : {
                                                "type":"string"
                                                },
                                            "gene-symbol" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "rs-id" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "pubmed-id" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "adverse-reaction" : {
                                                "type":"string"
                                                },
                                            "uniprot-id" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                },
                                            "allele" : {
                                                "type":"string" ,
                                                "analyzer":"string_lowercase"
                                                }
                                            }
                                        }
                                    }
                                }
                        }
                    },

                    "experimental_properties": {
                        "properties": {
                            "melting_point": {
                                "type":"string"
                            },
                            "isoelectric_point": {
                                "type":"float"
                            },
                            "molecular_formula": {
                                "type":"string"
                            },
                            "hydrophobicity": {
                                "type":"float"
                            },
                            "molecular_weight": {
                                "type":"float"
                            },
                            "pka": {
                                "type":"string"
                            },
                            "water_solubility": {
                                "type":"string"
                            },
                            "logs": {
                                "type":"float"
                            },
                            "logp": {
                                "type":"float"
                            }
                        }
                    },
                    "predicted_properties": {
                        "properties": {
                            "mddr_like_rule": {
                                "type":"boolean"
                            },
                            "logs": {
                                "type":"float"
                            },
                            "logp": {
                                "type":"float"
                            },
                            "number_of_rings": {
                                "type":"integer"
                            },
                            "ghose_filter": {
                                "type":"boolean"
                            },
                            "h_bond_donor_count": {
                                "type":"integer"
                            },
                            "molecular_weight": {
                                "type":"float"
                            },
                            "monoisotopic_weight": {
                                "type":"float"
                            },
                            "water_solubility": {
                                "type":"string"
                            },
                            "rotatable_bond_count": {
                                "type":"integer"
                            },
                            "iupac_name": {
                                "type":"string"
                            },
                            "polarizability": {
                                "type":"float"
                            },
                            "smiles": {
                                "type":"string",
                                "analyzer":"string_lowercase"
                            },
                            "inchikey": {
                                "type":"string" ,
                                "analyzer":"string_lowercase"
                            },
                            "bioavailability": {
                                "type":"boolean"
                            },
                            "physiological_charge": {
                                "type":"float"
                            },
                            "pka_(strongest_basic)": {
                                "type":"float"
                            },
                            "inchi": {
                                "type":"string",
                                "analyzer":"string_lowercase"
                            },
                            "polar_surface_area_(psa)": {
                                "type":"float"
                            },
                            "rule_of_five": {
                                "type":"boolean"
                            },
                            "refractivity": {
                                "type":"float"
                            },
                            "pka_(strongest_acidic)": {
                                "type":"float"
                            },
                            "traditional_iupac_name": {
                                "type":"string"
                            },
                            "h_bond_acceptor_count": {
                                "type":"integer"
                            },
                            "molecular_formula": {
                                "type":"string",
                                "analyzer":"string_lowercase"
                            }
                        }
                    },
                    "taxonomy": {
                        "properties": {
                            "kingdom": {
                                "type":"string"  ,
                                "analyzer":"string_lowercase"
                            },
                            "description": {
                                "type":"string"
                            },
                            "subclass": {
                                "type":"string"
                            },
                            "substituent": {
                                "type":"string"
                            },
                            "alternative_parent": {
                                "type":"string"
                            },
                            "superclass": {
                                "type":"string"
                            },
                            "direct_parent": {
                                "type":"string"
                            },
                            "class": {
                                "type":"string"
                            }
                        }
                    },
                    "packagers": {
                        "type":"string"
                    },
                    "manufacturers": {
                        "type":"string"
                    },
                    "products": {
                        "properties": {
                            "strength": {
                                "type":"string"
                            },
                            "name": {
                                "type":"string"
                            },
                            "generic": {
                                "type":"boolean"
                            },
                            "route": {
                                "type":"string",
                                "analyzer":"string_lowercase"
                            },
                            "otc": {
                                "type":"boolean",
                            },
                            "dosage_form": {
                                "type":"string"
                            }
                        }
                    }

                }
            }
        }

        return mapping

