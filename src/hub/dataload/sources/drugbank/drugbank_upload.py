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
                        "drugs_com": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "guide_to_pharmacology": {
                            "type": "integer"
                            },
                        "groups": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "uniprotkb": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "pharmacology": {
                            "properties": {
                                "protein_binding": {
                                    "type": "text"
                                    },
                                "indication": {
                                    "type": "text"
                                    },
                                "absorption": {
                                    "type": "text"
                                    },
                                "clearance": {
                                    "type": "text"
                                    },
                                "route_of_elimination": {
                                    "type": "text"
                                    },
                                "volume_of_distribution": {
                                    "type": "text"
                                    },
                                "snp_adverse_drug_reactions": {
                                    "properties": {
                                        "reaction": {
                                            "properties": {
                                                "protein-name": {
                                                    "type": "text"
                                                    },
                                                "gene-symbol": {
                                                    "normalizer": "keyword_lowercase_normalizer",
                                                    "type": "keyword",
                                                    },
                                                "pubmed-id": {
                                                    "type": "integer"
                                                    },
                                                "rs-id": {
                                                    "normalizer": "keyword_lowercase_normalizer",
                                                    "type": "keyword",
                                                    },
                                                "description": {
                                                    "type": "text"
                                                    },
                                                "allele": {
                                                    "type": "text"
                                                    },
                                                "uniprot-id": {
                                                    "normalizer": "keyword_lowercase_normalizer",
                                                    "type": "keyword",
                                                    },
                                                "adverse-reaction": {
                                                    "type": "text"
                                                    }
                                                }
                                            }
                                        }
                                    },
                                "toxicity": {
                                        "type": "text"
                                        },
                                "pharmacodynamics": {
                                        "type": "text"
                                        },
                                "snp_effects": {
                                        "properties": {
                                            "effect": {
                                                "properties": {
                                                    "defining-change": {
                                                        "type": "text"
                                                        },
                                                    "protein-name": {
                                                        "type": "text"
                                                        },
                                                    "rs-id": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword",
                                                        },
                                                    "pubmed-id": {
                                                        "type": "integer"
                                                        },
                                                    "description": {
                                                        "type": "text"
                                                        },
                                                    "allele": {
                                                        "type": "text"
                                                        },
                                                    "uniprot-id": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword",
                                                        },
                                                    "gene-symbol": {
                                                        "normalizer": "keyword_lowercase_normalizer",
                                                        "type": "keyword",
                                                        }
                                                    }
                                                }
                                            }
                                        },
                                "half_life": {
                                        "type": "text"
                                        },
                                "description": {
                                        "type": "text"
                                        },
                                "metabolism": {
                                        "type": "text"
                                        },
                                "affected_organisms": {
                                        "type": "text"
                                        },
                                "mechanism_of_action": {
                                        "type": "text"
                                        }
                                }
                    },
                    "rxlist": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "pubchem_substance": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "pubchem_compound": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "transporters": {
                            "properties": {
                                "organism": {
                                    "type": "text"
                                    },
                                "id": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "name": {
                                    "type": "text"
                                    },
                                "known_action": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "uniprot": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "source": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "general_function": {
                                    "type": "text"
                                    },
                                "actions": {
                                    "type": "text"
                                    },
                                "specific_function": {
                                    "type": "text"
                                    }
                                }
                            },
                    "food_interactions": {
                            "type": "text"
                            },
                    "inchi_key": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "synonyms": {
                            "type": "text",
                            'copy_to': ['all'],
                            },
                    "patents": {
                            "properties": {
                                "country": {
                                    "type": "text"
                                    },
                                "approved": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "number": {
                                    "type": "text"
                                    },
                                "expires": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "pediatric-extension": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    }
                                }
                            },
                    "pdrhealth": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "iupac": {
                            "type": "text"
                            },
                    "pharmgkb": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "inchi": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "mixtures": {
                            "properties": {
                                "name": {
                                    "type": "text",
                                    'copy_to': ['all'],
                                    },
                                "ingredients": {
                                    "type": "text"
                                    }
                                }
                            },
                    "manufacturers": {
                            "type": "text"
                            },
                    "iuphar": {
                            "type": "integer"
                            },
                    "smiles": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "pathways": {
                            "properties": {
                                "enzymes": {
                                    "properties": {
                                        "uniprot-id": {
                                            "normalizer": "keyword_lowercase_normalizer",
                                            "type": "keyword",
                                            }
                                        }
                                    },
                                "drugs": {
                                    "properties": {
                                        "drugbank-id": {
                                            "normalizer": "keyword_lowercase_normalizer",
                                            "type": "keyword",
                                            },
                                        "name": {
                                            "type": "text"
                                            }
                                        }
                                    },
                                "smpdb_id": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "name": {
                                    "type": "text"
                                    }
                                }
                            },
                    "international_brands": {
                            "properties": {
                                "company": {
                                    "type": "text"
                                    },
                                "name": {
                                    "type": "text"
                                    }
                                }
                            },
                    "bindingdb": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "dpd": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "predicted_properties": {
                            "properties": {
                                "number_of_rings": {
                                    "type": "integer"
                                    },
                                "smiles": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "h_bond_acceptor_count": {
                                    "type": "integer"
                                    },
                                "logp": {
                                    "type": "float"
                                    },
                                "traditional_iupac_name": {
                                    "type": "text"
                                    },
                                "inchikey": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "h_bond_donor_count": {
                                    "type": "integer"
                                    },
                                "polar_surface_area_(psa)": {
                                    "type": "float"
                                    },
                                "water_solubility": {
                                    "type": "text"
                                    },
                                "rotatable_bond_count": {
                                    "type": "integer"
                                    },
                                "inchi": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "refractivity": {
                                    "type": "float"
                                    },
                                "pka_(strongest_acidic)": {
                                    "type": "float"
                                    },
                                "logs": {
                                    "type": "float"
                                    },
                                "bioavailability": {
                                    "type": "boolean"
                                    },
                                "pka_(strongest_basic)": {
                                    "type": "float"
                                    },
                                "mddr_like_rule": {
                                        "type": "boolean"
                                        },
                                "physiological_charge": {
                                        "type": "integer"
                                        },
                                "polarizability": {
                                        "type": "float"
                                        },
                                "molecular_weight": {
                                        "type": "float"
                                        },
                                "monoisotopic_weight": {
                                        "type": "float"
                                        },
                                "iupac_name": {
                                        "type": "text"
                                        },
                                "rule_of_five": {
                                        "type": "boolean"
                                        },
                                "ghose_filter": {
                                        "type": "boolean"
                                        },
                                "molecular_formula": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        }
                                }
                    },
                    "carriers": {
                            "properties": {
                                "organism": {
                                    "type": "text"
                                    },
                                "id": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "name": {
                                    "type": "text"
                                    },
                                "known_action": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "uniprot": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "source": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "general_function": {
                                    "type": "text"
                                    },
                                "actions": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "specific_function": {
                                    "type": "text"
                                    }
                                }
                            },
                    "categories": {
                            "properties": {
                                "mesh-id": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "category": {
                                    "type": "text"
                                    }
                                }
                            },
                    "name": {
                            "type": "text"
                            },
                    "wikipedia": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "kegg_compound": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "weight": {
                            "properties": {
                                "monoisotopic": {
                                    "type": "float"
                                    },
                                "average": {
                                    "type": "float"
                                    }
                                }
                            },
                    "chembl": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "drugbank_id": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            'copy_to': ['all'],
                            },
                    "taxonomy": {
                            "properties": {
                                "subclass": {
                                    "type": "text"
                                    },
                                "superclass": {
                                    "type": "text"
                                    },
                                "class": {
                                    "type": "text"
                                    },
                                "substituent": {
                                    "type": "text"
                                    },
                                "description": {
                                    "type": "text"
                                    },
                                "alternative-parent": {
                                    "type": "text"
                                    },
                                "direct-parent": {
                                    "type": "text"
                                    },
                                "kingdom": {
                                    "type": "text"
                                    }
                                }
                            },
                    "products": {
                            "properties": {
                                "otc": {
                                    "type": "boolean"
                                    },
                                "approved": {
                                    "type": "boolean"
                                    },
                                "route": {
                                    "type": "text"
                                    },
                                "fda_application_number": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "started_marketing_on": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "generic": {
                                    "type": "boolean"
                                    },
                                "dosage_form": {
                                    "type": "text"
                                    },
                                "source": {
                                    "type": "text"
                                    },
                                "ended_marketing_on": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "country": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "strength": {
                                    "type": "text"
                                    },
                                "dpd": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "name": {
                                    "type": "text",
                                    'copy_to': ['all'],
                                    },
                                "ndc_product_code": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    }
                                }
                            },
                    "food-interactions": {
                            "type": "text"
                            },
                    "packagers": {
                            "type": "text"
                            },
                    "therapeutic_targets_database": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "salts": {
                            "type": "text"
                            },
                    "kegg_drug": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "accession_number": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "enzymes": {
                            "properties": {
                                "organism": {
                                    "type": "text"
                                    },
                                "id": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "name": {
                                    "type": "text"
                                    },
                                "known_action": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "uniprot": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "source": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "general_function": {
                                    "type": "text"
                                    },
                                "actions": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "specific_function": {
                                    "type": "text"
                                    }
                                }
                            },
                    "targets": {
                            "properties": {
                                "organism": {
                                    "type": "text"
                                    },
                                "id": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "name": {
                                    "type": "text"
                                    },
                                "known_action": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "uniprot": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "source": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "general_function": {
                                    "type": "text"
                                    },
                                "actions": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "specific_function": {
                                    "type": "text"
                                    }
                                }
                            },
                    "formula": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "chemspider": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "experimental_properties": {
                            "properties": {
                                "water_solubility": {
                                    "type": "text"
                                    },
                                "logs": {
                                    "type": "float"
                                    },
                                "boiling_point": {
                                    "type": "text"
                                    },
                                "caco2_permeability": {
                                    "type": "float"
                                    },
                                "melting_point": {
                                    "type": "text"
                                    },
                                "pka": {
                                    "type": "text"
                                    },
                                "logp": {
                                    "type": "float"
                                    }
                                }
                            },
                    "drug_interactions": {
                            "properties": {
                                "drugbank-id": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    },
                                "description": {
                                    "type": "text"
                                    },
                                "name": {
                                    "type": "text"
                                    }
                                }
                            },
                    "chebi": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "atc_codes": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "pdb": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                    "ahfs_codes": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            }
                    }
            }
        }

        return mapping

