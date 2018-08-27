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


SRC_META = {
        "url": 'https://www.ebi.ac.uk/chebi/',
        "license_url" : "https://www.ebi.ac.uk/about/terms-of-use",
        "license_url_short" : "https://goo.gl/FJpLMf"
        }


class ChebiUploader(BaseDrugUploader):

    name = "chebi"
    storage_class = storage.IgnoreDuplicatedStorage
    __metadata__ = {"src_meta" : SRC_META}

    @DataTransformMDB(G, ['chebi', ('drugbank', 'chebi.drugbank_database_links')], ['inchikey', 'drugbank'], skip_w_regex="^[A-Z]{14}\-[A-Z]{10}(\-[A-Z])?")
    def load_data(self,data_folder):
        self.logger.info("Load data from '%s'" % data_folder)
        input_file = os.path.join(data_folder,"ChEBI_complete.sdf")
        assert os.path.exists(input_file), "Can't find input file '%s'" % input_file
        return load_data(input_file)

    def post_update_data(self, *args, **kwargs):
        for idxname in ["chebi.chebi_id"]:
            self.logger.info("Indexing '%s'" % idxname)
            # background=true or it'll lock the whole database...
            self.collection.create_index([(idxname,pymongo.HASHED)],background=True)

    @classmethod
    def get_mapping(klass):
        mapping = {
            "chebi" : {
                "properties" : {
                    "chebi_id" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "chebi_name" : {
                        "type":"string"
                        },
                    "star" : {
                        "type":"integer"
                        },
                    "definition" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "secondary_chebi_id" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "smiles" : {
                        "type" : "string",
                        "analyzer":"string_lowercase"
                        },
                    "inchi" : {
                        "type" : "string",
                        "analyzer":"string_lowercase"
                        },
                    "inchikey" : {
                        "type" : "string",
                        "analyzer":"string_lowercase"
                        },
                    "formulae" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "charge" : {
                        "type":"integer"
                        },
                    "mass" : {
                        "type":"float"
                        },
                    "monoisotopic_mass" : {
                        "type":"float"
                        },
                    "iupac_names" : {
                        "type":"string"
                        },
                    "synonyms" : {
                        "type":"string"
                        },
                    "kegg_compound_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "lipid_maps_instance_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "pubchem_database_links" : {
                        "type" : "string"
                        },
                    "pubmed_citation_links" : {
                        "type" : "string"
                        },
                    "uniprot_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "last_modified" : {
                        "type" : "string"
                        },
                    "inn" : {
                        "type" : "string",
                        "analyzer":"string_lowercase"
                        },
                    "beilstein_registry_numbers" : {
                        "type" : "string"
                        },
                    "gmelin_registry_numbers" : {
                        "type" : "string"
                        },
                    "drugbank_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "kegg_drug_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "cas": {
                        "analyzer": "string_lowercase",
                        "type": "string"
                        },
                    "patent_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "sabio_rk_database_links" : {
                        "type" : "string"
                        },
                    "intenz_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "rhea_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "pdbechem_database_links" : {
                        "type":"string"
                        },
                    "kegg_glycan_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "molbase_database_links" : {
                        "type":"string"
                        },
                    "come_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "resid_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "um_bbd_compid_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "intact_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "biomodels_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "reactome_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        },
                    "arrayexpress_database_links" : {
                        "type":"string",
                        "analyzer":"string_lowercase"
                        }
                }
            }
        }

        return mapping


