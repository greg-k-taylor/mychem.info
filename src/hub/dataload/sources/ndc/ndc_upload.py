import os
import glob
import pymongo

from .ndc_parser import load_data
from hub.dataload.uploader import BaseDrugUploader
import biothings.hub.dataload.storage as storage
from biothings.utils.common import unzipall
from biothings.utils.mongo import get_src_db, check_document_size

from hub.datatransform.keylookup import MyChemKeyLookup

SRC_META = {
        "url" : "http://www.fda.gov/Drugs/InformationOnDrugs/ucm142438.htm",
        "license_url" : "?",
        }


class NDCUploader(BaseDrugUploader):

    name = "ndc"
    storage_class = (storage.BasicStorage,storage.CheckSizeStorage)
    __metadata__ = {"src_meta" : SRC_META}
    keylookup = MyChemKeyLookup([("ndc","ndc.productndc")])

    def load_data(self,data_folder):
        docs = self.keylookup(load_data)(data_folder)
        inchi_key = {}
        for doc in docs:
            # IK found, but other productndc could also match the same
            # IK so we keep them in a list
            if type(doc["ndc"]) == list:
                inchi_key.setdefault(doc["_id"],doc["ndc"])
            else:
                if not doc["ndc"] in inchi_key.setdefault(doc["_id"],[]):
                    inchi_key.setdefault(doc["_id"],[]).append(doc["ndc"])
        l = []
        for ik,ndc in inchi_key.items():
            if len(ndc) == 1:
                ndc = ndc.pop()
            yield {"_id": ik, "ndc": ndc}


    @classmethod
    def get_mapping(klass):
        mapping = {
                "ndc" : {
                    "properties" : {
                        "product_id" : {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "productndc" : {
                            "type" : "text"
                            },
                        "producttypename" : {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "proprietaryname" : {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "proprietarynamesuffix" : {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "nonproprietaryname" : {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "dosageformname" : {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "routename" : {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "startmarketingdate" : {
                            "type" : "text"
                            },
                        "endmarketingdate" : {
                            "type" : "text"
                            },
                        "marketingcategoryname" : {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "applicationnumber" : {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "labelername" : {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "substancename" : {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword",
                                },
                        "active_numerator_strength" : {
                                "type" : "text"                    
                                },
                        "active_ingred_unit" : {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword",
                                },
                        "pharm_classes" : {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword",
                                },
                        "deaschedule" : {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword",
                                },
                        "package" : {
                                "properties" : {
                                    "packagedescription" : {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                    "ndcpackagecode" : {
                                        "type" : "text"
                                        }
                                    }
                                }
                        }
            }
        }

        return mapping

