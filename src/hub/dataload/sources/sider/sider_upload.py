import os
import glob
import zipfile
import pymongo

from .sider_parser import load_data
from .sider_parser import percent_float
from hub.dataload.uploader import BaseDrugUploader
import biothings.hub.dataload.storage as storage
from biothings.utils.mongo import get_src_db
from dotstring import key_value


SRC_META = {
        "url": 'http://sideeffects.embl.de/',
        "license_url" : "ftp://xi.embl.de/SIDER/LICENSE",
        "license_url_short" : "https://goo.gl/8b7ZCQ",
        "license": "CC BY-NC-SA 3.0"
        }


class SiderUploader(BaseDrugUploader):

    name = "sider"
    #storage_class = storage.IgnoreDuplicatedStorage
    __metadata__ = {"src_meta" : SRC_META}

    def load_data(self,data_folder):
        input_file = os.path.join(data_folder,"merged_freq_all_se_indications.tsv")
        self.logger.info("Load data from file '%s'" % input_file)
        pubchem_col = get_src_db()["pubchem"]
        assert pubchem_col.count() > 0, "'pubchem' collection is empty (required for inchikey " + \
                "conversion). Please run 'pubchem' uploader first"
        docs = load_data(input_file, pubchem_col)
        for doc in docs:
            # sort the 'sider' list by "sider.side_effect.frequency"
            doc['sider'] = sorted(doc['sider'],
                                  key=lambda x: percent_float(key_value(x, "side_effect.frequency")),
                                  reverse=True)
            yield doc

    def post_update_data(self, *args, **kwargs):
        # hashed because inchi is too long (and we'll do == ops to hashed are enough)
        for idxname in ["pubchem.inchi"]:
            self.logger.info("Indexing '%s'" % idxname)
            self.collection.create_index([(idxname,pymongo.HASHED)],background=True)

    @classmethod
    def get_mapping(klass):
        mapping = {
            "sider" : {
                "properties" : {
                    "meddra" : {
                        "properties" : {
                            "umls_id" : {
                                "type" : "string",
                                "analyzer":"string_lowercase"
                                },
                            "type" : {
                                "type" : "string",
                                "analyzer":"string_lowercase"
                                }
                            }
                        },
                    "side_effect" : {
                        "properties" : {
                            "frequency" : {
                                "type" : "string",
                                "analyzer":"string_lowercase"
                                },
                            "name" : {
                                "type" : "string",
                                "analyzer":"string_lowercase"
                                },
                            "placebo" : {
                                "type":"boolean"
                                }
                            }
                        },
                    "stitch" : {
                        "properties" : {
                            "flat" : {
                                "type" : "string",
                                "analyzer":"string_lowercase"
                                },
                            "stereo" : {
                                "type" : "string",
                                "analyzer":"string_lowercase"
                                }
                            }
                        },
                    "indication" : {
                        "properties" : {
                            "name" : {
                                "type" : "string",
                                "analyzer":"string_lowercase"
                                },
                            "method_of_detection" : {
                                "type" : "string",
                                "analyzer":"string_lowercase"
                                }
                            }
                        }
                }
            }
        }
        return mapping

