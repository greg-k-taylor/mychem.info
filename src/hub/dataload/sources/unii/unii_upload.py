import os
import glob

from .unii_parser import load_data
from hub.dataload.uploader import BaseDrugUploader
import biothings.hub.dataload.storage as storage


SRC_META = {
        "url": 'https://fdasis.nlm.nih.gov/srs/',
        "license_url" : "?",
        }


class UniiUploader(BaseDrugUploader):

    name = "unii"
    storage_class = storage.IgnoreDuplicatedStorage
    __metadata__ = {"src_meta" : SRC_META}

    def load_data(self,data_folder):
        self.logger.info("Load data from '%s'" % data_folder)
        record_files = glob.glob(os.path.join(data_folder,"*Records*.txt"))
        assert len(record_files) == 1, "Expecting one record.txt file, got %s" % repr(record_files)
        input_file = record_files.pop()
        assert os.path.exists(input_file), "Can't find input file '%s'" % input_file
        return load_data(input_file)

    @classmethod
    def get_mapping(klass):
        mapping = {
                "unii": {
                    "properties": {
                        "unii": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            'copy_to': ['all'],
                            },
                        "preferred_term": {
                            "type": "text",
                            },
                        "registry_number": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "ec": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "ncit": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "rxcui": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "itis": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "ncbi": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "plants": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "grin": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "inn_id": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "molecular_formula": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "inchikey": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "smiles": {
                                "normalizer": "keyword_lowercase_normalizer",
                                "type": "keyword",
                                },
                        "unii_type": {
                                "type": "text"
                                },
                        }
            }
        }

        return mapping

