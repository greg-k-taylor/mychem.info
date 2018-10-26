import biothings.hub.dataload.uploader as uploader

class AeolusUploader(uploader.DummySourceUploader):

    name = "aeolus"
    __metadata__ = {
            "src_meta" : {
                "url" : "http://www.nature.com/articles/sdata201626",
                "license_url" : "http://datadryad.org/resource/doi:10.5061/dryad.8q0s4",
		"license_url_short" : "https://goo.gl/pLRNT8",
                "license" : "CC0 1.0",
                }
            }

    @classmethod
    def get_mapping(klass):
        mapping = {
                "aeolus": {
                    "properties": {
                        "drug_id": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "drug_name": {
                            "type": "text",
                            },
                        "inchikey": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "no_of_outcomes": {
                            "type": "integer",
                            },
                        "pt": {
                            "type": "text",
                            },
                        "unii": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "drug_vocab": {
                            "type": "text"
                            },
                        "drug_code": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "rxcui": {
                            "normalizer": "keyword_lowercase_normalizer",
                            "type": "keyword",
                            },
                        "relationships": {
                            "properties": {
                                "relatedSubstance": {
                                    "properties": {
                                        "approvalID": {
                                            "normalizer": "keyword_lowercase_normalizer",
                                            "type": "keyword",
                                            },
                                        "refPname": {
                                            "normalizer": "keyword_lowercase_normalizer",
                                            "type": "keyword",
                                            }
                                        }
                                    },
                                "type": {
                                    "normalizer": "keyword_lowercase_normalizer",
                                    "type": "keyword",
                                    }
                                }
                            },
                        "outcomes": {
                                "properties": {
                                    "code": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                    "vocab": {
                                        "type": "text"
                                        },
                                    "case_count": {
                                        "type": "long",
                                        },
                                    "id": {
                                        "normalizer": "keyword_lowercase_normalizer",
                                        "type": "keyword",
                                        },
                                    "name": {
                                        "type": "text",
                                        },
                                    "prr": {
                                        "type": "float",
                                        },
                                    "prr_95_ci": {
                                        "type": "float",
                                        },
                                    "ror": {
                                        "type": "float",
                                        },
                                    "ror_95_ci": {
                                        "type": "float",
                                        },
                                    },
                                }
                        }
            }
        }

        return mapping
