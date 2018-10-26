import requests
import math

from biothings.utils.dataload import dict_sweep, unlist
from biothings.hub.datatransform.datatransform_api import DataTransformMyChemInfo


def count_total_docs():
    """
    Get the total number of documents in DGIdb.
    There is no specific API endpoint providing that.
    But the informaiton is embeded in the results returned from interaction endpoint
    """
    query_url = 'http://www.dgidb.org/api/v2/interactions?count=1&page=1'
    return requests.get(query_url).json()['_meta']['total_count']

def fetch_all_docs_from_api():
    """
    Fetch all DGIdb data from api and store it in a list
    use pagination
    return 500 docs each time
    """
    dgidb_docs = []
    # number of docs returned per API call
    doc_per_query = 500
    # get the total number of docs in DGIdb
    total_count = count_total_docs()
    template_url = 'http://www.dgidb.org/api/v2/interactions?count=' + str(doc_per_query) + '&page={page}'
    # use pagination to fetch all docs
    for i in range(1, math.ceil(total_count/500) + 1):
        query_url = template_url.replace('{page}', str(i))
        doc = requests.get(query_url).json()
        dgidb_docs += doc.get('records')
    # make sure all docs are fetched
    assert len(dgidb_docs) == total_count
    return dgidb_docs

@DataTransformMyChemInfo([('chembl', 'dgidb.chembl_id')], ['inchikey'])
def load_data():
    dgidb_docs = fetch_all_docs_from_api()
    for _doc in dgidb_docs:
        _doc['interaction_id'] = _doc.pop('id')
        yield dict_sweep(unlist({'dgidb': _doc}), vals=[None, "", []])
