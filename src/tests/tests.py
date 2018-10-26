import httplib2
import sys
import os
from nose.tools import ok_, eq_

from tornado.testing import AsyncHTTPTestCase
from tornado.web import Application

from biothings.tests.test_helper import BiothingTestHelperMixin

class MyChemTest(BiothingTestHelperMixin):
    host = os.getenv("MC_HOST", "")
    host = host.rstrip('/')
    api = host + '/v1'
    if host:
        sys.stderr.write("Testing on host: {}...\n".format(api))
    else:
        sys.stderr.write("Testing on build-in server: {}...\n".format(api))
    h = httplib2.Http()
    inchikey_id = 'ZRALSGWEFCBTJO-UHFFFAOYSA-N'
    drugbank_id = 'DB00551'
    chembl_id = 'CHEMBL1308'
    chebi_id = 'CHEBI:6431'
    unii_id = '7AXV542LZ4'

    def has_hits(self, q, morethan=0):
        d = self.json_ok(self.get_ok(self.api + '/query?q='+q))
        ok_(d.get('total', 0) > morethan and len(d.get('hits', [])) > 0)
        ok_('_id' in d['hits'][0])

    def test_non_fielded_query(self):
        # we can't really compare the results, we just need to ensure we have data
        self.has_hits('imatinib')
        self.has_hits('drugbank.name:imatinib')

    def test_chem_object(self):
        #test all fields are loaded in drug objects
        res = self.json_ok(self.get_ok(self.api + '/drug/' + self.inchikey_id))
        attr_li = ['_id']
        for attr in attr_li:
            assert res.get(attr, None) is not None, 'Missing field "{}" in chem "{}"'.format(attr, self.inchikey_id)

        # test for specific databases

    def test_query(self):
        # test query by drug name
        monobenzone = self.query_has_hits(q='drugbank.name:monobenzone')
        assert 'drugbank' in monobenzone['hits'][0]
        assert 'name' in monobenzone['hits'][0]['drugbank']
        assert monobenzone['hits'][0]['drugbank']['name'].lower() == 'monobenzone'

        # test query by drug target
        P34981 = self.query_has_hits(q='drugbank.targets.uniprot:P34981')
        assert 'drugbank' in P34981['hits'][0]
        assert 'targets' in P34981['hits'][0]['drugbank']
        assert 'uniprot' in P34981['hits'][0]['drugbank']['targets']
        assert P34981['hits'][0]['drugbank']['targets']['uniprot'] == 'P34981'

        #C0242339 = self.json_ok(self.get_ok(self.api + '/query?q=drugcentral.drug_use.indication.umls_cui:C0242339'))
        #assert 'drugcentral' in C0242339
        #assert 'drug_use' in C0242339['drugcentral']
        #assert 'indication' in C0242339['drugcentral']['drug_use']
        #assert 'umls_cui' in C0242339['drugcentral']['drug_use']['indication']
        #public query self.api at /query via get
        con = self.get_ok(self.api + '/query?q=monobenzone&callback=mycallback')
        ok_(con.startswith('mycallback('.encode('utf-8')))

        # testing non-ascii character
        res = self.json_ok(self.get_ok(self.api + '/query?q=\xef\xbf\xbd\xef\xbf\xbd'))
        eq_(res['hits'], [])

        self.get_status_code(self.api + '/query', status_code=400)        

    def test_query_post(self):
        #/query via post
        self.json_ok(self.post_ok(self.api + '/query', {'q': self.inchikey_id}))

        res = self.json_ok(self.post_ok(self.api + '/query', {'q': self.drugbank_id,
                                                              'scopes': 'drugbank.drugbank_id'}))
        eq_(len(res), 1)
        eq_(res[0]['_id'], 'RRUDCFGSUDOHDG-UHFFFAOYSA-N')

        res = self.json_ok(self.post_ok(self.api + '/query', {'q': self.drugbank_id + ',DB00441',
                                                              'scopes': 'drugbank.drugbank_id'}))
        eq_(len(res), 2)
        eq_(res[0]['_id'], 'RRUDCFGSUDOHDG-UHFFFAOYSA-N')
        eq_(res[1]['_id'], 'SDUQYLNIPVEERB-QPPQHZFASA-N')

        res = self.json_ok(self.post_ok(self.api + '/query', {'q': self.drugbank_id,
                                                              'scopes': 'drugbank.drugbank_id',
                                                              'fields': 'drugbank.drugbank_id'}))
        assert len(res) == 1
        assert 'query' in res[0]
        assert 'drugbank' in res[0] and 'drugbank_id' in res[0]['drugbank']
        assert res[0]['query'] == res[0]['drugbank']['drugbank_id'] 
        
        self.post_status_code(self.api + '/query', {}, status_code=400)

    def test_query_size(self):
        res = self.json_ok(self.get_ok(self.api + '/query?q=drugbank.name:acid&fields=drugbank.name'))
        eq_(len(res['hits']), 10)    # default
        res = self.json_ok(self.get_ok(self.api + '/query?q=drugbank.name:acid&fields=drugbank.name&size=1000'))
        eq_(len(res['hits']), 1000)
        res = self.json_ok(self.get_ok(self.api + '/query?q=drugbank.name:acid&fields=drugbank.name&size=1001'))
        eq_(len(res['hits']), 1000)
        res = self.json_ok(self.get_ok(self.api + '/query?q=drugbank.name:acid&fields=drugbank.name&size=2000'))
        eq_(len(res['hits']), 1000)

    def test_chem(self):
        # test different endpoint aliases
        drug = self.json_ok(self.get_ok(self.api + '/drug/' + self.inchikey_id))
        chem = self.json_ok(self.get_ok(self.api + '/chem/' + self.inchikey_id))
        compound = self.json_ok(self.get_ok(self.api + '/compound/' + self.inchikey_id))

        assert drug == chem
        assert chem == compound

        # test different drug identifiers
        drugbank = self.json_ok(self.get_ok(self.api + '/drug/' + self.drugbank_id))
        assert 'drugbank' in drugbank
        assert 'drugbank_id' in drugbank['drugbank']
        assert drugbank['drugbank']['drugbank_id'] == self.drugbank_id

        chembl = self.json_ok(self.get_ok(self.api + '/drug/' + self.chembl_id))
        assert 'chembl' in chembl
        assert 'molecule_chembl_id' in chembl['chembl']
        assert chembl['chembl']['molecule_chembl_id'] == self.chembl_id

        unii = self.json_ok(self.get_ok(self.api + '/drug/' + self.unii_id))
        assert 'unii' in unii
        assert 'unii' in unii['unii']
        assert unii['unii']['unii'] == self.unii_id

        chebi = self.json_ok(self.get_ok(self.api + '/drug/' + self.chebi_id))
        assert 'chebi' in chebi
        assert 'id' in chebi['chebi']
        assert chebi['chebi']['id'] == self.chebi_id

        res = self.json_ok(self.get_ok(self.api + '/drug/' + self.inchikey_id))
        eq_(res['_id'], self.inchikey_id)

        # testing non-ascii character
        self.get_404(self.api + '/drug/' + self.inchikey_id + '\xef\xbf\xbd\xef\xbf\xbdmouse')
        ##*************************************************##
        # testing filtering parameters
        res = self.json_ok(self.get_ok(self.api + '/drug/{}?fields=pubchem'.format(self.inchikey_id)))
        eq_(set(res), set(['_id', '_version', 'pubchem']))
        self.get_404(self.api + '/drug')
        self.get_404(self.api + '/drug/')

    def test_drug_post(self):
        res = self.json_ok(self.post_ok(self.api + '/drug', {'ids': self.inchikey_id}))
        eq_(len(res), 1)
        eq_(res[0]['_id'], self.inchikey_id)

        res = self.json_ok(self.post_ok(self.api + '/drug', {'ids': self.inchikey_id + ',RRUDCFGSUDOHDG-UHFFFAOYSA-N'}))
        eq_(len(res), 2)
        eq_(res[0]['_id'], self.inchikey_id)
        eq_(res[1]['_id'], 'RRUDCFGSUDOHDG-UHFFFAOYSA-N')

        res = self.json_ok(self.post_ok(self.api + '/drug', {'ids': self.inchikey_id + ',RRUDCFGSUDOHDG-UHFFFAOYSA-N', 'fields': 'pubchem'}))
        eq_(len(res), 2)
        for _g in res:
            eq_(set(_g), set(['_id', 'query', 'pubchem']))

        # Test a large drug post
        # # too slow
        # res = self.json_ok(self.post_ok(self.api + '/drug', {'ids': DRUG_POST_LIST}))
        # eq_(len(res), 999)

    def test_metadata(self):
        self.get_ok(self.api + '/metadata')

    def test_query_facets(self):
        res = self.json_ok(self.get_ok(self.api + '/query?q=drugbank.name:acid&size=0&facets=drugbank.weight.average'))
        assert 'facets' in res and 'drugbank.weight.average' in res['facets']

    def test_unicode(self):
        s = '基因'

        self.get_404(self.api + '/drug/' + s)

        res = self.json_ok(self.post_ok(self.api + '/drug', {'ids': s}))
        eq_(res[0]['notfound'], True)
        eq_(len(res), 1)
        res = self.json_ok(self.post_ok(self.api + '/drug', {'ids': self.inchikey_id + ',' + s}))
        eq_(res[1]['notfound'], True)
        eq_(len(res), 2)

        res = self.json_ok(self.get_ok(self.api + '/query?q=' + s))
        eq_(res['hits'], [])

        res = self.json_ok(self.post_ok(self.api + '/query', {"q": s, "scopes": 'drugbank.drugbank_id'}))
        eq_(res[0]['notfound'], True)
        eq_(len(res), 1)

        res = self.json_ok(self.post_ok(self.api + '/query', {"q": self.drugbank_id + '+' + s, 'scopes': 'drugbank.drugbank_id'}))
        eq_(res[1]['notfound'], True)
        eq_(len(res), 2)

    def test_get_fields(self):
        res = self.json_ok(self.get_ok(self.api + '/metadata/fields'))
        # Check to see if there are enough keys
        ok_(len(res) > 490)

        # Check some specific keys
        assert 'drugbank' in res
        assert 'pubchem' in res
        assert 'ginas' in res
        assert 'aeolus' in res
        assert 'drugcentral' in res

    def test_fetch_all(self):
        q = 'drugbank.name:acid&fields=drugbank.name&fetch_all=TRUE'
        res = self.json_ok(self.get_ok(self.api + '/query?q=' + q))
        assert '_scroll_id' in res
        assert 'hits' in res

        # get one set of results
        res2 = self.json_ok(self.get_ok(self.api + '/query?scroll_id=' + res['_scroll_id']))
        assert 'hits' in res2

    def test_msgpack(self):
        res = self.json_ok(self.get_ok(self.api + '/drug/' + self.inchikey_id))
        res2 = self.msgpack_ok(self.get_ok(self.api + '/drug/{}?msgpack=true'.format(self.inchikey_id)))
        ok_(res, res2)

        res = self.json_ok(self.get_ok(self.api + '/query?q=drugbank.drugbank_id:{}&size=1'.format(self.drugbank_id)))
        res2 = self.msgpack_ok(self.get_ok(self.api + '/query?q=drugbank.drugbank_id:{}&size=1&msgpack=true'.format(self.drugbank_id)))
        ok_(res, res2)

        res = self.json_ok(self.get_ok(self.api + '/metadata'))
        res2 = self.msgpack_ok(self.get_ok(self.api + '/metadata?msgpack=true'))
        ok_(res, res2)

    def test_licenses(self):
        # cadd license
        res = self.json_ok(self.get_ok(self.api + '/drug/' + self.inchikey_id))
        if 'aeolus' in res:
            assert '_license' in res['aeolus']
            assert res['aeolus']['_license']
        if 'chebi' in res:
            assert '_license' in res['chebi']
            assert res['chebi']['_license']
        if 'chembl' in res:
            assert '_license' in res['chembl']
            assert res['chembl']['_license']
        if 'drugbank' in res:
            assert '_license' in res['drugbank']
            assert res['drugbank']['_license']
        if 'drugcentral' in res:
            assert '_license' in res['drugcentral']
            assert res['drugcentral']['_license']
        if 'ginas' in res:
            assert '_license' in res['ginas']
            assert res['ginas']['_license']
        if 'pubchem' in res:
            assert '_license' in res['pubchem']
            assert res['pubchem']['_license']

    def test_jsonld(self):
        pass

    def test_status_endpoint(self):
        self.get_ok(self.host + '/status')
        # (testing failing status would require actually loading tornado app from there
        #  and deal with config params...)

