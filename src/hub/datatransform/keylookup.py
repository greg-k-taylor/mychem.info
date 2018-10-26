from biothings.hub.datatransform import MongoDBEdge, RegExEdge, DataTransformMDB
import networkx as nx

graph_mychem = nx.DiGraph()

###############################################################################
# PharmGKB Nodes and Edges
###############################################################################
graph_mychem.add_node('inchi')
graph_mychem.add_node('chembl')
graph_mychem.add_node('drugbank')
graph_mychem.add_node('drugname')
graph_mychem.add_node('pubchem')
graph_mychem.add_node('rxnorm')
graph_mychem.add_node('unii')
graph_mychem.add_node('inchikey')
graph_mychem.add_node('pharmgkb')

graph_mychem.add_edge('inchi', 'drugbank',
                      object=MongoDBEdge('drugbank', 'drugbank.inchi', 'drugbank.drugbank_id'))

graph_mychem.add_edge('inchi', 'chembl',
                      object=MongoDBEdge('chembl', 'chembl.inchi', 'chembl.molecule_chembl_id'))

graph_mychem.add_edge('inchi', 'pubchem',
                      object=MongoDBEdge('pubchem', 'pubchem.inchi', 'pubchem.cid'))

graph_mychem.add_edge('chembl', 'inchikey',
                      object=MongoDBEdge('chembl', 'chembl.molecule_chembl_id', 'chembl.inchi_key'))

graph_mychem.add_edge('drugbank', 'inchikey',
                      object=MongoDBEdge('drugbank', 'drugbank.drugbank_id', 'drugbank.inchi_key'))

graph_mychem.add_edge('pubchem', 'inchikey',
                      object=MongoDBEdge('pubchem', 'pubchem.cid', 'pubchem.inchi_key'))

graph_mychem.add_edge('pharmgkb', 'drugbank',
                      object=MongoDBEdge('pharmgkb', 'pharmgkb.id', 'pharmgkb.xref.drugbank'))

# self-loops to check looked-up values exist in official collection
graph_mychem.add_edge('drugbank', 'drugbank',
                      object=MongoDBEdge('drugbank', 'drugbank.drugbank_id', 'drugbank.drugbank_id'))

###############################################################################
# NDC Nodes and Edges
###############################################################################
# ndc -> drugbank -> inchikey
# shortcut edge, one lookup for ndc to inchikey by way of drugbank
graph_mychem.add_node('ndc')

graph_mychem.add_edge('ndc', 'inchikey',
                      object=MongoDBEdge('drugbank', 'drugbank.products.ndc_product_code', 'drugbank.inchi_key'))

###############################################################################
# Chebi Nodes and Edges
###############################################################################
# chebi -> drugbank -> inchikey
# chebi -> chembl -> inchikey
graph_mychem.add_node('chebi')
#graph_mychem.add_node('chebi-short')

#graph_mychem.add_edge('chebi', 'chebi-short',
#                      object=RegExEdge('^CHEBI:', ''))
#graph_mychem.add_edge('chebi-short', 'chebi',
#                      object=RegExEdge('^', 'CHEBI:'))
#graph_mychem.add_edge('chebi-short', 'drugbank',
#                      object=MongoDBEdge('drugbank', 'drugbank.chebi', 'drugbank.drugbank_id'))
#graph_mychem.add_edge('chebi-short', 'chembl',
#                      object=MongoDBEdge('chembl', 'chembl.chebi_par_id', 'chembl.molecule_chembl_id'))


class MyChemKeyLookup(DataTransformMDB):

    def __init__(self, input_types, *args, **kwargs):
        super(MyChemKeyLookup, self).__init__(graph_mychem,
                input_types,
                output_types=['inchikey', 'unii', 'rxnorm', 'drugbank',
                              'chebi', 'chembl', 'pubchem', 'drugname'],
                *args, **kwargs)

