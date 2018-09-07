from biothings.hub.datatransform import MongoDBEdge, RegExEdge, MyChemInfoEdge, MyGeneInfoEdge
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

graph_mychem.add_edge('inchi', 'drugbank',
                      object=MongoDBEdge('drugbank', 'drugbank.inchi', 'drugbank.drugbank_id', weight=0.1))

graph_mychem.add_edge('inchi', 'chembl',
                      object=MongoDBEdge('chembl', 'chembl.inchi', 'chembl.molecule_chembl_id', weight=0.2))

graph_mychem.add_edge('inchi', 'pubchem',
                      object=MongoDBEdge('pubchem', 'pubchem.inchi', 'pubchem.cid', weight=1.0))

graph_mychem.add_edge('chembl', 'inchikey',
                      object=MongoDBEdge('chembl', 'chembl.molecule_chembl_id', 'chembl.inchi_key', weight=0.2))

graph_mychem.add_edge('drugbank', 'inchikey',
                      object=MongoDBEdge('drugbank', 'drugbank.drugbank_id', 'drugbank.inchi_key', weight=0.1))

graph_mychem.add_edge('pubchem', 'inchikey',
                      object=MongoDBEdge('pubchem', 'pubchem.cid', 'pubchem.inchi_key', weight=0.1))

###############################################################################
# Sider Nodes and Edges
###############################################################################
# pubchem -> inchikey
# specified in pharmgkb

###############################################################################
# NDC Nodes and Edges
###############################################################################
# ndc -> drugbank -> inchikey
# shortcut edge, one lookup for ndc to inchikey by way of drugbank
graph_mychem.add_node('ndc')

graph_mychem.add_edge('ndc', 'inchikey',
                      object=MongoDBEdge('drugbank', 'drugbank.products.ndc_product_code', 'drugbank.inchi_key', weight=0.1))

###############################################################################
# Chebi Nodes and Edges
###############################################################################
# chebi -> drugbank -> inchikey
# chebi -> chembl -> inchikey
graph_mychem.add_node('chebi')
graph_mychem.add_node('chebi-short')

graph_mychem.add_edge('chebi', 'chebi-short',
                      object=RegExEdge('^CHEBI:', ''))
graph_mychem.add_edge('chebi-short', 'chebi',
                      object=RegExEdge('^', 'CHEBI:'))
graph_mychem.add_edge('chebi-short', 'drugbank',
                      object=MongoDBEdge('drugbank', 'drugbank.chebi', 'drugbank.drugbank_id'))
graph_mychem.add_edge('chebi-short', 'chembl',
                      object=MongoDBEdge('chembl', 'chembl.chebi_par_id', 'chembl.molecule_chembl_id'))

