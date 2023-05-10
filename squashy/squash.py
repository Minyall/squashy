from mini_memgraph import Memgraph

from squashy.agglomeration import GraphAgglomerator, MetaRelate
from squashy.decomposition import KCoreIdentifier


class Squash:
    """
    A class for handling the full process of graph compression using primarily default values.
    If the Memgraph Database already contains a compressed graph this class allows access
    to the core graph as well as associated metrics via the individual stages of compression (see attributes).

    ...

    Attributes
    ----------
    decomposer : KCoreIdentifier
        The KCoreIdentifier used in the compression process. Use to access metrics and associated attributes.
    aggolmerator : GraphAgglomerator
        The GraphAgglomerator used in the compression process. Use to access metrics and associated attributes.
    meta_relator : MetaRelate
        The MetaRelate class used in the compression process. Use to access metrics and associated attributes.

    Methods
    ----------
    squash_graph(max_cores=500, k=2, min_hops=None, max_hops=None)
        Generates the compressed core graph. Steps through each stage of core identification,
         assignment of representatives and the generation of meta-relations.

    get_core_edge_list()
        Returns a list of dictionary edges of format {source, target, **weight values}.
        Equivalent to MetaRelate.get_core_edge_list()

    get_core_node_list()
        Returns a list of dictionary node records of format {id, n_subnodes}
        where n_subnodes is the number of node_label nodes represented by each core node.
        Equivalent to MetaRelate.get_core_node_list()
    reset()
        Wipes all core graph metrics and assignments from the database ready to re-run compression.

    """

    decomposer: KCoreIdentifier
    agglomerator: GraphAgglomerator
    meta_relator: MetaRelate

    def __init__(self, node_label: str, relation_label: str, weight_label: str = None, db_address: str = 'localhost',
                 db_port: int = 7687, decomposer: KCoreIdentifier = None,
                 agglomerator: GraphAgglomerator = None, meta_relator: MetaRelate = None):
        """

        Parameters
        ----------
        node_label : str
            The label of the nodes to be compressed.
        relation_label : str
            The label of the relations to be compressed.
        weight_label: str, optional
            The label of the attribute containing a weight value. Use this if your raw graph is weighted.
        db_address: str, optional
            Address of the Memgraph database (default is 'localhost')
        db_port: int, optional
            Port number of the Memgraph database.
        decomposer: KCoreIdentifier, optional
            To use non-default settings pass in a custom instance of KCoreIdentifier.
        agglomerator: GraphAgglomerator, optional
            To use non-default settings pass in a custom instance of GraphAgglomerator.
        meta_relator: MetaRelate, optional
            To use non-default settings pass in a custom instance of MetaRelate.
        """

        self.db = Memgraph(address=db_address, port=db_port)

        self.decomposer = decomposer
        self.agglomerator = agglomerator
        self.meta_relator = meta_relator

        if self.decomposer is None:
            self.decomposer = KCoreIdentifier(self.db, node_label, relation_label)
        if self.agglomerator is None:
            self.agglomerator = GraphAgglomerator(self.db, node_label,
                                                  relation_label,
                                                  weight=weight_label)
        if self.meta_relator is None:
            self.meta_relator = MetaRelate(self.db, node_label, relation_label,
                                           weight=weight_label)

    def reset(self):
        self.meta_relator.reset()
        self.agglomerator.reset()
        self.decomposer.reset()

    def squash_graph(self, max_cores: int = 500, k: int = 2, min_hops: int = None, max_hops: int = None):
        self.decomposer.max_cores = max_cores
        self.decomposer.k = k
        if min_hops is not None:
            self.agglomerator.set_minimum_hop(min_hops)
        if max_hops is not None:
            self.agglomerator.set_maximum_hop(max_hops)
        self.decomposer.identify_core_nodes()
        self.agglomerator.agglomerate()
        self.meta_relator.build_meta_relations()

    def get_core_edge_list(self):
        return self.meta_relator.get_core_edge_list()

    def get_core_node_list(self):
        return self.meta_relator.get_core_node_list()
