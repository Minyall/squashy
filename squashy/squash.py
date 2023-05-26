from typing import List, Dict

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
        db_address: str, default='localhost'
            Address of the Memgraph database
        db_port: int, default=7687
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
        """
        Wipes all core graph metrics and assignments from the database ready to re-run compression.

        Returns
        -------
        None
        """
        self.meta_relator.reset()
        self.agglomerator.reset()
        self.decomposer.reset()

    def squash_graph(self, max_cores: int = 500, k: int = 2, min_hops: int = None, max_hops: int = None):
        """

        Parameters
        ----------
        max_cores : int, optional
            Maximum number of core nodes to designate (default is 500)
        k : int, optional
            Value of k used for k-core decomposition during core identification (default is 2)
        min_hops : int, optional
            Minimum number of hops to make from each core node when determining which nodes it represents.
            Overrides any parameters passed to a custom GraphAggolmerator passed via __init__.
        max_hops : int, optional
            Maximum number of hops to make from each core node when determining which nodes it represents.
            Overrides any parameters passed to a custom GraphAggolmerator passed via __init__.

        Returns
        -------
        None

        """
        self.decomposer.max_cores = max_cores
        self.decomposer.k = k
        if min_hops is not None:
            self.agglomerator.set_minimum_hop(min_hops)
        if max_hops is not None:
            self.agglomerator.set_maximum_hop(max_hops)
        self.decomposer.identify_core_nodes()
        self.agglomerator.agglomerate()
        self.meta_relator.build_meta_relations()

    def get_core_edge_list(self) -> List[Dict]:
        """
        Returns a list of core edges as dictionaries, including associated edge attributes.

        Equivalent to MetaRelate.get_core_edge_list()

        Examples
        --------
        squasher = Squash('NODE', 'RELATION')
        squasher.get_core_edge_list()
        >>>> [{'source': 'fred', 'target':'joan', 'weight': 6, 'n_distinct':4, 'score': 0.24}...]


        Returns
        -------
        list
            List containing dictionary records.



        """
        return self.meta_relator.get_core_edge_list()

    def get_core_node_list(self):
        return self.meta_relator.get_core_node_list()
