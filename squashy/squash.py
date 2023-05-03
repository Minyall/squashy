from mini_memgraph import Memgraph

from squashy.agglomeration import GraphAgglomerator, MetaRelate
from squashy.decomposition import KCoreIdentifier


class Squash:
    def __init__(self, node_label: str, relation_label: str, weight_label: str = None, db_address: str = 'localhost',
                 db_port: int = 7687, decomposer: KCoreIdentifier = None,
                 agglomerator: GraphAgglomerator = None, meta_relator: MetaRelate = None):
        self.db = Memgraph(address=db_address, port=db_port)
        self.decomposer = decomposer if not None else KCoreIdentifier(self.db, node_label, relation_label)
        self.agglomerator = agglomerator if not None else GraphAgglomerator(self.db, node_label,
                                                                            relation_label,
                                                                            weight=weight_label)

        self.meta_relator = meta_relator if not None else MetaRelate(self.db, node_label, relation_label,
                                                                     weight=weight_label)

    def reset(self):
        self.meta_relator.reset()
        self.agglomerator.reset()
        self.decomposer.reset()

    def squash(self, max_cores: int = 500, k: int = 2, min_hops: int = None, max_hops: int = None):
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
