import random
from typing import List, Dict

from tqdm.auto import tqdm
from mini_memgraph import Memgraph
from squashy.metrics import AgglomeratorMetrics

# TODO make resumable similar to decomposition to allow a more nuanced expansion of representation
# and the ability of users to experiment with parameters.


class GraphAgglomerator:
    degree_label = 'agglom_degree'
    final_assignments: Dict[int, int]
    _minimum_degree = None
    degree_attr_exists: bool
    _node_label = None
    _rel_label = None
    _core_label = 'CORE'
    _represents_label = 'REPRESENTS'
    _hops = (1, 1)

    def __init__(self, database: Memgraph, node_label: str, rel_label: str, core_node_label: str = 'CORE', orientation:str='undirected'):

        self.database = database
        self.metrics = AgglomeratorMetrics(self.database)
        self.set_node_label(node_label)
        self.set_rel_label(rel_label)
        self.set_core_label(core_node_label)
        self.cores = [r['id'] for r in self.database.read(f'MATCH (c:{self._core_label}) RETURN c.id AS id')]
        self.final_assignments = {c: c for c in self.cores}
        self.orientation = orientation
        self._left_endpoint = '-'
        self._right_endpoint = '-'
        if orientation == 'in':
            self._left_endpoint = '<-'
        elif orientation == 'out':
            self._right_endpoint = '->'

        self.total_nodes = self.calculate_graph_size()

    def _set_label(self, attr: str, label: str):
        if not isinstance(label, str):
            raise ValueError
        setattr(self, attr, label)

    def _check_label(self, label: str):
        if not self.database.label_exists(label):
            raise ValueError(f'No {label} nodes identified.')

    def set_core_label(self, label: str):
        self._check_label(label)
        self._set_label('_core_label', label)

    def set_represents_label(self, label: str):
        self._set_label('_represents_label', label)

    def set_node_label(self, label: str):
        self._check_label(label)
        self._set_label('_node_label', label)
        self.degree_attr_exists = self.database.attr_exists(
            self._node_label, self.degree_label)

    def set_rel_label(self, label: str):
        self._set_label('_rel_label', label)

    def describe(self):
        print(f'To traverse: ({self._node_label}){self._left_endpoint}[{self._rel_label}]{self._right_endpoint}({self._node_label})')
        if self._minimum_degree is not None:
            print(f'({self._node_label}) must have at least {self._minimum_degree} {self._rel_label} connections.')
        print(f"To create: ({self._core_label})-[{self.represents_label}]->({self._node_label})")

    def set_minimum_degree(self, degree: int, recalculate=False):
        self._minimum_degree = degree
        self._calculate_degree(recalculate)

    def set_hop_range(self, max_hops: int, min_hops: int = None):
        hop_range = (max_hops, max_hops)
        if min_hops is not None:
            hop_range = (min_hops, max_hops)
        if not all(isinstance(x, int) for x in hop_range):
            raise TypeError(f'Any passed argument should be an integer - {hop_range=}')
        self._hops = hop_range

    def calculate_graph_size(self):
        if self.minimum_degree is not None:
            self._calculate_degree()
            total_nodes_where = f"WHERE u.{self.degree_label} >= {self.minimum_degree}"
        else:
            total_nodes_where = ""
        total_nodes_match = f'MATCH (u:{self._node_label})-[:{self._rel_label}]-(:{self._node_label})'
        total_nodes_with = f'WITH DISTINCT u'
        total_nodes_return = 'RETURN count(u) AS n_nodes'
        total_nodes_query = ' '.join([total_nodes_match, total_nodes_where, total_nodes_with, total_nodes_return])
        n_total_nodes = self.database.read(total_nodes_query)[0]['n_nodes']
        return n_total_nodes

    def _update_metrics(self):
        self.metrics.graph_size = self.total_nodes
        self.metrics.n_assigned = len(self.final_assignments)
        self.metrics.ratio = round(self.metrics.n_assigned / self.total_nodes, 4)

    def agglomerate(self):
        current_assignments = {}
        hop_options = list(range(self._hops[0], self._hops[1]+1))
        n_hops = len(hop_options)
        with tqdm(total=len(self.cores)*n_hops) as bar:
            for hop in hop_options:
                for i, core in enumerate(self.cores, start=1):
                    self._update_metrics()
                    self.metrics.hop = hop
                    self.metrics.n_cores = i

                    report = self.metrics.report()
                    bar.set_description(f'Core {i}/{len(self.cores)} | Hop Distance:{hop} | {str(report)}')

                    self.metrics.start_timer()
                    caught_nodes = self._find_represented_nodes(core, min_hops=hop, max_hops=hop)
                    current_assignments = self._organize_assignments(core, current_assignments, caught_nodes)
                    self._merge_assignments(current_assignments)
                    self.metrics.stop_timer()
                    self.metrics.new_record()
                    bar.update(1)

        return report

    def save_assignments(self):
        edge_list = [{'target': node, 'source': core} for node, core in self.final_assignments.items()]
        self.database.write_edges(edge_list,
                                  source_label=self._core_label,
                                  edge_label=self._represents_label,
                                  target_label=self._node_label)

    def reset(self):
        self.database.wipe_relationships(self._represents_label)
        self.metrics = self.metrics.reset_metrics()

    def _calculate_degree(self, force=False):
        if not self.degree_attr_exists or force:
            self.database.set_degree(self._node_label, self._rel_label, set_property=self.degree_label, orientation=self.orientation)
            self.degree_attr_exists = True

    def _find_represented_nodes(self, core_id: int, min_hops:int=1, max_hops:int=6):
        if self.minimum_degree is not None:
            where_min_degree = f"WHERE u.{self.degree_label} >= {self.minimum_degree}"
            path_degree_limiter = f' (r, u | u.{self.degree_label} >= {self.minimum_degree})'
        else:
            where_min_degree = ""
            path_degree_limiter = ""

        match = f"MATCH (c:{self._core_label} {{id:$id_val}})" \
                f"{self._left_endpoint}[r:{self._rel_label} *{min_hops}..{max_hops}{path_degree_limiter}]{self._right_endpoint}" \
                f"(u:{self._node_label})"
        with_ = "WITH DISTINCT u"
        ret = "RETURN u.id AS id"
        query = ' '.join([match, where_min_degree, with_, ret])
        result = self.database.read(query, id_val=core_id)
        ids = [res['id'] for res in result]

        return ids

    def _organize_assignments(self, core_id: int, assignments: Dict[int, List[int]], nodes_to_organize:List[int]) -> Dict[int, List[int]]:
        for node in nodes_to_organize:
            if node not in assignments:
                assignments[node] = [core_id]
            else:
                assignments[node].append(core_id)
        return assignments

    def _merge_assignments(self, assignments: Dict):
        to_assign = {node: cores for node, cores in assignments.items() if node not in self.final_assignments}
        # if not len(to_assign) == (len(assignments) + 1 - len(self.final_assignments)):
        #     raise Exception
        resolved_nodes = {node: random.choice(cores) if len(cores) > 1 else cores[0] for node, cores in
                          to_assign.items()}
        self.final_assignments.update(resolved_nodes)

    @property
    def core_label(self):
        return self._core_label

    @property
    def represents_label(self):
        return self._represents_label

    @property
    def node_label(self):
        return self._node_label

    @property
    def hop_range(self):
        return self._hops

    @property
    def minimum_degree(self):
        return self._minimum_degree
