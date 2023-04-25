import random
from typing import List, Dict

from tqdm.auto import tqdm
from mini_memgraph import Memgraph
from squashy.metrics import AgglomeratorMetrics

# TODO make resumable similar to decomposition to allow a more nuanced expansion of representation
# and the ability of users to experiment with parameters.


class GraphAgglomerator:
    degree_label = 'agglom_degree'
    final_assignments: Dict
    _minimum_degree = None
    degree_attr_exists: bool
    _node_label = None
    _rel_label = None
    _core_label = 'CORE'
    _represents_label = 'REPRESENTS'
    _hops = (1, 3)

    def __init__(self, database: Memgraph, node_label: str,
                 rel_label: str, core_node_label: str = 'CORE', weight:str=None,
                 orientation: str = 'undirected', min_hops: int = 1, max_hops: int = 3):

        self.database = database
        self.metrics = AgglomeratorMetrics(self.database)
        self.set_node_label(node_label)
        self.set_rel_label(rel_label)
        self.set_core_label(core_node_label)
        self.weight = weight
        self.set_hop_range(min_hops=min_hops, max_hops=max_hops)
        self.current_hop = 0

        self.cores = [r['id'] for r in self.database.read(f'MATCH (c:{self._core_label}) RETURN c.id AS id')]
        self.final_assignments = {c: c for c in self.cores}
        self.final_assignments = self._reshape_assignments(self.final_assignments)
        self.final_assignments = self._add_distance(self.final_assignments)
        self._save_assignments()

        self.orientation = orientation
        self._left_endpoint = '-'
        self._right_endpoint = '-'
        if orientation == 'in':
            self._left_endpoint = '<-'
        elif orientation == 'out':
            self._right_endpoint = '->'

        self.total_nodes = self.calculate_graph_size()
        self._progress_tracker = set()

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
        hop_range = (1, max_hops)
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
        self.metrics.n_assigned = len(self._progress_tracker)
        self.metrics.ratio = round(self.metrics.n_assigned / self.total_nodes, 4)

    def agglomerate(self):
        current_assignments = {}
        hop_options = list(range(self._hops[0], self._hops[1]+1))
        n_hops = len(hop_options)
        with tqdm(total=len(self.cores)*n_hops) as bar:
            for hop in hop_options:
                self.current_hop = hop
                for i, core in enumerate(self.cores, start=1):
                    self._update_metrics()
                    self.metrics.hop = hop
                    self.metrics.n_cores = i

                    report = self.metrics.report()
                    bar.set_description(f'Core {i}/{len(self.cores)} | Hop Distance:{hop} | {str(report)}')

                    self.metrics.start_timer()
                    caught_nodes = self._find_represented_nodes(core, min_hops=hop, max_hops=hop)
                    id_list = [node['id'] for node in caught_nodes]
                    self._track_assignments(id_list)
                    current_assignments = self._organize_assignments(core, current_assignments, caught_nodes)
                    self.metrics.stop_timer()
                    self.metrics.new_record()
                    bar.update(1)

                current_assignments = self._deduplicate_assignments(current_assignments)
                current_assignments = self._select_closest_core(current_assignments)
                current_assignments = self._reshape_assignments(current_assignments)
                current_assignments = self._add_distance(current_assignments)
                self.final_assignments.update(current_assignments)

                self._save_assignments()

        return report

    def _save_assignments(self):
        edge_list = [{'target': node, 'source': data['core'], 'distance': data['distance']} for node, data in self.final_assignments.items() if data['distance'] == self.current_hop]
        self.database.write_edges(edge_list,
                                  source_label=self._core_label,
                                  edge_label=self._represents_label,
                                  target_label=self._node_label,
                                  add_attributes=['distance'])

    def reset(self):
        self.database.wipe_relationships(self._represents_label)
        self.metrics = self.metrics.reset_metrics()

    def _calculate_degree(self, force=False):
        if not self.degree_attr_exists or force:
            self.database.set_degree(self._node_label, self._rel_label, set_property=self.degree_label, orientation=self.orientation)
            self.degree_attr_exists = True

    def _find_represented_nodes(self, core_id: int, min_hops:int=1, max_hops:int=6) -> List[Dict]:
        if self.minimum_degree is not None:
            where_min_degree = f"WHERE u.{self.degree_label} >= {self.minimum_degree}"
            path_degree_limiter = f' (r, u | u.{self.degree_label} >= {self.minimum_degree})'
        else:
            where_min_degree = ""
            path_degree_limiter = ""

        match = f"MATCH p=(c:{self._core_label} {{id:$id_val}})" \
                f"{self._left_endpoint}[r:{self._rel_label} *{min_hops}..{max_hops}{path_degree_limiter}]{self._right_endpoint}" \
                f"(:{self._node_label})"
        with_ = "WITH last(nodes(p)) AS end_node"
        ret = "RETURN end_node.id AS id"

        if self.weight is not None:
            with_ = with_ + f", reduce(total_weight=0, n IN relationships(p) | total_weight + n.{self.weight}) AS total_weight "
            ret = ret + ", total_weight AS path_weight"

            query = ' '.join([match, where_min_degree, with_, ret])
            node_data = self.database.read(query, id_val=core_id)
        else:
            query = ' '.join([match, where_min_degree, with_, ret])
            result = self.database.read(query, id_val=core_id)
            node_data = [{'id':res['id'], 'path_weight':0} for res in result]

        return node_data

    def _organize_assignments(self, core_id: int, assignments: Dict[int, Dict], nodes_to_organize: List[Dict]) -> Dict[int, Dict]:
        for node in nodes_to_organize:
            if node['id'] not in assignments:
                assignments[node['id']] = {core_id: node['path_weight']}
            else:
                assignments[node['id']].update({core_id: node['path_weight']})
        return assignments

    def _deduplicate_assignments(self, assignments: Dict) -> Dict:
        return {node: cores for node, cores in assignments.items() if node not in self.final_assignments}

    def _select_closest_core(self, assignments: Dict) -> Dict:
        return {node: max(cores, key=cores.get) for node, cores in assignments.items()}

    def _reshape_assignments(self, assignments: Dict) -> Dict:
        return {node: dict(core=core) for node, core in assignments.items()}

    def _add_distance(self, assignments: Dict) -> Dict:
        for _, data in assignments.items():
            data.update(dict(distance=self.current_hop))
        return assignments

    def _track_assignments(self, node_ids: List) -> None:
        self._progress_tracker.update(node_ids)

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
