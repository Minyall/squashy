from typing import List, Dict

from tqdm.auto import tqdm
from kneed import KneeLocator
import plotly.express as px
from mini_memgraph import Memgraph
from squashy.metrics import AgglomeratorMetrics


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
    current_hop: int = 0

    def __init__(self, database: Memgraph, node_label: str,
                 rel_label: str, core_label: str = 'CORE', weight:str=None,
                 orientation: str = 'undirected', min_hops: int = 1, max_hops: int = 3):

        self.database = database
        self.metrics = AgglomeratorMetrics(self.database)
        self.set_node_label(node_label)
        self.set_rel_label(rel_label)
        self.set_core_label(core_label)
        self.weight = weight
        self._original_hop_options = (min_hops, max_hops)
        self.set_hop_range(min_hops=min_hops, max_hops=max_hops)

        self.orientation = orientation
        self._left_endpoint = '-'
        self._right_endpoint = '-'
        if orientation == 'in':
            self._left_endpoint = '<-'
        elif orientation == 'out':
            self._right_endpoint = '->'

        self.cores = [r['id'] for r in self.database.read(f'MATCH (c:{self._core_label}) RETURN c.id AS id')]
        self.total_nodes = self.calculate_graph_size()

        if self._is_resuming():
            self._resume()
        else:
            self._initialize()

    def _is_resuming(self) -> bool:
        return self.metrics.pass_ > 0

    def elegant_exit(func):
        def report_and_exit(self, *args, **kwargs):
            try:
                func(self, *args, **kwargs)
            except KeyboardInterrupt:
                print(self.metrics)
                print('Agglomeration incomplete.')
                print(f'Use .agglomerate() to restart hop {self.current_hop}.')
                print('Use .reset() to restart from the beginning')
                self.database._disconnect()
        return report_and_exit

    def _initialize(self):
        self.current_hop = 0
        self.set_hop_range(min_hops=self._original_hop_options[0],
                           max_hops=self._original_hop_options[1])
        self.final_assignments = {c: c for c in self.cores}
        self.final_assignments = self._reshape_assignments(self.final_assignments)
        self.final_assignments = self._add_distance(self.final_assignments)
        self._save_assignments()
        self._progress_tracker = set(self.cores)

    def _resume(self):
        complete = self.list_complete_hops()
        hop_options = self._get_hop_options()
        remaining_hops = [hop for hop in hop_options if hop not in complete]
        start_hop = min(remaining_hops)
        self.set_minimum_hop(start_hop)
        self.drop_incomplete_hop_rels(start_hop)
        self.drop_incomplete_hop_metrics(start_hop)
        self.final_assignments = self.load_assignments()
        self._progress_tracker = set(self.final_assignments.keys())
        self.metrics.local_pass_ = 0
        self.metrics.load_metrics()

    def list_complete_hops(self) -> List[int]:
        n_cores = len(self.cores)
        metric_node_label = self.metrics.node_label
        counts = self.database.read(f'MATCH (n:{metric_node_label}) RETURN n.hop AS hop, count(n) AS freq')
        complete = [record['hop'] for record in counts if record['freq'] == n_cores]
        return complete

    def drop_incomplete_hop_rels(self, max_hop_val: int):
        self.database.write(
            query=f'MATCH ()-[r:{self.represents_label}]-() '
                  f'WHERE r.distance >= $max_hop_val '
                  f'DELETE r',
            max_hop_val=max_hop_val
        )

    def drop_incomplete_hop_metrics(self, max_hop_val: int):
        self.database.write(query=f'MATCH (n:META:{self.metrics.node_label}) WHERE n.hop >= $max_hop_val DELETE n',
                            max_hop_val=max_hop_val)

    def load_assignments(self):
        assignments = self.database.read(
            query=f"MATCH (c:{self.core_label})-[r:{self.represents_label}]->(n:{self.node_label})"
                  " RETURN c.id AS core, n.id AS node, r.distance AS distance"
        )
        return {record['node']:dict(
            distance=record['distance'],
            core=record['core']) for record in assignments}

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

    def set_hop_range(self, max_hops: int = 3, min_hops: int = 1):
        hop_range = (min_hops, max_hops)
        if not all(isinstance(x, int) for x in hop_range):
            raise TypeError(f'Any passed argument should be an integer - {hop_range=}')
        self._hops = hop_range

    def set_minimum_hop(self, min_hop:int):
        max_hop = self._hops[1]
        self.set_hop_range(min_hops=min_hop, max_hops=max_hop)

    def set_maximum_hop(self, max_hop: int):
        min_hop = self._hops[0]
        self.set_hop_range(min_hops=min_hop, max_hops=max_hop)

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

    def _get_hop_options(self):
        return list(range(self._hops[0], self._hops[1]+1))

    @elegant_exit
    def agglomerate(self):
        if self._is_resuming():
            self._resume()
        current_assignments = {}
        hop_options = self._get_hop_options()
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
            self._calculate_n_subnodes()
        return report

    def _calculate_n_subnodes(self):
        self.database.set_degree(self.core_label, self.represents_label, self.node_label, set_property='n_subnodes',
                                 orientation='out')

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
        self._initialize()

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

# TODO add option to choose whether to score by ratio of distinct users, or simply number of distinct users.
class MetaRelate:
    _knee = None

    def __init__(self, database: Memgraph, node_label: str, rel_label:str, core_label: str = 'CORE',
                 represents_label: str = 'REPRESENTS', weight:str=None,
                 orientation: str = 'undirected'):
        self.db = database
        self.node = node_label
        self.rel = rel_label
        self.meta_rel = "META_" + rel_label
        self.core = core_label
        self.represents = represents_label
        self.weight = weight

        # May be unneeded
        self.orientation = orientation

    def _build_query(self):
        match_statement = f"MATCH (source:{self.core})-[:{self.represents}]->(n:{self.node})-[r:{self.rel}]->" \
                          f"(:{self.node})<-[:{self.represents}]-(target:{self.core})"
        no_self_loop = "WHERE source <> target"
        with_distinct = "WITH DISTINCT source, target,"
        if self.weight is None:
            aggregate_weight = "count(r) AS weight,"
            set_weight = "mr.weight = weight, "
        else:
            aggregate_weight = f'sum(r.{self.weight}) AS weight, min(r.{self.weight}) AS min_weight, max(r.{self.weight}) AS max_weight,'
            set_weight = "mr.weight = weight, mr.min_weight = min_weight, mr.max_weight = max_weight"

        count_distinct = "count(n) AS n_distinct"

        create_rel = f"MERGE (source)-[mr:{self.meta_rel}]->(target)"
        set_properties = f"ON CREATE SET mr.n_distinct = n_distinct, mr.score = ((n_distinct * 1.0) / source.n_subnodes) * weight, {set_weight}"
        return_count = "RETURN count(mr) AS n_rels"

        query = ' '.join([
            match_statement,
            no_self_loop,
            with_distinct,
            aggregate_weight,
            count_distinct,
            create_rel,
            set_properties,
            return_count
        ])
        return query

    def build_meta_relations(self):
        res = self.db.write(self.query)
        n_created = res[0]['n_rels']
        return n_created

    def get_meta_rel_weights(self, score_type: str = 'score'):
        result = self.db.read(f'MATCH ()-[r:{self.meta_rel}]-() WITH DISTINCT r RETURN r.{score_type} AS weight')
        return [record['weight'] for record in result]

    def score_ecdf(self, markers=True, ecdfnorm=None, **kwargs):
        knee = self.cutoff_score
        weights = sorted(self.get_meta_rel_weights(score_type='score'))

        fig = px.ecdf(x=weights, title=f'Weight ECDF: {self.meta_rel.title()}',
                      ecdfnorm=ecdfnorm, labels=dict(x='score'), markers=markers, **kwargs)

        y_point = len(weights) - weights[::-1].index(knee)
        fig.add_hline(y=y_point, fillcolor='green')

        return fig

    def calculate_cutoff_score(self, online=True, **kwargs):
        sorted_weights = sorted(self.get_meta_rel_weights('score'))
        kneedle = KneeLocator(range(len(sorted_weights)), sorted_weights,online=online, direction='increasing', curve='convex', **kwargs)
        self._knee = kneedle.knee_y
        return float(self._knee)

    def reset(self):
        self.db.write(f'MATCH ()-[r:{self.meta_rel}]-() DELETE r')

    def get_core_edge_list(self, unfiltered: bool = False) -> List[Dict]:
        if not self.count_meta_relations() > 0:
            raise Exception(f'No meta_relations of type {self.meta_rel} detected. Please .build_meta_relations() first.')
        match_query = f'MATCH (source:{self.core})-[r:{self.meta_rel}]->(target:{self.core})'
        where_query = 'WHERE r.score >= $cutoff'
        return_query = "RETURN source.id AS source, target.id AS target, r.weight AS weight, r.n_distinct AS n_distinct, r.score AS score"
        query_sequence = [match_query, where_query, return_query]
        if unfiltered:
            query_sequence.pop(1)
        query = ' '.join(query_sequence)
        result = self.db.read(query, cutoff=self.cutoff_score)
        return result

    def get_core_node_list(self, unfiltered: bool = False) -> List[Dict]:
        if not self.count_meta_relations() > 0:
            raise Exception(f'No meta_relations of type {self.meta_rel} detected. Please .build_meta_relations() first.')
        if unfiltered:
            match_query = f'MATCH (c:{self.core})'
        else:
            match_query = f"MATCH (c:{self.core})-[r:{self.meta_rel}]-(:{self.core}) WHERE r.score >= $cutoff WITH DISTINCT c"

        return_query = 'RETURN c.id AS id, c.n_subnodes AS n_subnodes'
        query = ' '.join([match_query, return_query])
        result = self.db.read(query, cutoff=self.cutoff_score)
        return result


    def count_meta_relations(self) -> int:
        return self.db.read(f'MATCH ()-[r:{self.meta_rel}]->() RETURN count(r) AS n_rels')[0]['n_rels']

    @property
    def query(self):
        return self._build_query()

    @property
    def cutoff_score(self):
        if self._knee is None:
            self._knee = self.calculate_cutoff_score()
        return self._knee

