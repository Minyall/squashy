from tqdm.auto import tqdm

from mini_memgraph import Memgraph
from squashy.metrics import DecomposerMetrics


class KCoreIdentifier:
    def __init__(self, database: Memgraph, node_label: str, rel_label: str, core_label: str = 'CORE',
                 target_label: str = None, metrics_path=None, k: int = 2, max_cores: int = 500,
                 filter_attr: str = 'decomposed', degree_attr: str = 'decomp_degree', orientation='undirected'):

        self.database = database
        self.k = k
        self.max_cores = max_cores
        self.metrics = DecomposerMetrics(self.database)
        self.filter_attr = filter_attr
        self.calc_degree_label = 'calc_degree'
        self.degree_label = degree_attr
        self.orientation = orientation

        self.node_label = node_label
        self.rel_label = rel_label
        self.core_label = core_label
        self.target_label = target_label

        self.not_decomposed = f"WHERE n.{self.filter_attr} IS NULL"
        self.degree_filter = f"WHERE s.{self.filter_attr} IS NULL AND t.{self.filter_attr} IS NULL"
        self.database.set_index('CORE')
        self.database.set_index('CORE', 'id')
        self._graph_size = self.database.node_count(self.node_label)
        self.metrics.graph_size = self._graph_size
        # self.metrics.cores_identified = self.database.node_count(self.core_label)

    def reset_cores(self):
        self.database.remove_node_label(self.core_label)
        if not self.database.node_count(self.core_label) == 0:
            raise Exception(f'Error - Label {self.core_label} still present after reset.')

    def reset_assignments(self):
        for label in (self.filter_attr, self.degree_label, self.calc_degree_label):
            self.database.remove_node_attr(self.node_label, label)
            if not self.database.node_count(self.node_label, where=f'WHERE n.{label} IS NOT NULL') == 0:
                raise Exception(f'Error - Filter attribute {label} still present after reset.')

    def reset(self):
        self.reset_assignments()
        self.reset_cores()
        self.metrics = self.metrics.reset_metrics()
        self.metrics.graph_size = self._graph_size


    def _prune(self) -> int:
        match_q = f"MATCH (n:{self.node_label}) WHERE n.{self.degree_label} < {self.k}"
        set_q = f"SET n.{self.filter_attr} = true"
        return_q = "RETURN count(n) AS total"
        query = ' '.join([match_q, set_q, return_q])
        res = self.database.write(query)
        return res[0]['total']

    def _register_core(self):
        register_core_query = f"MATCH (n:{self.node_label}) {self.not_decomposed} " \
                              f"WITH max(n.{self.degree_label}) as max_degree " \
                              f"MATCH (new_core:{self.node_label}) " \
                              f"WHERE new_core.{self.degree_label} = max_degree AND new_core.{self.filter_attr} IS NULL " \
                              f"WITH new_core LIMIT 1 " \
                              f"SET new_core:{self.node_label}:{self.core_label}, new_core.{self.filter_attr} = true " \
                              f"RETURN new_core.id, new_core.{self.degree_label}, new_core.{self.filter_attr}"
        self.database.write(register_core_query)
        self.metrics.cores_identified += 1

    def _update_min_max_degree(self):
        self.metrics.min_degree = self.database.attr_minimum(self.node_label, self.degree_label,
                                                             where=self.not_decomposed)
        self.metrics.max_degree = self.database.attr_maximum(self.node_label, self.degree_label,
                                                             where=self.not_decomposed)

    def _update_n_remaining(self) -> int:
        self.metrics.n_remaining = self.database.node_count(label=self.node_label, where=self.not_decomposed)

    def _calculate_degree_scores(self) -> int:
        n_nodes_updated = self.database.set_degree(node_label=self.node_label, rel_label=self.rel_label,
                                                   target_label=self.target_label,
                                                   where=self.degree_filter, set_property=self.degree_label,
                                                   orientation=self.orientation)
        return n_nodes_updated

    def _run_decomposition_pass(self):
        self._calculate_degree_scores()
        self._update_n_remaining()
        self._update_min_max_degree()

    def get_average_degree(self):
        if not self.database.attr_exists(self.node_label,self.calc_degree_label):
            self.database.set_degree(node_label=self.node_label, rel_label=self.rel_label,
                                     target_label=self.target_label,set_property=self.calc_degree_label,
                                     orientation=self.orientation)
        avg_degree_query = f'MATCH (n:{self.node_label}) RETURN avg(n.{self.calc_degree_label}) AS avg_degree'
        return self.database.read(avg_degree_query)[0]['avg_degree']

    def identify_core_nodes(self):

        self._run_decomposition_pass()

        with tqdm(total=self.max_cores,
                  # bar_format='{desc}:{bar} {elapsed}<{remaining}',
                  desc=self.metrics.report()) as bar:
            bar.update(self.metrics.cores_identified)
            while (self.metrics.min_degree < self.k) and (self.metrics.cores_identified < self.max_cores):
                self.metrics.start_timer()
                self._prune()
                if self.metrics.n_remaining < 1:
                    break

                self._run_decomposition_pass()
                bar.set_description(self.metrics.report())

                while (self.metrics.min_degree >= self.k) and (self.metrics.cores_identified < self.max_cores):

                    self._register_core()

                    self._run_decomposition_pass()
                    bar.update(1)
                    bar.set_description(self.metrics.report())

                    if self.metrics.min_degree is None:
                        break
                if self.metrics.min_degree is None:
                    break
                self.metrics.stop_timer()
                self.metrics.new_record()

        self.metrics.report('Finished')
