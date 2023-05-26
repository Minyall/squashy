from typing import List, Tuple

from mini_memgraph import Memgraph


class DataImporter:
    """
     A class for quick insertion of basic graph data into the Memgraph backend database. A convenience class for quick
     data insertion of simple graph data.

     ...

     Attributes
     ----------
     node_label : str
        The name used to label the nodes in your dataset. Use this to provide a descriptive name. For example USER, ITEM,
        MESSAGE etc. Should be uppercase.
    edge_label: str
        The name used to label the relations between nodes in your dataset. Use this to provide a descriptive name that
        explains how nodes relate to one another. For example REPLIES_TO, LINKS_TO, BOUGHT_WITH etc. Should be uppercase.
    db: mini_memgraph.Memgraph
        Database interface. Can be accessed directly for .read() and .write() functionality with custom Cypher queries.

     """


    node_label: str
    edge_label: str

    def __init__(self, database: Memgraph = None, node_label='NODE', edge_label='REL', weight_label: str = None,
                 address: str = 'locahost', port: int = 7687, wipe_db: bool = False):
        if database is None:
            self.db = Memgraph(address, port)
        else:
            self.db = database

        if not self._db_is_empty():
            if wipe_db:
                self.clear_database()
            else:
                raise Exception(
                    f'Your Memgraph instance already has data in it. Pass wipe_db=True to clear. '
                    f'Warning this will destroy all existing data in your Memgraph instance at {address=} {port=}')

        self.node_label = self._check_label(node_label)
        self.edge_label = self._check_label(edge_label)
        self.weight_label = weight_label

        self.db.set_index(node_label)
        self.db.set_index(node_label, 'id')
        self.db.set_constraint(node_label, 'id')

    def load_nodes(self, node_list: List):
        if not isinstance(node_list, list):
            raise TypeError('node_list must be a list of node ids')
        if len(node_list) > len(set(node_list)):
            raise ValueError('All ids in the node list must be unique')
        data = self._prep_node_list(node_list)
        self.db.write_nodes(data, label=self.node_label, id_val='id')

    def load_edges(self, edge_list: List[Tuple]):
        if not isinstance(edge_list, list) or not isinstance(edge_list[0], tuple) or not len(edge_list[0]) > 1:
            raise TypeError('edge_list must be a list of Tuples of len 2 or 3 if including weight')
        data = self._prep_edge_list(edge_list)
        self.db.write_edges(data, source_label=self.node_label, edge_label=self.edge_label,
                            target_label=self.node_label,
                            on_duplicate_edges='increment', add_attributes=[self.weight_label],
                            source_id_label='id', target_id_label='id')

    def load_from_edge_list(self, edge_tuples: List[Tuple]):
        node_list = set()
        node_list.update([record[0] for record in edge_tuples])
        node_list.update([record[1] for record in edge_tuples])
        node_list = list(node_list)
        self.load_nodes(node_list)
        self.load_edges(edge_tuples)

    def report(self):
        n_nodes = self.db.node_count(self.node_label)
        n_rels = self.db.read(f'MATCH ()-[r:{self.edge_label}]-() WITH DISTINCT r RETURN count(r) AS n_rels')[0][
            'n_rels']
        return f"Database currently has {n_nodes:,} {self.node_label} nodes, and {n_rels:,} {self.edge_label} edges."

    def clear_database(self):
        self.db.write('MATCH (n) DETACH DELETE n')

    def _db_is_empty(self):
        res = self.db.read('MATCH (n) RETURN n LIMIT 1')
        return res is None

    def _prep_node_list(self, nodes: List):
        return [{'id': n} for n in nodes]

    def _prep_edge_list(self, edges: List):
        if self.weight_label is not None:
            prepped = [{'source': record[0],
                        'target': record[1],
                        self.weight_label: record[2]} for record in edges]
        else:
            prepped = [{'source': record[0],
                        'target': record[1]} for record in edges]
        return prepped

    def _check_label(self, label: str):
        if not label.isupper():
            raise Exception(f'Node or edge labels should be UPPERCASE. {label=}')
        return label

    def _set_node_label(self, label: str):
        self._set_label(label, '_node_label')
