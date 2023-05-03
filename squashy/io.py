from mini_memgraph import Memgraph
from typing import List, Tuple

class DataImporter:
    def __init__(self, database: Memgraph = None, node_label='NODE', edge_label='REL', weight_label: str = 'weight', address: str = 'locahost', port: int = 7687):
        if database is None:
            self.db = Memgraph(address, port)
        else:
            self.db = database
        self.set_node_label(node_label)
        self.set_edge_label(edge_label)
        self.weight_label = weight_label

    def _prep_node_list(self, nodes: List):
        return [{id:n} for n in nodes]

    def _prep_edge_list(self, edges: List):
        if self.weight_label is not None:
            prepped = [{'source': record[0],
                 'target':record[1],
                 self.weight_label:record[2]} for record in edges]
        else:
            prepped = [{'source': record[0],
                 'target':record[1]} for record in edges]

    def load_nodes(self, node_list: List):
        data = self._prep_node_list(node_list)
        self.db.write_nodes(data, label=self.node_label, id_val='id')

    def load_edges(self, edge_list: List[Tuple], weight_label:):
        if not isinstance(edge_list,list) or not isinstance(edge_list[0], tuple) or not len(edge_list[0]) > 1:
            raise TypeError('edge_list must be a list of Tuples of len 2 or 3 if including weight')
        data = self._prep_edge_list(edge_list)
        self.db.write_edges(data, label=self.node_label, edge_label=self.edge_label, on_duplicate_edges='increment', add_attributes=[self.weight_label], id_val='id')

    def _set_label(self, label:str, attr:str):
        if not label.isupper()
            raise Exception(f'Node or edge labels should be UPPERCASE. {label=}')
        setattr(self,attr,label)

    def set_node_label(self, label:str):
        self._set_label(label, '_node_label')

    def set_edge_label(self, label: str):
        self._set_label(label, '_edge_label')

    def set_node_label(self, label:str):
        self._set_label(label, '_edge_label')

    def load_from_edge_list(self, edge_tuples: List[Tuple], node_label, edge_label, weight_label):
        node_list = set()
        node_list.update([record[0] for record in edge_tuples])
        node_list.update([record[1] for record in edge_tuples])
        node_list = list(node_list)
        node_data = self._prep_node_list(node_list)

        self.db.write_nodes(node_list=)

    @property
    def node_label(self):
        return self._node_label

    @property
    def edge_label(self):
        return self._edge_label

