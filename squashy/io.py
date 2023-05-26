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
        The name of the nodes in your dataset.
    edge_label: str
        The name of the edges in your dataset
    db: mini_memgraph.Memgraph
        Database interface. Can be accessed directly for .read() and .write() functionality with custom Cypher queries.

    """

    node_label: str
    edge_label: str

    def __init__(self, database: Memgraph = None, node_label='NODE', edge_label='REL', weight_label: str = None,
                 address: str = 'locahost', port: int = 7687, wipe_db: bool = False):

        """

        Parameters
        ----------
        database : mini_memgraph.Memgraph, default=None
            Database connector instance. Pass in to use a specific instance of a Memgraph connector.
            Leave blank to have one created upon initialisation.
        node_label : str, default='NODE'
            The name used to label the nodes in your dataset. Use this to provide a descriptive name.
            For example USER, ITEM, MESSAGE etc. Should be uppercase.
        edge_label: str, default='REL'
            The name used to label the relations between nodes in your dataset. Use this to provide a
            descriptive name that explains how nodes relate to one another.
            For example REPLIES_TO, LINKS_TO, BOUGHT_WITH etc. Should be uppercase.
        weight_label: str, optional
            The label of the attribute containing a weight value. Use this if your raw graph is weighted.
        address: str, default='localhost'
            Address of the Memgraph database
        port: int, default=7687
            Port number of the Memgraph database.
        wipe_db: bool, default=False
            Pass True to delete all data in your database before data import. DataImporter will not write to a
            pre-populated database.
        """

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
        """
        Loads a list of unique node ids into the Memgraph database.
        ...

        Parameters
        ----------
        node_list : list
            A list of node ids. Must be unique.

        Returns
        -------
        None
            No value returned
        """

        if not isinstance(node_list, list):
            raise TypeError('node_list must be a list of node ids')
        if len(node_list) > len(set(node_list)):
            raise ValueError('All ids in the node list must be unique')
        data = self._prep_node_list(node_list)
        self.db.write_nodes(data, label=self.node_label, id_val='id')

    def load_edges(self, edge_list: List[Tuple]):
        """
        Loads a list of edge tuples into the Memgraph database, with an optional weight.
        ...

        Parameters
        ----------
        edge_list : list
            A list of edge tuples structured as (source, target) or (source, target, weight)

        Returns
        -------
        None
            No value returned
        """
        if not isinstance(edge_list, list) or not isinstance(edge_list[0], tuple) or not len(edge_list[0]) > 1:
            raise TypeError('edge_list must be a list of Tuples of len 2 or 3 if including weight')
        data = self._prep_edge_list(edge_list)
        self.db.write_edges(data, source_label=self.node_label, edge_label=self.edge_label,
                            target_label=self.node_label,
                            on_duplicate_edges='increment', add_attributes=[self.weight_label],
                            source_id_label='id', target_id_label='id')

    def load_from_edge_list(self, edge_tuples: List[Tuple]):
        """
        A convenience method that loads both nodes and edges from a single edge list.
        ...

        Parameters
        ----------
        edge_tuples : list
            A list of edge tuples structured as (source, target) or (source, target, weight)

        Returns
        -------
        None
            No value returned
        """
        node_list = set()
        node_list.update([record[0] for record in edge_tuples])
        node_list.update([record[1] for record in edge_tuples])
        node_list = list(node_list)
        self.load_nodes(node_list)
        self.load_edges(edge_tuples)

    def report(self):
        """
        Reports on the number of nodes and edges currently in the database.
        ...

        Returns
        -------
        str
            A string describing the current number of nodes and edges in the database.
        """
        n_nodes = self.db.node_count(self.node_label)
        n_rels = self.db.read(f'MATCH ()-[r:{self.edge_label}]-() WITH DISTINCT r RETURN count(r) AS n_rels')[0][
            'n_rels']
        return f"Database currently has {n_nodes:,} {self.node_label} nodes, and {n_rels:,} {self.edge_label} edges."

    def clear_database(self):
        """
        Wipes the connected Memgraph instance of all data. Irreversible.
        ...

        Returns
        -------
        None
            No value returned
        """
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
