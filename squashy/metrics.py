import time

import pandas as pd
from plotly import express as px

from mini_memgraph import Memgraph

class Metrics:
    node_label: str
    _major_fields: list
    _minor_fields: list
    pass_: int
    local_pass_ : int
    runtime: float
    start_time: float

    def __init__(self, database: Memgraph):
        self.db = database
        self._fields = self._major_fields + self._minor_fields
        self._set_default_attributes()
        self.load_metrics()

    def __repr__(self):
        field_strings = [f"{field}={getattr(self,field):,}" for field in self._major_fields]
        return ', '.join(field_strings)

    def __dir__(self):
        return super().__dir__() + [str(v) for v in self._fields]

    def _set_default_attributes(self):
        for f in self._fields:
            setattr(self,f,0)

    def _set_attributes_from_dict(self, attr_dict: dict):
        for name, value in attr_dict.items():
            if value is not None:
                setattr(self, name, value)
            else:
                setattr(self, name, 0)

    def _make_dict_from_attributes(self) -> dict:
        return {field: getattr(self, field) for field in self._fields}

    def load_metrics(self):
        query_head = f'MATCH (n:META:{self.node_label}) RETURN'
        query_body = ', '.join(f"n.{field} AS {field}" for field in self._fields)
        query = ' '.join([query_head, query_body])
        result = self.db.read(query)

        if result is not None:
            ordered_result = sorted(result, key=lambda x: x['pass_'])
            last_row = ordered_result[-1]
            self._set_attributes_from_dict(last_row)
            self.pass_ += 1

    def new_record(self):
        current_data = self._make_dict_from_attributes()
        self.db.write(f'CREATE (n:META:{self.node_label}) SET n += $data_attr', data_attr=current_data)
        self.pass_ += 1
        self.local_pass_ += 1

    def start_timer(self):
        self.start_time = time.time()

    def stop_timer(self):
        self.runtime = time.time() - self.start_time

    def report(self, custom_message: str = None):
        reported_values = [f"{field}:{getattr(self,field):,}" for field in self._major_fields]
        reported_values += [f"local_pass__:{self.local_pass_}"]
        if custom_message is not None:
            reported_values += [custom_message]

        return " | ".join(reported_values)

    def reset_metrics(self):
        self.db.write(f'MATCH (n:META:{self.node_label}) DELETE n')
        return self.__class__(self.db)

    def get_data_df(self) -> pd.DataFrame:
        records = self.db.read(f'MATCH (n:META:{self.node_label}) RETURN n')
        df = pd.DataFrame([rec['n'] for rec in records])
        return df

    def save_metrics(self, path: str):
        self.new_record()
        df = self.get_data_df()
        df.sort_values('pass_').to_csv(path, index=False)

    def visualize(self, x='pass_', y='runtime', **kwargs):
        data = self.get_data_df().sort_values(x)
        return px.line(data, x=x, y=y, **kwargs)


class DecomposerMetrics(Metrics):
    node_label = 'DECOMPOSE'
    _major_fields = ['cores_identified','graph_size','n_remaining','min_degree','max_degree','pass_']
    _minor_fields = ['runtime','start_time','local_pass_']

class AgglomeratorMetrics(Metrics):
    node_label = 'AGGLOM'
    _major_fields = ['pass_','graph_size','hop','n_cores','n_assigned','ratio']
    _minor_fields = ['runtime','start_time','local_pass_']

