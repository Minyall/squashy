
# :material-arrow-collapse-right:Squashy:material-arrow-collapse-left:
Large scale graph compression and summarization tool for research and analysis.

## Quick Install
For full documentation see the [installation guide](installation.md).

Install from PyPi
```bash
pip install squashy
```
Run your Memgraph instance

```bash
docker run -it -p 7687:7687 -p 7444:7444 -p 3000:3000 memgraph/memgraph-platform
```

## Quick Start
A more detailed [quickstart guide](quickstart.md) is available.
```python
from squashy import Squash

# Initialise Squash class
squasher = Squash(node_label='SUBREDDIT',relation_label='LINKS_TO', weight_label='weight')

# Compress the graph
squasher.squash_graph(max_cores=400, k=2, max_hops=2)

# Get the resulting compressed graph
edges = squasher.get_core_edge_list()
nodes = squasher.get_core_node_list()
```
