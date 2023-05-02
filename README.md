# ➡️Squashy⬅️

Large scale graph compression and summarization tool for research and analysis.

### Note on suitability for use
➡️Squashy⬅️ is relatively new. It was developed for one of my own academic research projects. The principles behind it are based on published research, however the implementation is my own. I think it works well, however it could do with more testing

## Install
Use Docker to initialise a new instance of Memgraph.

````
docker run -it -p 7687:7687 -p 7444:7444 -p 3000:3000 memgraph/memgraph-platform
````
[Full Memgraph Documentation](https://memgraph.com/docs/memgraph)

Then install squashy using PyPi
````
pip install squashy
````

## What does it do?

At some point the key tools of network analysis struggle with scale. Beyond a few hundred nodes, graphs become too dense
to visualise. Nodes too numerous to detect communities.

This can become even more of an issue in a research context, where you often want to run analysis multiple times, tweak
settings or actually _see_ your data.

➡️Squashy⬅️can compress graphs made up of millions of nodes and edges into a graph of a few hundred nodes that retains the overall structure.

## How does it work?
Demo and quick start code coming.
