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

## Usage
See the `demos` folder for a Jupyter notebook showing a quick start guide to using Squashy. 
A demo showing the individual component classes is coming soon.

## What does it do?

At some point the key tools of network analysis struggle with scale. Beyond a few hundred nodes, graphs become too dense
to visualise. Nodes too numerous to detect communities.

This can become even more of an issue in a research context, where you often want to run analysis multiple times, tweak
settings or actually _see_ your data.

➡️Squashy⬅️can compress graphs made up of millions of nodes and edges into a graph of a few hundred nodes that retains the overall structure.

## How does it work?
Squashy is based on work by Natalie Stanley and colleagues published in _Scientific Reports_ ([DOI](http://dx.doi.org/10.1038/s41598-018-29174-3)).
In short compression works by first identifying nodes in the network to be 'supernodes' or what we refer to in Squashy as 'CORE' nodes.
These are determined by a process of K-Core decomposition, trimming away at the graph until all nodes have a degree of `k`, which is typically 2.
Once this point is reached, the highest degree node is selected as a CORE, removed from the network and decomposition repeats until either there are no nodes left, or the desired number of CORES is reached.

After decomposition, nodes are assigned to CORE nodes to represent them, through a process of breadth-first-search. Finally, new edges are created between nodes that represent the underlying edges of the nodes they represent.
These edges are weighted to best represent the relationship between the nodes represented by two different cores. A graph formed of these core nodes and representative edges is our core graph, and should represent the underlying structure of the full raw graph.

### Documentation?
Please see the `demos` folder for a quickstart guide. Built-in documentation of the major classes and methods of Squashy is next on the list!