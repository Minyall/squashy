# Squashy Quickstart Tutorial
## Introduction
In this tutorial we cover the basics of loading data, compression and then demonstrate a number of ways to explore the
compressed graph.

The dataset we will use is the 'Subreddit Hyperlink Network' which represents the relationships between different
communities on [Reddit](https://www.reddit.com/).
Each row represents a post in a community that links to a different Reddit community.

Whilst there are a lot of different properties for each post, for this tutorial we'll keep things simple and just consider the source community of the post and the target community that it linked to.

We will specifically be using `soc-redditHyperlinks-body.tsv` available to download directly from [SNAP](https://snap.stanford.edu/data/soc-RedditHyperlinks.html). It's recommended that you download the TSV file first as it is quite large and we'll be loading it in chunks to keep memory use low.

[Download the dataset from SNAP](https://snap.stanford.edu/data/soc-RedditHyperlinks.html)

## 1. Loading the Data
First we'll iterate through the dataset in discrete chunks. We'll only load the two columns we need, `SOURCE_SUBREDDIT` and `TARGET_SUBREDDIT`, and we'll convert those rows into a list of tuples using Pandas' `.to_records()` method.

```python
import pandas as pd

# You may need to adjust the path to point to where you have stored the dataset.
DATA_PATH = 'soc-redditHyperlinks-body.tsv'

edge_tuples = []

with pd.read_csv(DATA_PATH, usecols=['SOURCE_SUBREDDIT','TARGET_SUBREDDIT'], sep='\t', chunksize=10000) as reader:
    for chunk in reader:
        edges = chunk.to_records(index=False).tolist()
        edge_tuples.extend(edges)
```
This will produce a list of tuples, which is our edge list.
```python
>>> edge_tuples[:5]
[('leagueoflegends', 'teamredditteams'),
 ('theredlion', 'soccer'),
 ('inlandempire', 'bikela'),
 ('nfl', 'cfb'),
 ('playmygame', 'gamedev')]
```
Next we load the edge list into our database using the Squashy `DataImporter` class. We specify that the nodes are type `SUBREDDIT`, and the edges are type `LINKS_TO`. These labels are purely descriptive and it is advised that you name your nodes and edges in a way that describes the relations represented.

```python
from squashy import DataImporter

loader = DataImporter(address='localhost', port=7687, node_label='SUBREDDIT', edge_label='LINKS_TO')
loader.load_from_edge_list(edge_tuples)
```
!!! failure "OperationalError"

    If you get an `OperationalError` this is likely because Memgraph is not running.
    Please use Docker to [start a Memgraph instance](installation.md) before continuing. 
    If your instance is running, check the address and port number passed to `DataImporter` and adjust if necessary.

Once the data is loaded we can ask for the loader to `.report()` and check that we have data in our Memgraph instance.
```python
>>> loader.report()
'Database currently has 35,776 SUBREDDIT nodes, and 137,821 LINKS_TO edges.'
```


## Acknowledgements
Thanks to the authors for their contribution of the Subreddit Hyperlink Network dataset. 
```
@inproceedings{kumar2018community,
  title={Community interaction and conflict on the web},
  author={Kumar, Srijan and Hamilton, William L and Leskovec, Jure and Jurafsky, Dan},
  booktitle={Proceedings of the 2018 World Wide Web Conference on World Wide Web},
  pages={933--943},
  year={2018},
  organization={International World Wide Web Conferences Steering Committee}
}
```



