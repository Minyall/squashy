# Installation
Squashy installation has two stages.

1. Installation of the Squashy package from pypi.
2. Installation and initialisation of a Memgraph graph database.

## 1. Installing Squashy

Install as standard from [pypi](https://pypi.org/project/squashy/)

````bash
pip install squashy
````

## 2. Installing Memgraph
Squashy relies on the graph database [Memgraph](https://memgraph.com/) for data storage and a number of key operations to support 
the compression process. A live instance of the Memgraph database must be active before running Squashy. In most cases 
this database can be run locally on your laptop or similar.

### 2a. Docker
Memgraph installation relies on [Docker](https://www.docker.com/) to download and run Memgraph in a self-contained
container. If you don't already have it, ensure Docker is [installed and running](https://docs.docker.com/get-docker/) 
before continuing.

### 2b. Memgraph
Memgraph can be installed and initialised with one command in the terminal.

```bash
docker run -it -p 7687:7687 -p 7444:7444 -p 3000:3000 memgraph/memgraph-platform
```
Once installation is complete, Memgraph will be running in your terminal. You can check that it is running
by visiting `localhost:3000` in your web browser to view the [Memgraph Lab](https://memgraph.com/docs/memgraph-lab).

Further information can be fo