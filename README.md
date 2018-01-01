# Minimum Spanning Tree

This is a distributed Flink implementation of Boruvka's algorithm which finds a Minimum Spanning Tree (MST).

In a connected undirected graph, a minimum spanning tree is s subgraph which connects all the vertices with the minimal total edge weight. A graph can have many different spanning trees, this algorithm returns one of them. If the input graph is disconnected, output is a Minimum Spanning Forest. 

Since minimum spanning trees are defined for undirected graphs only, this implementation does not take into account Edge Directions. For example, the following edges in the input graph are treated identically: Source -> Target, Source <- Target and Source <-> Target. That is, every directed edge of the input graph is complemented with the reverse directed edge of the same weight (the complementary edges never appear in the output).
 
The basic algorithm is descibed [here](http://www.vldb.org/pvldb/vol7/p1047-han.pdf), and works as follows: In the first phase, each vertex finds a minimum weight outgoing edge. These edges are added to the intermediate MST (i.e. the MST at the current step). In the second phase, vertices perform Summarization algorithm, using information about Connected Components in the intermediate MST. In this step we contract each connected component into a vertex. In the third phase, vertices perform edges cleaning. The graph gets smaller and smaller, and the algorithm terminates when only unconnected vertices (i.e. no more Edges) remain. The program returns the resulting graph, which represents an MST (or Forest) of the input graph.

![Distributed Boruvka's algorithm](https://github.com/OlgaGolovneva/MST/blob/master/boruvka.png)

# Travelling Salesman Problem

As an application of the implemented Minimum Spanning Tree algorithm, we present an approximation algorithm for the Metric Travelling Salesman Problem (TSP). We implement the 2-approximation algorithm for the Metric version of TSP by Kou, Markowsky, and Berman ["A fast algorithm for Steiner trees." Acta informatica 15.2 (1981): 141-145](http://aturing.umcs.maine.edu/~markov/SteinerTrees.pdf). This algorithm is guaranteed to return a cycle that is at most twice as long as an optimal cycle: C ≤ 2 · OPT.

The algorithm guarantees 2-approximation only for Metric graphs, i.e., graphs where edge weights satisfy the triangle inequality:  w(A,C) ≤ w(A,B) + w(B,C). In particular, if graph edges represent distances between points in any Euclidean space (R<sup>n</sup>), then they do satisfy the triangle inequality.
