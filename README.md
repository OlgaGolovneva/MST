# Minimum Spanning Tree

This implementation uses Boruvka's algorithm to find a Minimum Spanning Tree (MST)

A minimum spanning tree is a spanning tree of a connected, undirected graph. It connects all the vertices together with the minimal total weighting for its edges. A single graph can have many different spanning trees, this algorithm returns one of them. If the input graph is disconnected, output is a Minimum Spanning Forest. Implementation does not take into account Edge Directions, i.e. the following edges in the input graph are treated equivalently: Source -> Target, Source <- Target and Source <-> Target. That is, every directed edge of the input graph is complemented with the reverse directed edge of the same weight (the complementary edges never appear in the output).
 
The basic algorithm is descibed [here](http://www.vldb.org/pvldb/vol7/p1047-han.pdf), and works as follows: In the first phase, each vertex finds a minimum weight out-edge. These edges are added to intermediate MST (i.e. MST at current iteration step). In the second phase, vertices perform Summarization algorithm, using information about Connected Components in intermediate MST. In the third phase, vertices perform edges cleaning. The graph gets smaller and smaller, and the algorithm terminates when only unconnected vertices (i.e. no more Edges) remain. The program returns the resulting graph, which represents the MST (or Forest) of the input graph.

# Travelling Salesman Problem

Another algorithm uses the MST library method to compute an approximate solution to the Metric Travelling Salesman Problem (TSP). This class implements the 2-approximation algorithm for Metric version of TSP by Kou, Markowsky, and Berman ["A fast algorithm for Steiner trees." Acta informatica 15.2 (1981): 141-145.]. It always returns a cycle that is at most twice as long as an optimal cycle: C ≤ 2 · OPT.

The algorithm guarantees 2-approximation only for Metric graphs, i.e., graphs where edge weights satisfy triangle inequality:   w(A,C) ≤ w(A,B) + w(B,C). In particular, if edges represent distances between points in any Euclidean space (R^n), they do satisfy triangle inequality.
