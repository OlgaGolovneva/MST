/* My main program
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.graph.library;

/**
 * Created by Olga on 9/12/16.
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.*;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.GraphAlgorithm;

/**Describe your program
 * This implementation uses Boruvka's algorithm to create the Minimum Spanning Tree (MST)
 * Implementation ALLOWS FOR disconnected, directed graph (See MY_MSTDefaultData for Examples)
 * If graph is disconnected, output is a Minimum Spanning Forest
 * Implementation does not take into account Edge Directions, i.e. Sorce -> Target, Sorce <- Target
 * and Sorce <-> Target are equivalent
 *
 * A minimum spanning tree is a spanning tree of a connected, undirected graph.
 * It connects all the vertices together with the minimal total weighting for its edges.
 * A single graph can have many different spanning trees, this algorithm returns one of them
 *
 * The basic algorithm is descibed here: http://www.vldb.org/pvldb/vol7/p1047-han.pdf,
 * and works as follows: In the first phase, each vertex finds a minimum weight out-edge. These edges are added to
 * intermediate MST (i.e. MST at current iteration step). In the second phase, vertices perform Summarization algorithm,
 * using information about Connected Components in intermediate MST. In the third phase, vertices perform edge cleaning.
 * The graph gets smaller and smaller, with the algorithm terminating when only unconnected vertices (i.e. no more
 * Edges) remain.
 *
 * The program returns the resulting graph, which represents the MST (or Forest) of the input graph
 */

public class MY_MST <K, VV, EV extends Comparable<EV>>
        implements GraphAlgorithm<Long, NullValue, Double, Graph<Long, NullValue, Double>> {

    //Implemented in while loop
    private Integer maxIterations;
    //goes to ConnectedComponents
    //private Integer maxIterations2=10;
    private Integer maxIterations2;

    public MY_MST(Integer maxIterations) {
        this.maxIterations = maxIterations;
        this.maxIterations2 = maxIterations;
    }

    @Override
    public Graph<Long, NullValue, Double> run(Graph<Long, NullValue, Double> graph) throws Exception {

        ExecutionEnvironment env =graph.getContext();

        Graph<Long, NullValue, Double> undirectedGraph = graph.getUndirected();

        /**
         * Create working graph with </String> Vertex Values - (!) Currently only String values are
         * acceptable by Summarization library.
         *
         * Each Vertex Value corresponds to its Connected Component
         * Each Edge Value stores its Original Sourse and Target values, and Edge Value
         * Vertex<VertexID,NullValue> -> Vertex<VertexID,ConComp=(String)VertexID>
         * Edge<SourceID,TargetID,Double> -> Edge<SourceID,TargetID,<Double,OriginalSourseID,OriginalTargetID>>
         */
        Graph<Long, String, Double> InVertGr=undirectedGraph.mapVertices(new InitializeVert ());
        Graph<Long, String, Tuple3<Double,Long,Long>> graphWork=InVertGr.mapEdges(new InitializeEdges ());

        /**
         * Create MSTGraph with NO Edges
         * This graph will contain intermediate solution
         */

        Graph<Long, String, Double> MSTGraph = null;

        /**
         * Iterate while working graph has more that 1 Vertex or Number of Iterations < maxIterations
         *
         * "while" loop has to be changed to Bulk/Delta iterations AFTER nested iterations will be allowed in Flink
         */
        int numberOfIterations=0;
        while (graphWork.getVertices().count()>1 && numberOfIterations<maxIterations) {

            numberOfIterations++;

            //This set potentially might convert to IterativeDataSet
            DataSet<Edge<Long, Tuple3<Double, Long, Long>>> CurrentEdges = graphWork.getEdges();

            /**
             * Find a (not necessary connected) subgraph, which contains for each vertex Edges with min(EV)
             * Iterates function SelectMinWeight over all the vertices in graph
             */
            DataSet<Edge<Long, Double>> MinEdgeTuples =
                    graphWork.groupReduceOnEdges(new SelectMinWeight (), EdgeDirection.OUT);

            //Collect intermediate results
            if (MSTGraph == null) {
                MSTGraph = Graph.fromDataSet(graphWork.getVertices(), MinEdgeTuples, env);
            } else {
                MSTGraph = MSTGraph.union(Graph.fromDataSet(graphWork.getVertices(), MinEdgeTuples, env));
            }

            /**
             * Use GSAConnectedComponents to find connected components in the output graph
             */

            DataSet<Vertex<Long, String>> UpdateConComp =
                    MSTGraph.run(new GSAConnectedComponents<Long, String, Double>(maxIterations2));

            /**
             * Use Summarize to create/edit SuperVertices in ORIGINAL graph
             */

            Graph<Long, Summarization.VertexValue<String>, Summarization.EdgeValue<Tuple3<Double, Long, Long>>> CompressedGraph =
                    Graph.fromDataSet(UpdateConComp, CurrentEdges, env)
                            .run(new Summarization<Long, String, Tuple3<Double, Long, Long>>());

            /**
             * Now we want to "clean" our graph: 1) delete loops
             * 2) select minWeightEdge and go back to original VV type
             */

            CompressedGraph = CompressedGraph.filterOnEdges(new CleanEdges<Long, Summarization.EdgeValue<Tuple3<Double,Long,Long>>>());

            DataSet<Edge<Long, Tuple3<Double, Long, Long>>> FinalEdges =
                    CompressedGraph.groupReduceOnEdges(new SelectMinWeight2(), EdgeDirection.OUT);

            DataSet<Vertex<Long, String>> FinalVertices = CompressedGraph.mapVertices(new ExtractVertVal ()).getVertices();

            //collect data for the next loop iteration or quit iteration
            if (FinalEdges.count()>0) {
                graphWork = Graph.fromDataSet(FinalVertices, FinalEdges, env);
            }
            else numberOfIterations=maxIterations;
        }

        //Final solution
        Graph<Long, NullValue, Double> MSTout=Graph.fromDataSet(graph.getVertices(), MSTGraph.getEdges(),env)
                .getUndirected()
                .intersect(graph,true);

        return MSTout;
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    /**
     * Each VV corresponds to its </String> Connected Component (CC) in MST Graph.
     * Before iterations, the number of CC is equal to the number of Vertices (MST Graph has NO edges)
     * </String> is used only to make Summarization work correctly
     */
    @SuppressWarnings("serial")
    public static final class InitializeVert implements MapFunction<Vertex<Long, NullValue>, String> {

        public InitializeVert() {}
        @Override
        public String map(Vertex<Long, NullValue> vertex) throws Exception {
            return Long.toString(vertex.f0);
        }
    }

    /**
     * Each Edge will store its original Source and Target Vertices along with its VV
     */
    public static final class InitializeEdges
            implements MapFunction<Edge<Long, Double>, Tuple3<Double, Long, Long>> {

        public InitializeEdges() {}
        @Override
        public Tuple3<Double, Long, Long> map(Edge<Long, Double> edge) throws Exception {
            return new Tuple3(edge.f2,edge.f0,edge.f1);
        }
    }

    /**
     * For given vertex find edge with min(VV) and change VV type from </Tuple3> to </Double>.
     * If vertex has multiple edges with the same min(VV), output edge with min(TargetSource)
     * This allows for graphs with non-distinct edges
     */

    private static final class SelectMinWeight
            implements EdgesFunction<Long,Tuple3<Double,Long,Long>,Edge<Long, Double>> {

        public void iterateEdges(Iterable<Tuple2<Long, Edge<Long, Tuple3<Double,Long,Long>>>> edges,
                                 Collector<Edge<Long, Double>> out) throws Exception
        {
            Double minVal = Double.MAX_VALUE;
            Edge<Long,Double> minEdge = null;
            for (Tuple2<Long, Edge<Long, Tuple3<Double,Long,Long>>> tuple : edges)
            {
                if (tuple.f1.getValue().f0 < minVal)
                {
                    minVal = tuple.f1.getValue().f0;
                    //Original Source and Target!!!!
                    minEdge=new Edge(tuple.f1.getValue().f1, tuple.f1.getValue().f2,minVal);
                }
                //we need to take into account equal edges!
                else if (tuple.f1.getValue().f0 == minVal && tuple.f1.getValue().f2<minEdge.getTarget()){
                    minEdge=new Edge(tuple.f1.getValue().f1, tuple.f1.getValue().f2,minVal);
                }
            }
            if (minEdge!= null)
                out.collect(minEdge);
        }
    }

    /**
     * For given vertex find edge with min(VV) and change VV type from </Summarization.EdgeValue</Tuple3>>
     * to </Tuple3>.
     * If vertex has multiple edges with the same min(VV), output edge with min(OriginalTargetSource)
     * This allows for graphs with non-distinct edges
     */

    private static final class SelectMinWeight2
            implements EdgesFunction<Long,Summarization.EdgeValue<Tuple3<Double,Long,Long>>,
            Edge<Long, Tuple3<Double,Long,Long>>> {

        public void iterateEdges(Iterable<Tuple2<Long, Edge<Long,
                Summarization.EdgeValue<Tuple3<Double,Long,Long>>>>> edges,
                                 Collector<Edge<Long, Tuple3<Double,Long,Long>>> out) throws Exception
        {
            Double minVal = Double.MAX_VALUE;
            Edge<Long,Summarization.EdgeValue<Tuple3<Double,Long,Long>>> minEdge = null;
            Edge<Long,Tuple3<Double,Long,Long>> outEdge= new Edge();
            for (Tuple2<Long, Edge<Long, Summarization.EdgeValue<Tuple3<Double,Long,Long>>>> tuple : edges)
            {
                if (tuple.f1.getValue().f0.f0 < minVal)
                {
                    minVal = tuple.f1.getValue().f0.f0;
                    minEdge = tuple.f1;
                }
                else if (tuple.f1.getValue().f0.f0 == minVal && tuple.f1.getValue().f0.f2<minEdge.getValue().f0.f2){
                    minEdge=tuple.f1;
                }
            }
            if (minEdge!= null)
                outEdge.setSource(minEdge.getSource());
                outEdge.setTarget(minEdge.getTarget());
                outEdge.setValue(minEdge.getValue().f0);
                out.collect(outEdge);
        }
    }

    /**
     * For given vertex extract </String> VV out of </Summarization.VertexValue<String>>>
     */

    @SuppressWarnings("serial")
    private static class ExtractVertVal implements MapFunction<Vertex<Long, Summarization.VertexValue<String>>, String> {

        @Override
        public String map(Vertex<Long, Summarization.VertexValue<String>> vertex) throws Exception {
            return vertex.f1.f0;
        }
    }

    /**
     * For given vertex delete all loop Edges
     */

    @SuppressWarnings("serial")
    @FunctionAnnotation.ForwardedFields("*->*")
    private static class CleanEdges<T extends Comparable<T>, ET> implements FilterFunction<Edge<T, ET>> {
        @Override
        public boolean filter(Edge<T, ET> value) throws Exception {
            return !(value.f0.compareTo(value.f1)==0);
        }
    }
}