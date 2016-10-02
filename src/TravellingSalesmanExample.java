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

package org.apache.flink.graph.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.examples.data.TSPDefaultData;
import org.apache.flink.graph.library.MinimumSpanningTree;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.types.NullValue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 *
 * This example shows how to use Gelly's library method.
 * You can find all available library methods in {@link org.apache.flink.graph.library}.
 *
 * In particular, this example uses the {@link MinimumSpanningTree}
 * library method to compute the approximate solution of the Metric Travelling Salesman Problem (TSP).
 *
 * The algorithm is based on Christofides' algorithm for the TSP
 * This is a 2-approximation algorithm: it returns a cycle that is at most twice as long
 * as an optimal cycle: C ≤ 2 · OPT
 *
 * The input file is a plain text file and must be formatted as follows:
 * Edges are represented by tuples of srcVertexId, trgVertexId and EdgeValue (Distance between srcVertexId and trgVertexId) which are
 * separated by tabs. Edges themselves are separated by newlines. List of edges should correspond to undirected complete graph
 * For example: <code>1\t2\t0.3\n1\t3\t1.5\n</code> defines two edges,
 * 1-2 with weight 0.3, and 1-3 with weight 1.5.
 *
 * Usage <code>MST &lt;edge path&gt; &lt;result path for TSP&gt; &lt;result path for MST&gt;
 * &lt;number of vertices&gt; &lt;number of iterations&gt; </code><br>
 *
 * If no parameters are provided, the program is run with default data from
 * {@link TSPDefaultData}
 */

public class TravellingSalesmanExample implements ProgramDescription{

    @SuppressWarnings("serial")
    public static void main(String [] args) throws Exception {

        if(!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Edge<Short, Float>> edges = getEdgeDataSet(env);

        // Find MST of the given graph
        Graph<Short, NullValue, Float> result=Graph.fromDataSet(edges, env)
                .run(new MinimumSpanningTree(maxIterations));

        if(fileOutput) {
            result.getEdges().writeAsCsv(outputPathMST, "\n", ",");
            env.execute("MST is written");
        }
        else {
            System.out.println("Minimum Spanning Tree");
            result.getEdges().print();
            System.out.println("Correct answer:\n" + TSPDefaultData.RESULTED_MST);
        }

        List<Edge<Short,Float>> MSTedges=result.getEdges().collect();

        //Create a Hamiltonian cycle: Tuple2 < Vertex out, Vertex in >
        List<Tuple2<Short,Short>> tspList =
                HamCycle(MSTedges, numOfPoints);

        //Collect edges - approximate TSP
        DataSet<Tuple2<Short,Short>> tspSet = env.fromCollection(tspList);

        DataSet<Tuple3<Short,Short,Float>> tspCycle = tspSet
                .join(edges)
                .where(0,1)
                .equalTo(0,1)
                .with(new JoinFunction<Tuple2<Short, Short>, Edge<Short, Float>, Tuple3<Short, Short, Float>>() {
                    @Override
                    public Tuple3<Short, Short, Float> join(Tuple2<Short, Short> first, Edge<Short, Float> second)
                            throws Exception {
                        return new Tuple3<>(first.f0,first.f1, second.getValue());
                    }
                });

        // emit result
        if(fileOutput) {
            tspCycle.writeAsCsv(outputPath, "\n", ",");
            env.execute("Metric TSP solution");
        } else {
            System.out.println("TSP cycle:");
            tspCycle.print();
            System.out.println("Correct answer:\n" + TSPDefaultData.RESULTED_TSP);
        }

    }

    @Override
    public String getDescription() {
        return "Travelling Salesman Problem Example";
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;

    private static String edgeInputPath = null;

    private static String outputPath = null;

    private static String outputPathMST = null;

    private static Integer maxIterations = TSPDefaultData.MAX_ITERATIONS;

    private static Integer numOfPoints = 9;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if(args.length != 5) {
                System.err.println("Usage: <input edges path> <output path TSP> <output path MST> " +
                        "<num vertices> <num iterations>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            outputPath = args[1];
            outputPathMST=args[2];
            maxIterations = Integer.parseInt(args[4]);
            numOfPoints=Integer.parseInt(args[3]);
        } else {
            System.out.println("Executing Example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("Usage: <input edges path> <output path TSP> <output path MST> " +
                    "<num vertices> <num iterations>");
        }
        return true;
    }

    private static DataSet<Edge<Short, Float>> getEdgeDataSet(ExecutionEnvironment env) {
        if (fileOutput) {
            return env.readCsvFile(edgeInputPath)
                    .fieldDelimiter("\t")
                    .lineDelimiter("\n")
                    .types(Short.class, Short.class, Float.class)
                    .map(new Tuple3ToEdgeMap<Short, Float>());
        } else {
            return TSPDefaultData.getDefaultEdgeDataSet(env);
        }
    }

    /*
     * Walk through all vertices exactly once using MST
     * Create list of pairs Source-Target
     * DFS-based method
     */
    private static List<Tuple2<Short,Short>> HamCycle (List<Edge<Short,Float>> edges, int n)
    {
        List<List<Short>> adj = new ArrayList<>(n+1);
        for (int i=0; i<=n; i++)
            adj.add(new ArrayList<Short>());
        for(Edge<Short,Float> edge : edges)
            adj.get(edge.f0.intValue()).add(edge.f1);

        List<Tuple2<Short,Short>> result = new ArrayList<>();
        Short root = new Short((short)1);  //arbitrary vertex
        Short prev = root;
        Stack<Short> dfs = new Stack<>();
        dfs.push(root);
        boolean[] marked = new boolean[n+1];
        marked[root.intValue()]=true;
        while (!dfs.empty())
        {
            Short curVert = dfs.peek();
            dfs.pop();
            if (!curVert.equals(root)) {
                result.add(new Tuple2<>(prev, curVert));
            }

            for (Short neighbor: adj.get(curVert.intValue()))
            {
                if (!marked[neighbor.intValue()])
                {
                    dfs.push(neighbor);
                    marked[neighbor.intValue()]=true;
                }
            }
            prev = curVert;
        }
        result.add(new Tuple2<>(prev, root));
        return result;
    }

}