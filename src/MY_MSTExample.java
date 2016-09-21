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

/**
 * Created by Olga on 9/12/16.
 */

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.data.MY_MSTDefaultData;
import org.apache.flink.graph.library.MY_MST;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.types.NullValue;

/**
 * This example shows how to use Gelly's library method.
 * You can find all available library methods in {@link org.apache.flink.graph.library}.
 *
 * In particular, this example uses the {@link MY_MST}
 * library method to compute the connected components of the input graph.
 *
 * The input file is a plain text file and must be formatted as follows:
 * Edges are represented by tuples of srcVertexId, trgVertexId and Weight (Value, Distance) which are
 * separated by tabs. Edges themselves are separated by newlines.
 * For example: <code>1\t2\t0.3\n1\t3\t1.5\n</code> defines two edges,
 * 1-2 with weight 0.3, and 1-3 with weigth 1.5.
 *
 * Usage <code>MST &lt;edge path&gt; &lt;result path&gt;
 * &lt;number of iterations&gt; </code><br>
 *
 * If no parameters are provided, the program is run with default data from
 * {@link MY_MSTDefaultData}
 */

public class MY_MSTExample implements ProgramDescription {

    @SuppressWarnings("serial")
    public static void main(String [] args) throws Exception {

        if(!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Edge<Long, Double>> edges = getEdgeDataSet(env);

        Graph<Long, NullValue, Double> graph = Graph.fromDataSet(edges, env);

        // Find MST of the given graph
        Graph<Long, NullValue, Double> result=graph
                .run(new MY_MST<Long, NullValue, Double>(maxIterations));

        // Extract the Edges as the result
        DataSet<Edge<Long, Double>> MSTEdges = result.getEdges();

        // Extract the Vertices as the result
        DataSet<Vertex<Long, NullValue>> MSTVertices = result.getVertices();

        // emit result
        if(fileOutput) {
            MSTEdges.writeAsCsv(outputPath, "\n", ",");

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("Minimum Spanning Tree");
        } else {

            //uncomment if needed:
            //MSTVertices.print();
            MSTEdges.print();

            System.out.println("Correct answer:\n" + MY_MSTDefaultData.RESULTED_MST);
        }

    }

    @Override
    public String getDescription() {
        return "Minimum Spanning Tree Example";
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;

    private static String edgeInputPath = null;

    private static String outputPath = null;

    private static Integer maxIterations = MY_MSTDefaultData.MAX_ITERATIONS;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if(args.length != 3) {
                System.err.println("Usage: MST  <input edges path> <output path> <num iterations>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            outputPath = args[1];
            maxIterations = Integer.parseInt(args[2]);
        } else {
            System.out.println("Executing MST with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("Usage: MST  <input edges path> <output path>" +
                    " <num iterations>");
        }
        return true;
    }

    private static DataSet<Edge<Long, Double>> getEdgeDataSet(ExecutionEnvironment env) {
        if (fileOutput) {
            return env.readCsvFile(edgeInputPath)
                    .fieldDelimiter("\t")
                    .lineDelimiter("\n")
                    .types(Long.class, Long.class, Double.class)
                    .map(new Tuple3ToEdgeMap<Long, Double>());
        } else {
            return MY_MSTDefaultData.getDefaultEdgeDataSet(env);
        }
    }

}
