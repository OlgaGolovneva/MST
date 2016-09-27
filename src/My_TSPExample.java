package org.apache.flink.graph.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.data.MY_MSTDefaultData;
import org.apache.flink.graph.library.MY_MST;
import org.apache.flink.graph.utils.Tuple2ToVertexMap;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import org.apache.flink.graph.EdgesFunctionWithVertexValue;
import java.io.FileWriter;

/**
 * Created by Olga on 9/26/16.
 */

//Christofides' algorithm for the TSP
//2-approximation algorithm: it returns a cycle that is at most twice as long
//as an optimal cycle: C ≤ 2 · OPT

public class My_TSPExample implements ProgramDescription{

    @SuppressWarnings("serial")
    public static void main(String [] args) throws Exception {
    //public static void main(String [] args) throws Exception {

        if(!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Edge<Long, Double>> edges = getEdgeDataSet(env);

        Graph<Long, NullValue, Double> graph = Graph.fromDataSet(edges, env).getUndirected();

        // Find MST of the given graph
        Graph<Long, NullValue, Double> result=graph
                .run(new MY_MST<Long, NullValue, Double>(maxIterations));

        //Eulerian graph
        DataSet<Edge<Long, Double>> outres=result.getUndirected().getEdges().distinct();

        Graph<Long, NullValue, Double> cyclic=Graph.fromDataSet(outres,env);

        //System.out.println("MSTEdges");
        //result.getEdges().print();
        //Find an Euler tour?

        DataSet<Tuple2<Long, LongValue>> VertDegrees = cyclic.outDegrees();

        DataSet<Edge<Long, Double>> mytspPath = null;

        Long root = new Long(1);  //arbitrary vertex
        Long prev = root;
        Stack<Long> dfs = new Stack<Long>();
        dfs.push(root);
        //result.getEdges().print();
        boolean[] marked = new boolean[(int)graph.numberOfVertices()+1];
        marked[root.intValue()]=true;
        while (!dfs.empty())
        {
            Long curVert = dfs.peek();
            dfs.pop();
            if (curVert != root) {
                if (mytspPath==null) {
                    mytspPath=graph.getEdges().filter(new SpecificEdgeFilter(prev, curVert));
                }
                else {
                    mytspPath=mytspPath.union(graph.getEdges().filter(new SpecificEdgeFilter(prev, curVert)));
                }
            }
            List<Edge<Long,Double>> neighbors =
                    cyclic.getEdges().filter(new NeighborsFilter(curVert)).collect();
            for (Edge<Long,Double> edge: neighbors)
            {
                if (!marked[edge.getTarget().intValue()]) {
                    dfs.push(edge.getTarget());
                    marked[edge.getTarget().intValue()]=true;
                }
            }
            prev = curVert;
        }
        //for a cycle (rather than a path)
        if (mytspPath==null) {
            mytspPath=graph.getEdges().filter(new SpecificEdgeFilter(prev, root));
        }
        else {
            mytspPath=mytspPath.union(graph.getEdges().filter(new SpecificEdgeFilter(prev, root)));
        }

        // emit result
        if(fileOutput) {
            mytspPath.writeAsCsv(outputPath, "\n", ",");
            //cyclic.getEdges().writeAsCsv(outputPath, "\n", ",");
           // result.getEdges().print();
            env.execute("Metric TSP solution");
        } else {
            System.out.println("TSPpath:");
            mytspPath.print();
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

    //leave only edges that has distinct Source and Target CC
    @SuppressWarnings("serial")
    @FunctionAnnotation.ForwardedFields("*->*")
    private static class OverweightVertices implements FilterFunction<Tuple2<Long, LongValue>> {
        private LongValue four = new LongValue(4);
        @Override
        public boolean filter(Tuple2<Long, LongValue> vertex) throws Exception {
            return (vertex.f1.compareTo(four)>0);
        }
    }

    private static class NeighborsFilter implements FilterFunction<Edge<Long, Double>> {
        public NeighborsFilter(Long v)
        {this.v=v;}
        Long v;
        @Override
        public boolean filter(Edge<Long, Double> edge) throws Exception {
            return (edge.getSource().compareTo(v)==0);
        }
    }

    private static class SpecificEdgeFilter implements FilterFunction<Edge<Long, Double>> {
        public SpecificEdgeFilter(Long s, Long t)
        {this.s=s; this.t = t;}
        Long s,t;
        @Override
        public boolean filter(Edge<Long, Double> edge) throws Exception {
            return ((edge.getSource().compareTo(s)==0) && (edge.getTarget().compareTo(t)==0));
        }
    }

    @SuppressWarnings("serial")
    private static final class ReduceOverwEdges implements EdgesFunctionWithVertexValue<Long, Long, Long, Tuple2<Long, Long>> {

        @Override
        public void iterateEdges(Vertex<Long, Long> v,
                                 Iterable<Edge<Long, Long>> edges, Collector<Tuple2<Long, Long>> out) throws Exception {

            long weight = Long.MIN_VALUE;

            for (Edge<Long, Long> edge: edges) {
                if (edge.getValue() > weight) {
                    weight = edge.getValue();
                }
            }
            out.collect(new Tuple2<>(v.getId(), weight));
        }
    }
}

