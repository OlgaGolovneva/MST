package org.apache.flink.graph.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.GSAConnectedComponents;
import org.apache.flink.graph.library.MY_MST;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Olga on 9/21/16.
 */
public class MyTuple3Example implements ProgramDescription {

    private static Integer maxIterations=2;

    @SuppressWarnings("serial")
    public static void main(String [] args) throws Exception{

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Create graph
        DataSet<Edge<Long, Double>> edges = getEdgeDataSet(env);
        DataSet<Vertex<Long, String>> vertices = getVertexDataSet(env);
        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);

        //emit input
        System.out.println("Executing example with the following input parameters:\n"+"Vertices:\n");
        vertices.print();
        System.out.println("Edges:\n");
        edges.print();

        Graph<Long, String, Tuple3<Double,Long,Long>> graphOut=graph.mapEdges(new InitializeEdges());

        // emit result
        System.out.println("Final graph:\n"+"Vertices:\n");
        graphOut.getVertices().print();
        System.out.println("Edges:\n");
        graphOut.getEdges().print();
    }

 /*   //Change Edge Values type: WORKS FINE
    public static final class InitializeEdges
            implements MapFunction<Edge<Long, Double>, Tuple3<Double, Long, Long>> {

        public InitializeEdges() {}
        @Override
        public Tuple3<Double, Long, Long> map(Edge<Long, Double> edge) throws Exception {
            return new Tuple3(edge.f2,edge.f0,edge.f1);
        }
   */

    //Change Edge Values type: DOES NOT WORK
    public static final class InitializeEdges <E, EV>
            implements MapFunction<Edge<E, EV>, Tuple3<EV, E, E>> {

        public InitializeEdges() {}
        @Override
        public Tuple3<EV, E, E> map(Edge<E, EV> edge) throws Exception {
            return new Tuple3(edge.f2,edge.f0,edge.f1);
        }
    }

    @Override
    public String getDescription() {
        return "Nested Iteration Example";
    }

    //Define edges
    private static DataSet<Edge<Long, Double>> getEdgeDataSet(ExecutionEnvironment env) {
        Object[][] DEFAULT_EDGES = new Object[][] {
                new Object[]{1L, 2L, 1.0},
                new Object[]{2L, 1L, 1.0},
                new Object[]{1L, 4L, 3.0},
                new Object[]{4L, 1L, 3.0},
                new Object[]{2L, 3L, 6.0},
                new Object[]{3L, 2L, 6.0},
                new Object[]{2L, 4L, 5.0},
                new Object[]{4L, 2L, 5.0},
                new Object[]{2L, 5L, 1.0},
                new Object[]{5L, 2L, 1.0},
                new Object[]{3L, 5L, 5.0},
                new Object[]{5L, 3L, 5.0},
                new Object[]{3L, 6L, 2.0},
                new Object[]{6L, 3L, 2.0},
                new Object[]{4L, 5L, 1.0},
                new Object[]{5L, 4L, 1.0},
                new Object[]{5L, 6L, 4.0},
                new Object[]{6L, 5L, 4.0}
        };
        List<Edge<Long, Double>> edgeList = new LinkedList<Edge<Long, Double>>();
        for (Object[] edge : DEFAULT_EDGES) {
            edgeList.add(new Edge<Long, Double>((Long) edge[0], (Long) edge[1], (Double) edge[2]));
        }
        return env.fromCollection(edgeList);
    }
    //Define vertices
    private static DataSet<Vertex<Long, String>> getVertexDataSet(ExecutionEnvironment env) {
        //We will summarize by <VV> = String
        Object[][] DEFAULT_VERTICES = new Object[][] {
                new Object[]{3L, "3"},
                new Object[]{6L, "3"},
                new Object[]{1L, "1"},
                new Object[]{5L, "1"},
                new Object[]{2L, "1"},
                new Object[]{4L, "1"}
        };
        List<Vertex<Long, String>> vertexList = new LinkedList<Vertex<Long, String>>();
        for (Object[] vertex : DEFAULT_VERTICES) {
            vertexList.add(new Vertex<Long, String>((Long) vertex[0], (String) vertex[1]));
        }
        return env.fromCollection(vertexList);
    }

}

