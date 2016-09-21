package org.apache.flink.graph.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.Summarization;
import java.util.LinkedList;
import java.util.List;

public class MySummarizationExample implements ProgramDescription {

    @SuppressWarnings("serial")
    public static void main(String [] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //Create graph
        DataSet<Edge<Long, Double>> edges = getEdgeDataSet(env);
        DataSet<Vertex<Long, String>> vertices = getVertexDataSet(env);
        Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);

        //emit input
        System.out.println("Executing example with following inputs:\n"+"Vertices:\n");
        vertices.print();
        System.out.println("Edges:\n");
        edges.print();

        Graph<Long, Summarization.VertexValue<String>, Summarization.EdgeValue<Double>> result = graph
                .run(new Summarization<Long, String, Double>());

        //now we want to read the output
        DataSet<Edge<Long, Summarization.EdgeValue<Double>>> edgesOut = result.getEdges();
        DataSet<Vertex<Long, Summarization.VertexValue<String>>> verticesOut = result.getVertices();

        // emit result
        System.out.println("Summarized graph:\n"+"Vertices:\n");
        verticesOut.print();
        System.out.println("Edges:\n");
        edgesOut.print();

        System.out.println("Summarized graph:\n"+"Vertices:\n");
        verticesOut.print();
        System.out.println("Edges:\n");
        edgesOut.print();

        System.out.println("Summarized graph:\n"+"Vertices:\n");
        verticesOut.print();
        System.out.println("Edges:\n");
        edgesOut.print();

        System.out.println("Summarized graph:\n"+"Vertices:\n");
        verticesOut.print();
        System.out.println("Edges:\n");
        edgesOut.print();
    }

    @Override
    public String getDescription() {
        return "Summarization Example";
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
