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

package org.apache.flink.graph.examples.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;

import java.util.LinkedList;
import java.util.List;

public class MSTDefaultData {

    public static final Integer MAX_ITERATIONS = 3;

    // TEST 1. Multiple MSTs.
    // Cyclic undirected graph that has several Minimum Spanning Trees.
    public static final String MULTIPLE_EDGES_STR = "1\t2\t1.0\n" + "1\t2\t1.0\n" + "2\t1\t1.0\n" + "2\t3\t1.0\n"+
            "3\t1\t1.0\n"+"3\t2\t1.0";

    private static final Object[][] MULTIPLE_EDGES = new Object[][] {
            new Object[]{(short)1, (short)2, 1.0f},
            new Object[]{(short)1, (short)3, 1.0f},
            new Object[]{(short)2, (short)1, 1.0f},
            new Object[]{(short)2, (short)3, 1.0f},
            new Object[]{(short)3, (short)1, 1.0f},
            new Object[]{(short)3, (short)2, 1.0f}
    };

    public static final String MULTIPLE_MST =  "1,2,1.0\n" + "1,3,1.0\n" + "2,1,1.0\n" + "3,1,1.0";


    // TEST 2. Directed.
    // Cyclic directed graph with distinct edge weights. Thus, there is only one Minimum Spanning Tree.
    public static final String DIRECTED_EDGES_STR = "1\t2\t1.0\n" + "1\t3\t4.0\n" + "2\t3\t8.0";

    private static final Object[][] DIRECTED_EDGES = new Object[][] {
            new Object[]{(short)1, (short)2, 1.0f},
            new Object[]{(short)1, (short)3, 4.0f},
            new Object[]{(short)2, (short)3, 8.0f}
    };

    public static final String DIRECTED_MST =  "1,2,1.0\n" + "2,1,1.0\n" + "1,3,4.0\n" + "3,1,4.0";


    // TEST 3. One MST.
    // Connected Directed Graph with non-distinct edge weights but containing ONLY 1 Minimum Spanning Tree.
    public static final String ONE_EDGES_STR = "1\t2\t1.0\n" + "1\t4\t4.0\n" + "2\t3\t6.0\n" + "2\t4\t5.0\n" +
            "2\t5\t1.0\n" + "3\t5\t5.0\n" + "4\t5\t1.0\n";

    private static final Object[][] ONE_EDGES = new Object[][] {
            new Object[]{(short)1, (short)2, 1.0f},
            new Object[]{(short)1, (short)4, 3.0f},
            new Object[]{(short)2, (short)4, 5.0f},
            new Object[]{(short)2, (short)5, 1.0f},
            new Object[]{(short)3, (short)5, 5.0f},
            new Object[]{(short)4, (short)5, 1.0f}
            //,
            //new Object[]{(short)5, (short)6, 4.0f}
    };

    public static final String ONE_MST =  "1,2,1.0\n" + "2,1,1.0\n" + "2,5,1.0\n" + "3,5,5.0\n" +
            "4,5,1.0\n" + "5,2,1.0\n" + "5,3,5.0\n" + "5,4,1.0";


    // TEST 4. Default.
    // Disconnected directed graph with cycles. Output is a forest of two trees

    public static final String DEFAULT_EDGES_STR =  "1\t2\t1.0\n" + "1\t3\t3.0\n" + "2\t3\t5.0\n" + "4\t5\t5.0\n" +
            "4\t6\t3.0\n" + "5\t6\t1.0";

    private static final Object[][] DEFAULT_EDGES = new Object[][] {
            new Object[]{(short)1, (short)2, 1.0f},
            new Object[]{(short)1, (short)3, 3.0f},
            new Object[]{(short)2, (short)3, 5.0f},
            new Object[]{(short)4, (short)5, 5.0f},
            new Object[]{(short)4, (short)6, 3.0f},
            new Object[]{(short)5, (short)6, 1.0f}
    };

    public static final String DEFAULT_MST =  "1,2,1.0\n" + "2,1,1.0\n" + "1,3,3.0\n" +
            "3,1,3.0\n" + "4,6,3.0\n" + "6,4,3.0\n" + "5,6,1.0\n" + "6,5,1.0\n";


    // END OF DATA SETS


    public static DataSet<Edge<Short, Float>> getEdgeDataSet(ExecutionEnvironment env, int dataSet) {

        Object[][] edges;
        switch (dataSet) {
            case 1: edges = MULTIPLE_EDGES;
                break;
            case 2: edges = DIRECTED_EDGES;
                break;
            case 3: edges = ONE_EDGES;
                break;
            default: edges = DEFAULT_EDGES;
                break;
        }
        List<Edge<Short, Float>> edgeList = new LinkedList<>();
        for (Object[] edge : edges) {
            edgeList.add(new Edge<>((Short)edge[0], (Short)edge[1], (Float)edge[2]));
        }
        return env.fromCollection(edgeList);
    }

    public static String getResultedMST(int dataSet) {

        switch (dataSet) {
            case 1: return MULTIPLE_MST;
            case 2: return DIRECTED_MST;
            case 3: return ONE_MST;
            default: return DEFAULT_MST;
        }
    }

    public static DataSet<Edge<Short, Float>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

        return getEdgeDataSet(env, 0);
    }

    public static String getDefaultResultedMST() {

        return getResultedMST(0);
    }

    private MSTDefaultData() {}
}