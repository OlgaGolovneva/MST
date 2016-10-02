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

/**
 * Provides the data set used for the TravellingSalesmanExample program.
 */

public class TSPDefaultData {

    public static final Integer MAX_ITERATIONS = 3;

    public static final Object[][] DEFAULT_EDGES = new Object[][] {
            new Object[]{1, 2, 74.535614},
            new Object[]{1, 3, 4109.913460},
            new Object[]{1, 4, 3047.995707},
            new Object[]{2, 3, 4069.705149},
            new Object[]{2, 4, 2999.490730},
            new Object[]{3, 4, 1172.366994},
            new Object[]{1, 5, 2266.911731},
            new Object[]{2, 5, 2213.594362},
            new Object[]{3, 5, 1972.941966},
            new Object[]{4, 5, 816.666700}
    };

    public static final String RESULTED_MST =  "1\t2\t74.535614\n" + "2\t1\t74.535614\n" +
            "2\t5\t2213.594362\n" + "5\t2\t2213.594362\n"+
            "3\t4\t1172.366994\n" + "4\t3\t1172.366994\n" +
            "4\t5\t816.666700\n" + "5\t4\t816.666700\n";

    public static final String RESULTED_TSP =  "1\t2\t74.535614\n" +
            "2\t5\t2213.594362\n" +
            "5\t4\t816.666700\n" +
            "4\t3\t1172.366994\n" +
            "3\t1\t4109.913460\n";


    public static DataSet<Edge<Short, Float>> getDefaultEdgeDataSet(ExecutionEnvironment env) {

        List<Edge<Short, Float>> edgeList = new LinkedList<Edge<Short, Float>>();
        for (Object[] edge : DEFAULT_EDGES) {
            edgeList.add(new Edge<>(((Integer)edge[0]).shortValue(), ((Integer) edge[1]).shortValue(), ((Double) edge[2]).floatValue()  ));
            edgeList.add(new Edge<>(((Integer)edge[1]).shortValue(), ((Integer) edge[0]).shortValue(), ((Double) edge[2]).floatValue()  ));
        }
        return env.fromCollection(edgeList);
    }

    private TSPDefaultData() {}
}