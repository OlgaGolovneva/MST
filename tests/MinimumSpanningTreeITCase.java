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

package org.apache.flink.graph.test;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.MinimumSpanningTreeExample;
import org.apache.flink.graph.examples.data.MSTDefaultData;
import org.apache.flink.graph.library.MinimumSpanningTree;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.NullValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.List;

@RunWith(Parameterized.class)
public class MinimumSpanningTreeITCase extends MultipleProgramsTestBase{

    public MinimumSpanningTreeITCase(TestExecutionMode mode) {
        super(mode);
    }

    private String edgesPath;

    private String resultPath;

    private String expected;

    private File edgesFile;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void before() throws Exception {

        resultPath = tempFolder.newFile().toURI().toString();
        edgesFile = tempFolder.newFile();
        edgesPath = edgesFile.toURI().toString();
    }

    // --------------------------------------------------------------------------------------------
    //  Minimum Spanning Tree Tests
    // --------------------------------------------------------------------------------------------

    @Test
    public void testMultipleMST() throws Exception {
        Files.write(MSTDefaultData.MULTIPLE_EDGES_STR, edgesFile, Charsets.UTF_8);
        MinimumSpanningTreeExample.main(new String[]{edgesPath, resultPath,
                MSTDefaultData.MAX_ITERATIONS + ""});
        expected = MSTDefaultData.MULTIPLE_MST;
    }

    @Test
    public void testDirectedMST() throws Exception {
        Files.write(MSTDefaultData.DIRECTED_EDGES_STR, edgesFile, Charsets.UTF_8);
        MinimumSpanningTreeExample.main(new String[]{edgesPath, resultPath,
                MSTDefaultData.MAX_ITERATIONS + ""});
        expected = MSTDefaultData.DIRECTED_MST;
    }

    @Test
    public void testOneMST() throws Exception {
        Files.write(MSTDefaultData.ONE_EDGES_STR, edgesFile, Charsets.UTF_8);
        MinimumSpanningTreeExample.main(new String[]{edgesPath, resultPath,
                MSTDefaultData.MAX_ITERATIONS + ""});
        expected = MSTDefaultData.ONE_MST;
    }

    @Test
    public void testForestMST() throws Exception {
        Files.write(MSTDefaultData.DEFAULT_EDGES_STR, edgesFile, Charsets.UTF_8);
        MinimumSpanningTreeExample.main(new String[]{edgesPath, resultPath,
                MSTDefaultData.MAX_ITERATIONS + ""});
        expected = MSTDefaultData.DEFAULT_MST;
    }

    @After
    public void after() throws Exception {
        TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath);
    }
}