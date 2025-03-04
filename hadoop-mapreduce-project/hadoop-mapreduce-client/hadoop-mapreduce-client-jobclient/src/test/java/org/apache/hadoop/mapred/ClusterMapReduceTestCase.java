/**
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
package org.apache.hadoop.mapred;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Test case to run a MapReduce job.
 * <p/>
 * It runs a 2 node cluster Hadoop with a 2 node DFS.
 * <p/>
 * The JobConf to use must be obtained via the creatJobConf() method.
 * <p/>
 * It creates a temporary directory -accessible via getTestRootDir()-
 * for both input and output.
 * <p/>
 * The input directory is accesible via getInputDir() and the output
 * directory via getOutputDir()
 * <p/>
 * The DFS filesystem is formated before the testcase starts and after it ends.
 */
public abstract class ClusterMapReduceTestCase {
  private static File testRootDir;
  private static File dfsFolder;

  private MiniDFSCluster dfsCluster = null;
  private MiniMRClientCluster mrCluster = null;

  protected static void setupClassBase(Class<?> testClass) throws Exception {
    // setup the test root directory
    testRootDir = GenericTestUtils.setupTestRootDir(testClass);
    dfsFolder = new File(testRootDir, "dfs");
  }


  /**
   * Creates Hadoop Cluster and DFS before a test case is run.
   *
   * @throws Exception
   */
  @BeforeEach
  public void setUp() throws Exception {
    startCluster(true, null);
  }

  /**
   * Starts the cluster within a testcase.
   * <p/>
   * Note that the cluster is already started when the testcase method
   * is invoked. This method is useful if as part of the testcase the
   * cluster has to be shutdown and restarted again.
   * <p/>
   * If the cluster is already running this method does nothing.
   *
   * @param reformatDFS indicates if DFS has to be reformated
   * @param props configuration properties to inject to the mini cluster
   * @throws Exception if the cluster could not be started
   */
  protected synchronized void startCluster(boolean reformatDFS, Properties props)
          throws Exception {
    if (dfsCluster == null) {
      JobConf conf = new JobConf();
      if (props != null) {
        for (Map.Entry entry : props.entrySet()) {
          conf.set((String) entry.getKey(), (String) entry.getValue());
        }
      }
      dfsCluster =
          new MiniDFSCluster.Builder(conf, dfsFolder)
              .numDataNodes(2).format(reformatDFS).racks(null).build();
      mrCluster = MiniMRClientClusterFactory.create(this.getClass(), 2, conf);
    }
  }

  /**
   * Stops the cluster within a testcase.
   * <p/>
   * Note that the cluster is already started when the testcase method
   * is invoked. This method is useful if as part of the testcase the
   * cluster has to be shutdown.
   * <p/>
   * If the cluster is already stopped this method does nothing.
   *
   * @throws Exception if the cluster could not be stopped
   */
  protected void stopCluster() throws Exception {
    if (mrCluster != null) {
      mrCluster.stop();
      mrCluster = null;
    }
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }
  }

  /**
   * Destroys Hadoop Cluster and DFS after a test case is run.
   *
   * @throws Exception
   */
  @AfterEach
  public void tearDown() throws Exception {
    stopCluster();
  }

  /**
   * Returns a preconfigured Filesystem instance for test cases to read and
   * write files to it.
   * <p/>
   * TestCases should use this Filesystem instance.
   *
   * @return the filesystem used by Hadoop.
   * @throws IOException 
   */
  protected FileSystem getFileSystem() throws IOException {
    return dfsCluster.getFileSystem();
  }

  /**
   * Returns the path to the root directory for the testcase.
   *
   * @return path to the root directory for the testcase.
   */
  protected Path getTestRootDir() {
    return new Path(testRootDir.getPath());
  }

  /**
   * Returns a path to the input directory for the testcase.
   *
   * @return path to the input directory for the tescase.
   */
  protected Path getInputDir() {
    return new Path("target/input");
  }

  /**
   * Returns a path to the output directory for the testcase.
   *
   * @return path to the output directory for the tescase.
   */
  protected Path getOutputDir() {
    return new Path("target/output");
  }

  /**
   * Returns a job configuration preconfigured to run against the Hadoop
   * managed by the testcase.
   *
   * @return configuration that works on the testcase Hadoop instance
   */
  protected JobConf createJobConf() throws IOException {
    return new JobConf(mrCluster.getConfig());
  }

}
