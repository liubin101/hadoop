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
package org.apache.hadoop.mapred.lib;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.HadoopTestCase;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestMultipleOutputs extends HadoopTestCase {

  public TestMultipleOutputs() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);
  }

  @Test
  public void testWithoutCounters() throws Exception {
    _testMultipleOutputs(false);
    _testMOWithJavaSerialization(false);
  }

  @Test
  public void testWithCounters() throws Exception {
    _testMultipleOutputs(true);
    _testMOWithJavaSerialization(true);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testParallelCloseIOException() throws IOException {
    assertThrows(IOException.class, () -> {
      RecordWriter writer = mock(RecordWriter.class);
      Map<String, RecordWriter> recordWriters = mock(Map.class);
      when(recordWriters.values()).thenReturn(Arrays.asList(writer, writer));
      doThrow(new IOException("test IO exception")).when(writer).close(null);
      JobConf conf = createJobConf();
      MultipleOutputs mos = new MultipleOutputs(conf);
      mos.setRecordWriters(recordWriters);
      mos.close();
    });
  }

  private static final Path ROOT_DIR = new Path("testing/mo");
  private static final Path IN_DIR = new Path(ROOT_DIR, "input");
  private static final Path OUT_DIR = new Path(ROOT_DIR, "output");

  private Path getDir(Path dir) {
    // Hack for local FS that does not have the concept of a 'mounting point'
    if (isLocalFS()) {
      String localPathRoot = System.getProperty("test.build.data", "/tmp")
        .replace(' ', '+');
      dir = new Path(localPathRoot, dir);
    }
    return dir;
  }

  @BeforeEach
  public void setUp() throws Exception {
    super.setUp();
    Path rootDir = getDir(ROOT_DIR);
    Path inDir = getDir(IN_DIR);

    JobConf conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(rootDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    Path rootDir = getDir(ROOT_DIR);

    JobConf conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);
    fs.delete(rootDir, true);
    super.tearDown();
  }
  
  protected void _testMOWithJavaSerialization(boolean withCounters) throws Exception {
    Path inDir = getDir(IN_DIR);
    Path outDir = getDir(OUT_DIR);

    JobConf conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);

    DataOutputStream file = fs.create(new Path(inDir, "part-0"));
    file.writeBytes("a\nb\n\nc\nd\ne");
    file.close();

    fs.delete(inDir, true);
    fs.delete(outDir, true);

    file = fs.create(new Path(inDir, "part-1"));
    file.writeBytes("a\nb\n\nc\nd\ne");
    file.close();

    conf.setJobName("mo");

    conf.set("io.serializations",
    "org.apache.hadoop.io.serializer.JavaSerialization," +
    "org.apache.hadoop.io.serializer.WritableSerialization");

    conf.setInputFormat(TextInputFormat.class);

    conf.setMapOutputKeyClass(Long.class);
    conf.setMapOutputValueClass(String.class);
    conf.setOutputKeyComparatorClass(JavaSerializationComparator.class);

    conf.setOutputKeyClass(Long.class);
    conf.setOutputValueClass(String.class);
    
    conf.setOutputFormat(TextOutputFormat.class);

    MultipleOutputs.addNamedOutput(conf, "text", TextOutputFormat.class,
      Long.class, String.class);

    MultipleOutputs.setCountersEnabled(conf, withCounters);

    conf.setMapperClass(MOJavaSerDeMap.class);
    conf.setReducerClass(MOJavaSerDeReduce.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);

    JobClient jc = new JobClient(conf);
    RunningJob job = jc.submitJob(conf);
    while (!job.isComplete()) {
      Thread.sleep(100);
    }

    // assert number of named output part files
    int namedOutputCount = 0;
    FileStatus[] statuses = fs.listStatus(outDir);
    for (FileStatus status : statuses) {
      if (status.getPath().getName().equals("text-m-00000") ||
        status.getPath().getName().equals("text-r-00000")) {
        namedOutputCount++;
      }
    }
    assertEquals(2, namedOutputCount);

    // assert TextOutputFormat files correctness
    BufferedReader reader = new BufferedReader(
      new InputStreamReader(fs.open(
        new Path(FileOutputFormat.getOutputPath(conf), "text-r-00000"))));
    int count = 0;
    String line = reader.readLine();
    while (line != null) {
      assertTrue(line.endsWith("text"));
      line = reader.readLine();
      count++;
    }
    reader.close();
    assertNotEquals(0, count);

    Counters.Group counters =
      job.getCounters().getGroup(MultipleOutputs.class.getName());
    if (!withCounters) {
      assertEquals(0, counters.size());
    }
    else {
      assertEquals(1, counters.size());
      assertEquals(2, counters.getCounter("text"));
    }
  }

  protected void _testMultipleOutputs(boolean withCounters) throws Exception {
    Path inDir = getDir(IN_DIR);
    Path outDir = getDir(OUT_DIR);

    JobConf conf = createJobConf();
    FileSystem fs = FileSystem.get(conf);

    DataOutputStream file = fs.create(new Path(inDir, "part-0"));
    file.writeBytes("a\nb\n\nc\nd\ne");
    file.close();

    file = fs.create(new Path(inDir, "part-1"));
    file.writeBytes("a\nb\n\nc\nd\ne");
    file.close();

    conf.setJobName("mo");
    conf.setInputFormat(TextInputFormat.class);

    conf.setOutputKeyClass(LongWritable.class);
    conf.setOutputValueClass(Text.class);

    conf.setMapOutputKeyClass(LongWritable.class);
    conf.setMapOutputValueClass(Text.class);

    conf.setOutputFormat(TextOutputFormat.class);

    MultipleOutputs.addNamedOutput(conf, "text", TextOutputFormat.class,
      LongWritable.class, Text.class);
    MultipleOutputs.addMultiNamedOutput(conf, "sequence",
      SequenceFileOutputFormat.class, LongWritable.class, Text.class);

    MultipleOutputs.setCountersEnabled(conf, withCounters);

    conf.setMapperClass(MOMap.class);
    conf.setReducerClass(MOReduce.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);

    JobClient jc = new JobClient(conf);
    RunningJob job = jc.submitJob(conf);
    while (!job.isComplete()) {
      Thread.sleep(100);
    }

    // assert number of named output part files
    int namedOutputCount = 0;
    FileStatus[] statuses = fs.listStatus(outDir);
    for (FileStatus status : statuses) {
      if (status.getPath().getName().equals("text-m-00000") ||
        status.getPath().getName().equals("text-m-00001") ||
        status.getPath().getName().equals("text-r-00000") ||
        status.getPath().getName().equals("sequence_A-m-00000") ||
        status.getPath().getName().equals("sequence_A-m-00001") ||
        status.getPath().getName().equals("sequence_B-m-00000") ||
        status.getPath().getName().equals("sequence_B-m-00001") ||
        status.getPath().getName().equals("sequence_B-r-00000") ||
        status.getPath().getName().equals("sequence_C-r-00000")) {
        namedOutputCount++;
      }
    }
    assertEquals(9, namedOutputCount);

    // assert TextOutputFormat files correctness
    BufferedReader reader = new BufferedReader(
      new InputStreamReader(fs.open(
        new Path(FileOutputFormat.getOutputPath(conf), "text-r-00000"))));
    int count = 0;
    String line = reader.readLine();
    while (line != null) {
      assertTrue(line.endsWith("text"));
      line = reader.readLine();
      count++;
    }
    reader.close();
    assertNotEquals(0, count);

    // assert SequenceOutputFormat files correctness
    SequenceFile.Reader seqReader =
      new SequenceFile.Reader(fs, new Path(FileOutputFormat.getOutputPath(conf),
        "sequence_B-r-00000"), conf);

    assertEquals(LongWritable.class, seqReader.getKeyClass());
    assertEquals(Text.class, seqReader.getValueClass());

    count = 0;
    LongWritable key = new LongWritable();
    Text value = new Text();
    while (seqReader.next(key, value)) {
      assertEquals("sequence", value.toString());
      count++;
    }
    seqReader.close();
    assertNotEquals(0, count);

    Counters.Group counters =
      job.getCounters().getGroup(MultipleOutputs.class.getName());
    if (!withCounters) {
      assertEquals(0, counters.size());
    }
    else {
      assertEquals(4, counters.size());
      assertEquals(4, counters.getCounter("text"));
      assertEquals(2, counters.getCounter("sequence_A"));
      assertEquals(4, counters.getCounter("sequence_B"));
      assertEquals(2, counters.getCounter("sequence_C"));

    }

  }


  @SuppressWarnings({"unchecked"})
  public static class MOMap implements Mapper<LongWritable, Text, LongWritable,
    Text> {

    private MultipleOutputs mos;

    public void configure(JobConf conf) {
      mos = new MultipleOutputs(conf);
    }

    public void map(LongWritable key, Text value,
                    OutputCollector<LongWritable, Text> output,
                    Reporter reporter)
      throws IOException {
      if (!value.toString().equals("a")) {
        output.collect(key, value);
      } else {
        mos.getCollector("text", reporter).collect(key, new Text("text"));
        mos.getCollector("sequence", "A", reporter).collect(key,
          new Text("sequence"));
        mos.getCollector("sequence", "B", reporter).collect(key,
          new Text("sequence"));
      }
    }

    public void close() throws IOException {
      mos.close();
    }
  }

  @SuppressWarnings({"unchecked"})
  public static class MOReduce implements Reducer<LongWritable, Text,
    LongWritable, Text> {

    private MultipleOutputs mos;

    public void configure(JobConf conf) {
      mos = new MultipleOutputs(conf);
    }

    public void reduce(LongWritable key, Iterator<Text> values,
                       OutputCollector<LongWritable, Text> output,
                       Reporter reporter)
      throws IOException {
      while (values.hasNext()) {
        Text value = values.next();
        if (!value.toString().equals("b")) {
          output.collect(key, value);
        } else {
          mos.getCollector("text", reporter).collect(key, new Text("text"));
          mos.getCollector("sequence", "B", reporter).collect(key,
            new Text("sequence"));
          mos.getCollector("sequence", "C", reporter).collect(key,
            new Text("sequence"));
        }
      }
    }

    public void close() throws IOException {
      mos.close();
    }
  }
  
  @SuppressWarnings({"unchecked"})
  public static class MOJavaSerDeMap implements Mapper<LongWritable, Text, Long,
    String> {

    private MultipleOutputs mos;

    public void configure(JobConf conf) {
      mos = new MultipleOutputs(conf);
    }

    public void map(LongWritable key, Text value,
                    OutputCollector<Long, String> output,
                    Reporter reporter)
      throws IOException {
      if (!value.toString().equals("a")) {
        output.collect(key.get(), value.toString());
      } else {
        mos.getCollector("text", reporter).collect(key, "text");
      }
    }

    public void close() throws IOException {
      mos.close();
    }
  }

  @SuppressWarnings({"unchecked"})
  public static class MOJavaSerDeReduce implements Reducer<Long, String,
    Long, String> {

    private MultipleOutputs mos;

    public void configure(JobConf conf) {
      mos = new MultipleOutputs(conf);
    }

    public void reduce(Long key, Iterator<String> values,
                       OutputCollector<Long, String> output,
                       Reporter reporter)
      throws IOException {
      while (values.hasNext()) {
        String value = values.next();
        if (!value.equals("b")) {
          output.collect(key, value);
        } else {
          mos.getCollector("text", reporter).collect(key, "text");
        }
      }
    }

    public void close() throws IOException {
      mos.close();
    }
  }


}
