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
package org.apache.hadoop.mapreduce.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.server.jobtracker.JTConfig;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import static junit.framework.Assert.assertTrue;

public class TestMRDistributed {
  private static Path TEST_ROOT_DIR =
          new Path(System.getProperty("test.build.data", "/tmp"));

  private Path makeJar(Path p, int index) throws IOException {
    FileOutputStream fos = new FileOutputStream(new File(p.toString()));
    JarOutputStream jos = new JarOutputStream(fos);
    ZipEntry ze = new ZipEntry("distributed.jar.inside" + index);
    jos.putNextEntry(ze);
    jos.write(("inside the jar!" + index).getBytes());
    jos.closeEntry();
    jos.close();
    return p;
  }

  @Test
  public void testDistributedFileAccessLocalMode() throws IOException,
          ClassNotFoundException, InterruptedException, URISyntaxException {
    Configuration c = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(c).numDataNodes(1)
            .build();
    try {

      cluster.waitActive();
      FileSystem fs = cluster.getFileSystem();
      Path inputFile = new Path("/my_temp");
      FSDataOutputStream fos = fs.create(inputFile);
      fos.close();
      Path physicalFile =
              makeJar(new Path(TEST_ROOT_DIR, "distributed.third.jar"), 1);
      c.set("tmpjars", "file:///" + physicalFile.toString());
      c.set(JTConfig.JT_IPC_ADDRESS, "local");
      Job job = Job.getInstance(c);
      job.setOutputFormatClass(NullOutputFormat.class);
      FileInputFormat.setInputPaths(job, inputFile);
      job.setMaxMapAttempts(1); // speed up failures
      job.submit();
      assertTrue(job.waitForCompletion(true));
    } finally {
      cluster.shutdown();
    }
  }
}
