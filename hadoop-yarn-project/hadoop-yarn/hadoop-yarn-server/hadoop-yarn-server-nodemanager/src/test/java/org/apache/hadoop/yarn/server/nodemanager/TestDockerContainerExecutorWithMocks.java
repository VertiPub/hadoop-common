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

package org.apache.hadoop.yarn.server.nodemanager;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
public class TestDockerContainerExecutorWithMocks {

  private static final Log LOG = LogFactory
      .getLog(TestDockerContainerExecutorWithMocks.class);
  public static final String DOCKER_LAUNCH_COMMAND = "/bin/true";
  public static final String DOCKER_LAUNCH_ARGS = "-args";

  private DockerContainerExecutor mockExec = null;
  private final File mockParamFile = new File("./params.txt");
  private LocalDirsHandlerService dirsHandler;
  private Path workDir;
  private FileContext lfs;


  private void deleteMockParamFile() {
    if(mockParamFile.exists()) {
      mockParamFile.delete();
    }
  }
  
  private List<String> readMockParams() throws IOException {
    LinkedList<String> ret = new LinkedList<String>();
    LineNumberReader reader = new LineNumberReader(new FileReader(
        mockParamFile));
    String line;
    while((line = reader.readLine()) != null) {
      ret.add(line);
    }
    reader.close();
    return ret;
  }
  
  @Before
  public void setup() {
    assumeTrue(!Path.WINDOWS);
    File f = new File("./src/test/resources/mock-container-executor");
    if(!FileUtil.canExecute(f)) {
      FileUtil.setExecutable(f, true);
    }
    String executorPath = f.getAbsolutePath();
    Configuration conf = new Configuration();
    long time = System.currentTimeMillis();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, executorPath);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, "/tmp/nm-local-dir" + time);
    conf.set(YarnConfiguration.NM_LOG_DIRS, "/tmp/userlogs" + time);
    conf.set(ApplicationConstants.DOCKER_LAUNCH_COMMAND, DOCKER_LAUNCH_COMMAND);
    conf.set(ApplicationConstants.CONTAINER_ARGS, DOCKER_LAUNCH_ARGS);
    mockExec = new DockerContainerExecutor();
    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    mockExec.setConf(conf);
    lfs = null;
    try {
      lfs = FileContext.getLocalFSFileContext();
      workDir = new Path("/tmp/temp-"+ System.currentTimeMillis());
      lfs.mkdir(workDir, FsPermission.getDirDefault(), true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  @After
  public void tearDown() {
    deleteMockParamFile();
    try {
      lfs.delete(workDir, true);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  @Test
  public void testContainerLaunch() throws IOException {
    String appSubmitter = "nobody";
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";

    Container container = mock(Container.class, RETURNS_DEEP_STUBS);
    ContainerId cId = mock(ContainerId.class, RETURNS_DEEP_STUBS);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String,String>();
    
    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);
    when(cId.getApplicationAttemptId().getApplicationId().toString()).thenReturn(appId);
    when(cId.toString()).thenReturn(containerId);
    
    when(context.getEnvironment()).thenReturn(env);
    
    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");

    Path pidFile = new Path(workDir, "pid.txt");

    mockExec.activateContainer(cId, pidFile);
    int ret = mockExec.launchContainer(container, scriptPath, tokensPath,
        appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
        dirsHandler.getLogDirs());
    assertEquals(0, ret);
    //get the script
    Path wrapperScriptPath = new Path(workDir,
            Shell.appendScriptExtension(
                    DockerContainerExecutor.DOCKER_CONTAINER_EXECUTOR_SCRIPT));
    LineNumberReader lnr = new LineNumberReader(new FileReader(wrapperScriptPath.toString()));
    boolean cmdFound = false;
    List<String> localDirs = dirsToMount(dirsHandler.getLocalDirs());
    List<String> logDirs = dirsToMount(dirsHandler.getLogDirs());
    List<String> expectedCommands =  new ArrayList<String>(Arrays.asList(DOCKER_LAUNCH_COMMAND, containerId));
    expectedCommands.addAll(localDirs);
    expectedCommands.addAll(logDirs);
    String shellScript =  workDir + "/launch_container.sh";
    expectedCommands.addAll(Arrays.asList(DOCKER_LAUNCH_ARGS, ApplicationConstants.DEFAULT_CONTAINER_NAME,
            "/bin/bash","\"" + shellScript + "\""));
    while(lnr.ready()){
      String line = lnr.readLine();
      LOG.info("line: " + line);
      if (line.equals("#!/bin/bash") || line.isEmpty() || line.startsWith("echo $$") || line.startsWith("/bin/mv")){
        continue;
      }
      List<String> command = Arrays.asList(line.split(" "));

      assertEquals(expectedCommands, command);
      cmdFound = true;
    }
    assertTrue(cmdFound);
  }

  private List<String> dirsToMount(List<String> dirs) {
    List<String> localDirs = new ArrayList<String>();
    for(String dir: dirs){
      localDirs.add("-v");
      localDirs.add(dir + ":" + dir);
    }
    return localDirs;
  }


  @Test (timeout = 5000)
  public void testStartLocalizer() throws IOException {

  
    InetSocketAddress address = InetSocketAddress.createUnresolved("localhost", 8040);
    Path nmPrivateCTokensPath= new Path("file:///bin/nmPrivateCTokensPath");
 
    try {
      mockExec.startLocalizer(nmPrivateCTokensPath, address, "test", "application_0", "12345", dirsHandler.getLocalDirs(), dirsHandler.getLogDirs());
      List<String> result=readMockParams();
      Assert.assertEquals(result.size(), 16);
      Assert.assertEquals(result.get(0), "test");
      Assert.assertEquals(result.get(1), "0" );
      Assert.assertEquals(result.get(2),"application_0" );
      Assert.assertEquals(result.get(3), "/bin/nmPrivateCTokensPath");
      Assert.assertEquals(result.get(7), "-classpath" );
      Assert.assertEquals(result.get(10),"org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer" );
      Assert.assertEquals(result.get(11), "test");
      Assert.assertEquals(result.get(12), "application_0");
      Assert.assertEquals(result.get(13),"12345" );
      Assert.assertEquals(result.get(14),"localhost" );
      Assert.assertEquals(result.get(15),"8040" );

    } catch (InterruptedException e) {
      LOG.error("Error:"+e.getMessage(),e);
      Assert.fail();
    }
  }
  
  
  @Test
  public void testContainerLaunchError() throws IOException {

    // reinitialize executer
    File f = new File("./src/test/resources/mock-container-executer-with-error");
    if (!FileUtil.canExecute(f)) {
      FileUtil.setExecutable(f, true);
    }
    String executorPath = f.getAbsolutePath();
    Configuration conf = new Configuration();
    conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, executorPath);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, "file:///bin/echo");
    conf.set(YarnConfiguration.NM_LOG_DIRS, "file:///dev/null");

    mockExec = new DockerContainerExecutor();
    dirsHandler = new LocalDirsHandlerService();
    dirsHandler.init(conf);
    mockExec.setConf(conf);

    String appSubmitter = "nobody";
    String cmd = ApplicationConstants.DEFAULT_DOCKER_LAUNCH_COMMAND;
    String appId = "APP_ID";
    String containerId = "CONTAINER_ID";
    Container container = mock(Container.class);
    ContainerId cId = mock(ContainerId.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String, String>();

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);

    when(cId.toString()).thenReturn(containerId);

    when(context.getEnvironment()).thenReturn(env);

    Path scriptPath = new Path("file:///bin/echo");
    Path tokensPath = new Path("file:///dev/null");
    Path workDir = new Path("/tmp");
    Path pidFile = new Path(workDir, "pid.txt");

    mockExec.activateContainer(cId, pidFile);
    int ret = mockExec.launchContainer(container, scriptPath, tokensPath,
        appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
        dirsHandler.getLogDirs());
    Assert.assertNotSame(0, ret);
    assertEquals(Arrays.asList(appSubmitter, cmd, appId, containerId,
        workDir.toString(), "/bin/echo", "/dev/null", pidFile.toString(),
        StringUtils.join(",", dirsHandler.getLocalDirs()),
        StringUtils.join(",", dirsHandler.getLogDirs()),
        "cgroups=none"), readMockParams());

  }
  
  @Test
  public void testInit() throws Exception {

    mockExec.init();
    assertEquals(Arrays.asList("--checksetup"), readMockParams());
    
  }

  
  @Test
  public void testContainerKill() throws IOException {
    String appSubmitter = "nobody";
    String cmd = String.valueOf(
        LinuxContainerExecutor.Commands.SIGNAL_CONTAINER.getValue());
    ContainerExecutor.Signal signal = ContainerExecutor.Signal.QUIT;
    String sigVal = String.valueOf(signal.getValue());
    
    mockExec.signalContainer(appSubmitter, "1000", signal);
    assertEquals(Arrays.asList(appSubmitter, cmd, "1000", sigVal),
        readMockParams());
  }
  
//  @Test
//  public void testDeleteAsUser() throws IOException {
//    String appSubmitter = "nobody";
//    String cmd = String.valueOf(
//        LinuxContainerExecutor.Commands.DELETE_AS_USER.getValue());
//    Path dir = new Path("/tmp/testdir");
//
//    mockExec.deleteAsUser(appSubmitter, dir);
//    assertEquals(Arrays.asList(appSubmitter, cmd, "/tmp/testdir"),
//        readMockParams());
//  }
}
