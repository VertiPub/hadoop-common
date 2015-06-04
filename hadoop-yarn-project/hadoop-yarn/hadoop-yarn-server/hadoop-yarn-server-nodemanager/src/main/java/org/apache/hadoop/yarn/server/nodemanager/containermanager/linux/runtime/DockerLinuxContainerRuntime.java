/*
 * *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * /
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperation;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerModule;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerClient;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.docker.DockerRunCommand;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.LinuxContainerRuntimeConstants.*;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DockerLinuxContainerRuntime implements LinuxContainerRuntime {
  private static final Log LOG = LogFactory.getLog(
      DockerLinuxContainerRuntime.class);
  private static final String ENV_CONTAINER_TYPE =
      "yarn.container-runtime.type";
  private static final String ENV_DOCKER_CONTAINER_IMAGE =
      "yarn.container-runtime.docker.image";
  private static final String ENV_DOCKER_CONTAINER_IMAGE_FILE =
      "yarn.container-runtime.docker.image-file";
  private static final String ENV_DOCKER_CONTAINER_DISABLE_RUN_OVERRIDE =
      "yarn.container-runtime.docker.run.override.disable";

  private Configuration conf;
  private DockerClient dockerClient;

  public static boolean isDockerContainerRequested(
      Map<String, String> env) {
    if (env == null) {
      return false;
    }

    String type = env.get(ENV_CONTAINER_TYPE);

    return type != null && type.equals("docker");
  }

  @Override
  public void initialize(Configuration conf)
      throws ContainerExecutionException {
    this.conf = conf;
    dockerClient = new DockerClient(conf);
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

  }

  //hack
  public void overwriteLaunchScript(OutputStream out,
      Map<String, String> environment, Map<Path, List<String>> resources,
      List<String> command) throws IOException {
    ContainerLaunch.ShellScriptBuilder sb = ContainerLaunch.ShellScriptBuilder.create();

    Set<String> exclusionSet = new HashSet<String>();
    exclusionSet.add(YarnConfiguration.NM_DOCKER_CONTAINER_EXECUTOR_IMAGE_NAME);
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_YARN_HOME.name());
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_COMMON_HOME.name());
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_HDFS_HOME.name());
    exclusionSet.add(ApplicationConstants.Environment.HADOOP_CONF_DIR.name());
    exclusionSet.add(ApplicationConstants.Environment.JAVA_HOME.name());
    exclusionSet.add(ENV_CONTAINER_TYPE);
    exclusionSet.add(ENV_DOCKER_CONTAINER_IMAGE);
    exclusionSet.add(ENV_DOCKER_CONTAINER_IMAGE_FILE);
    exclusionSet.add(ENV_DOCKER_CONTAINER_DISABLE_RUN_OVERRIDE);

    if (environment != null) {
      for (Map.Entry<String,String> env : environment.entrySet()) {
        if (!exclusionSet.contains(env.getKey())) {
          sb.env(env.getKey().toString(), env.getValue().toString());
        }
      }
    }

    if (resources != null) {
      for (Map.Entry<Path,List<String>> entry : resources.entrySet()) {
        for (String linkName : entry.getValue()) {
          sb.symlink(entry.getKey(), new Path(linkName));
        }
      }
    }

    sb.command(command);

    PrintStream pout = null;
    PrintStream ps = null;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      pout = new PrintStream(out, false, "UTF-8");
      if (LOG.isDebugEnabled()) {
        ps = new PrintStream(baos, false, "UTF-8");
        sb.write(ps);
      }
      sb.write(pout);

    } finally {
      if (out != null) {
        out.close();
      }
      if (ps != null) {
        ps.close();
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Script: " + baos.toString("UTF-8"));
    }
  }

  public void addCGroupParentIfRequired(String resourcesOptions,
      String containerIdStr, DockerRunCommand runCommand)
      throws ContainerExecutionException {
    if (resourcesOptions.equals(
        (PrivilegedOperation.CGROUP_ARG_PREFIX + PrivilegedOperation
            .CGROUP_ARG_NO_TASKS))) {
      if (LOG.isInfoEnabled()) {
        LOG.info("no resource restrictions specified. not using docker's "
            + "cgroup options");
      }
    } else {
      if (LOG.isInfoEnabled()) {
        LOG.info("using docker's cgroups options");
      }

      try {
        CGroupsHandler cGroupsHandler = ResourceHandlerModule
            .getCGroupsHandler(conf);
        String cGroupPath = "/" + cGroupsHandler.getRelativePathForCGroup(
            containerIdStr);

        if (LOG.isInfoEnabled()) {
          LOG.info("using cgroup parent: " + cGroupPath);
        }

        runCommand.setCGroupParent(cGroupPath);
      } catch (ResourceHandlerException e) {
        LOG.warn("unable to use cgroups handler. Exception: ", e);
        throw new ContainerExecutionException(e);
      }
    }
  }



  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    Map<String, String> environment = container.getLaunchContext()
        .getEnvironment();
    String imageName = environment.get(ENV_DOCKER_CONTAINER_IMAGE);

    if (imageName == null) {
      throw new ContainerExecutionException(ENV_DOCKER_CONTAINER_IMAGE
          + " not set!");
    }

    String containerIdStr = container.getContainerId().toString();
    String runAsUser = ctx.getExecutionAttribute(RUN_AS_USER);
    Path containerWorkDir = ctx.getExecutionAttribute(CONTAINER_WORK_DIR);
    //List<String> -> stored as List -> fetched/converted to List<String>
    //we can't do better here thanks to type-erasure
    @SuppressWarnings("unchecked")
    List<String> localDirs = ctx.getExecutionAttribute(LOCAL_DIRS);
    @SuppressWarnings("unchecked")
    List<String> logDirs = ctx.getExecutionAttribute(LOG_DIRS);
    @SuppressWarnings("unchecked")
    Map<Path, List<String>> localizedResources = ctx.getExecutionAttribute(
        LOCALIZED_RESOURCES);
    DockerRunCommand runCommand = new DockerRunCommand(containerIdStr,
        runAsUser, imageName)
        .detachOnRun()
        .setContainerWorkDir(containerWorkDir.toString())
        .setNetworkType("host")
        .addMountLocation("/etc/passwd", "/etc/password:ro");
    List<String> allDirs = new ArrayList<>(localDirs);

    allDirs.add(containerWorkDir.toString());
    allDirs.addAll(logDirs);
    for (String dir: allDirs) {
      runCommand.addMountLocation(dir, dir);
    }

    String resourcesOpts = ctx.getExecutionAttribute(RESOURCES_OPTIONS);

    //addCGroupParentIfRequired(resourcesOpts, containerIdStr, runCommand);

    try {
      //hack - we'll need to overwrite the launch script, for the time being
      Path nmPrivateContainerScriptPath = ctx.getExecutionAttribute(
          NM_PRIVATE_CONTAINER_SCRIPT_PATH);
      FileContext lfs = FileContext.getLocalFSFileContext();
      OutputStream containerScriptOutStream = lfs.create(
          nmPrivateContainerScriptPath, EnumSet.of(CREATE, OVERWRITE));

      overwriteLaunchScript(containerScriptOutStream, environment,
          localizedResources, container.getLaunchContext().getCommands());

      String disableOverride = environment.get(
          ENV_DOCKER_CONTAINER_DISABLE_RUN_OVERRIDE);

      if (disableOverride != null && disableOverride.equals("true")) {
        if (LOG.isInfoEnabled()) {
          LOG.info("command override disabled");
        }
      } else {
        List<String> overrideCommands = new ArrayList<>();
        Path launchDst =
            new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);

        overrideCommands.add("bash");
        overrideCommands.add(launchDst.toUri().getPath());
        runCommand.setOverrideCommandWithArgs(overrideCommands);
      }

      String commandFile = dockerClient.writeCommandToTempFile(runCommand,
          containerIdStr);
      PrivilegedOperation launchOp = new PrivilegedOperation(
          PrivilegedOperation.OperationType.LAUNCH_DOCKER_CONTAINER, (String)
          null);

      launchOp.appendArgs(runAsUser, ctx.getExecutionAttribute(USER),
          Integer.toString(Commands.LAUNCH_DOCKER_CONTAINER.getValue()),
          ctx.getExecutionAttribute(APPID),
          containerIdStr, containerWorkDir.toString(),
          nmPrivateContainerScriptPath.toUri().getPath(),
          ctx.getExecutionAttribute(NM_PRIVATE_TOKENS_PATH).toUri().getPath(),
          ctx.getExecutionAttribute(PID_FILE_PATH).toString(),
          StringUtils.join(",", localDirs),
          StringUtils.join(",", logDirs),
          commandFile,
          resourcesOpts);

      String tcCommandFile = ctx.getExecutionAttribute(TC_COMMAND_FILE);

      if (tcCommandFile != null) {
        launchOp.appendArgs(tcCommandFile);
      }

      try {
        PrivilegedOperationExecutor executor = PrivilegedOperationExecutor
            .getInstance(conf);

        executor.executePrivilegedOperation(null,
            launchOp, null, container.getLaunchContext().getEnvironment(),
            false);
      } catch (PrivilegedOperationException e) {
        LOG.warn("Launch container failed. Exception: ", e);

        throw new ContainerExecutionException("Launch container failed", e
            .getExitCode(), e.getOutput(), e.getErrorOutput());
      }
    } catch (IOException e) {
      throw new ContainerExecutionException(e);
    }
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    PrivilegedOperation signalOp = new PrivilegedOperation(
        PrivilegedOperation.OperationType.SIGNAL_CONTAINER, (String) null);

    signalOp.appendArgs(ctx.getExecutionAttribute(RUN_AS_USER),
        ctx.getExecutionAttribute(USER),
        Integer.toString(Commands.SIGNAL_CONTAINER.getValue()),
        ctx.getExecutionAttribute(PID),
        Integer.toString(ctx.getExecutionAttribute(SIGNAL).getValue()));

    try {
      PrivilegedOperationExecutor executor = PrivilegedOperationExecutor
          .getInstance(conf);

      executor.executePrivilegedOperation(null,
          signalOp, null, container.getLaunchContext().getEnvironment(),
          false);
    } catch (PrivilegedOperationException e) {
      LOG.warn("Signal container failed. Exception: ", e);

      throw new ContainerExecutionException("Signal container failed", e
          .getExitCode(), e.getOutput(), e.getErrorOutput());
    }
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {

  }
}