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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerDiagnosticsUpdateEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.launcher.ContainerLaunch;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;

public class DockerContainerExecutor extends DefaultContainerExecutor {

private static final Log LOG = LogFactory
        .getLog(DockerContainerExecutor.class);

private final FileContext lfs;

public DockerContainerExecutor() {
  try {
    this.lfs = FileContext.getLocalFSFileContext();
  } catch (UnsupportedFileSystemException e) {
    throw new RuntimeException(e);
  }
}

DockerContainerExecutor(FileContext lfs) {
  this.lfs = lfs;
}

  @Override
public int launchContainer(Container container,
                           Path nmPrivateContainerScriptPath, Path nmPrivateTokensPath,
                           String userName, String appId, Path containerWorkDir,
                           List<String> localDirs, List<String> logDirs) throws IOException {

  String containerName = getConf().get(ApplicationConstants.CONTAINER_NAME, ApplicationConstants.DEFAULT_CONTAINER_NAME);
  String containerArgs = Strings.nullToEmpty(getConf().get(ApplicationConstants.CONTAINER_ARGS));
  // This needs to happen because the application master that resides on a separate container would not
    // be able to speak to a regular docker container without some network bridging.
  if (getConf().getBoolean(ApplicationConstants.APPLICATION_MASTER_CONTAINER, false)){
    if (LOG.isDebugEnabled()) {
      LOG.debug("Launching application master container with Default container executor");
    }
    getConf().setBoolean(ApplicationConstants.APPLICATION_MASTER_CONTAINER, false);
    return super.launchContainer(container, nmPrivateContainerScriptPath, nmPrivateTokensPath,
          userName, appId, containerWorkDir, localDirs, logDirs);
  }
  FsPermission dirPerm = new FsPermission(APPDIR_PERM);
  ContainerId containerId = container.getContainerId();

  // create container dirs on all disks
  String containerIdStr = ConverterUtils.toString(containerId);
  String appIdStr =
          ConverterUtils.toString(
                  containerId.getApplicationAttemptId().
                          getApplicationId());
  for (String sLocalDir : localDirs) {
    Path usersdir = new Path(sLocalDir, ContainerLocalizer.USERCACHE);
    Path userdir = new Path(usersdir, userName);
    Path appCacheDir = new Path(userdir, ContainerLocalizer.APPCACHE);
    Path appDir = new Path(appCacheDir, appIdStr);
    Path containerDir = new Path(appDir, containerIdStr);
    createDir(containerDir, dirPerm, true);
  }

  // Create the container log-dirs on all disks
  createContainerLogDirs(appIdStr, containerIdStr, logDirs);

  Path tmpDir = new Path(containerWorkDir,
          YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR);
  createDir(tmpDir, dirPerm, false);

  // copy launch script to work dir
  Path launchDst =
          new Path(containerWorkDir, ContainerLaunch.CONTAINER_SCRIPT);
  lfs.util().copy(nmPrivateContainerScriptPath, launchDst);

  // copy container tokens to work dir
  Path tokenDst =
          new Path(containerWorkDir, ContainerLaunch.FINAL_CONTAINER_TOKENS_FILE);
  lfs.util().copy(nmPrivateTokensPath, tokenDst);

  // Create new local launch wrapper script
  LocalWrapperScriptBuilder sb =
          new UnixLocalWrapperScriptBuilder(containerWorkDir);

  String localDirMount = toMount(localDirs);
  String logDirMount = toMount(logDirs);
  StringBuilder commands = new StringBuilder();
  String commandStr = commands.append("docker -H tcp://0.0.0.0:4243 run -rm -name ")
          .append(containerIdStr)
          .append(localDirMount)
          .append(logDirMount)
          .append(" ")
          .append(containerArgs)
          .append(" ")
          .append(containerName)
          .toString();
  Path pidFile = getPidFilePath(containerId);
  if (pidFile != null) {
    sb.writeLocalWrapperScript(launchDst, pidFile, commandStr);
  } else {
    LOG.info("Container " + containerIdStr
            + " was marked as inactive. Returning terminated error");
    return ExitCode.TERMINATED.getExitCode();
  }

  // create log dir under app
  // fork script
  ShellCommandExecutor shExec = null;
  try {
    lfs.setPermission(launchDst,
            ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);
    lfs.setPermission(sb.getWrapperScriptPath(),
            ContainerExecutor.TASK_LAUNCH_SCRIPT_PERMISSION);

    // Setup command to run
    String[] command = getRunCommand(sb.getWrapperScriptPath().toString(),
            containerIdStr, this.getConf());
    if (LOG.isInfoEnabled()) {
      LOG.info("launchContainer: " + commandStr + " " + Joiner.on(" ").join(command));
    }
    shExec = new ShellCommandExecutor(
            command,
            new File(containerWorkDir.toUri().getPath()),
            container.getLaunchContext().getEnvironment());      // sanitized env
    if (isContainerActive(containerId)) {
      shExec.execute();
    }
    else {
      LOG.info("Container " + containerIdStr +
              " was marked as inactive. Returning terminated error");
      return ExitCode.TERMINATED.getExitCode();
    }
  } catch (IOException e) {
    if (null == shExec) {
      return -1;
    }
    int exitCode = shExec.getExitCode();
    LOG.warn("Exit code from container " + containerId + " is : " + exitCode);
    // 143 (SIGTERM) and 137 (SIGKILL) exit codes means the container was
    // terminated/killed forcefully. In all other cases, log the
    // container-executor's output
    if (exitCode != ExitCode.FORCE_KILLED.getExitCode()
            && exitCode != ExitCode.TERMINATED.getExitCode()) {
      LOG.warn("Exception from container-launch with container ID: "
              + containerId + " and exit code: " + exitCode , e);
      logOutput(shExec.getOutput());
      String diagnostics = "Exception from container-launch: \n"
              + StringUtils.stringifyException(e) + "\n" + shExec.getOutput();
      container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
              diagnostics));
    } else {
      container.handle(new ContainerDiagnosticsUpdateEvent(containerId,
              "Container killed on request. Exit code is " + exitCode));
    }
    return exitCode;
  } finally {
    ; //
  }
  return 0;
}

  private String toMount(List<String> dirs) {
    StringBuilder builder = new StringBuilder();
    for (String dir: dirs){
      builder.append(" -v " + dir + ":" + dir);
    }
    return builder.toString();
  }

  private abstract class LocalWrapperScriptBuilder {

  private final Path wrapperScriptPath;

  public Path getWrapperScriptPath() {
    return wrapperScriptPath;
  }

  public void writeLocalWrapperScript(Path launchDst, Path pidFile, String commandStr) throws IOException {
    DataOutputStream out = null;
    PrintStream pout = null;

    try {
      out = lfs.create(wrapperScriptPath, EnumSet.of(CREATE, OVERWRITE));
      pout = new PrintStream(out);
      writeLocalWrapperScript(launchDst, pidFile, pout, commandStr);
    } finally {
      IOUtils.cleanup(LOG, pout, out);
    }
  }

  protected abstract void writeLocalWrapperScript(Path launchDst, Path pidFile,
                                                  PrintStream pout, String commandStr);

  protected LocalWrapperScriptBuilder(Path containerWorkDir) {
    this.wrapperScriptPath = new Path(containerWorkDir,
            Shell.appendScriptExtension("default_container_executor"));
  }
}

  private final class UnixLocalWrapperScriptBuilder
          extends LocalWrapperScriptBuilder {

    public UnixLocalWrapperScriptBuilder(Path containerWorkDir) {
      super(containerWorkDir);
    }

    @Override
    public void writeLocalWrapperScript(Path launchDst, Path pidFile,
                                        PrintStream pout, String commandStr) {

      // We need to do a move as writing to a file is not atomic
      // Process reading a file being written to may get garbled data
      // hence write pid to tmp file first followed by a mv
      pout.println("#!/bin/bash");
      pout.println();

      pout.println("echo $$ > " + pidFile.toString() + ".tmp");
      pout.println("/bin/mv -f " + pidFile.toString() + ".tmp " + pidFile);
      String exec = commandStr;
      pout.println(exec + " /bin/bash \"" +
              launchDst.toUri().getPath().toString() + "\"");
    }
  }

  private void createDir(Path dirPath, FsPermission perms,
                         boolean createParent) throws IOException {
    lfs.mkdir(dirPath, perms, createParent);
    if (!perms.equals(perms.applyUMask(lfs.getUMask()))) {
      lfs.setPermission(dirPath, perms);
    }
  }
}