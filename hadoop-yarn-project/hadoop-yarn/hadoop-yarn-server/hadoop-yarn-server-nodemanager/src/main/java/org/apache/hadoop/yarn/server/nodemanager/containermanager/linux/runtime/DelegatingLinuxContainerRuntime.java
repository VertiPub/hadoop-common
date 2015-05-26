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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerExecutionException;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.runtime.ContainerRuntimeContext;

import java.util.Map;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class DelegatingLinuxContainerRuntime implements LinuxContainerRuntime {
  private StandardLinuxContainerRuntime standardLinuxContainerRuntime;
  private DockerLinuxContainerRuntime dockerLinuxContainerRuntime;

  @Override
  public void initialize(Configuration conf)
      throws ContainerExecutionException {
    standardLinuxContainerRuntime = new StandardLinuxContainerRuntime();
    standardLinuxContainerRuntime.initialize(conf);
    dockerLinuxContainerRuntime = new DockerLinuxContainerRuntime();
    dockerLinuxContainerRuntime.initialize(conf);
  }

  private LinuxContainerRuntime pickContainerRuntime(Container container) {
    Map<String, String> env = container.getLaunchContext().getEnvironment();

    if (DockerLinuxContainerRuntime.isDockerContainerRequested(env)){
      return dockerLinuxContainerRuntime;
    }

    return standardLinuxContainerRuntime;
  }

  @Override
  public void prepareContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.prepareContainer(ctx);
  }

  @Override
  public void launchContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.launchContainer(ctx);
  }

  @Override
  public void signalContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.signalContainer(ctx);
  }

  @Override
  public void reapContainer(ContainerRuntimeContext ctx)
      throws ContainerExecutionException {
    Container container = ctx.getContainer();
    LinuxContainerRuntime runtime = pickContainerRuntime(container);

    runtime.reapContainer(ctx);
  }
}