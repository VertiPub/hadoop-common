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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import java.util.Comparator;

import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueueWithNodeGroup;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNodeWithNodeGroup;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNodeWithNodeGroup;

/**
 * This is an implementation factory for constructing scheduler related objects
 * on environment with nodegroup layer.
 */
public class SchedulerElementsFactoryWithNodeGroup extends
    AbstractSchedulerElementsFactory {

  @Override
  public FiCaSchedulerNode constructFiCaSchedulerNode(RMNode node, boolean usePortForNodeName) {
    return new FiCaSchedulerNodeWithNodeGroup(node, usePortForNodeName);
  }

  @Override
  public FSSchedulerNode constructFSSchedulerNode(RMNode node, boolean usePortForNodeName) {
    return new FSSchedulerNodeWithNodeGroup(node, usePortForNodeName);
  }

  @Override
  public LeafQueue constructLeafQueue(CapacitySchedulerContext cs, String queueName,
      CSQueue parent, Comparator<FiCaSchedulerApp> applicationComparator,
      CSQueue old) {
    return new LeafQueueWithNodeGroup(cs, queueName, parent, old);
  }

}
