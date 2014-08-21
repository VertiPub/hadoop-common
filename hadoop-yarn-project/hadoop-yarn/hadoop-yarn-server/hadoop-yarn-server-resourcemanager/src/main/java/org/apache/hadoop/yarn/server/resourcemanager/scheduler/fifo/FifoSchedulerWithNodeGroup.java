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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNodeWithNodeGroup;
import org.apache.hadoop.yarn.util.resource.Resources;

public class FifoSchedulerWithNodeGroup extends FifoScheduler {
  @Override
  protected int assignContainersOnNode(FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority) {
    // Data-local
    int nodeLocalContainers = 
        assignNodeLocalContainers(node, application, priority);

    // NodeGroup-local
    int nodegroupLocalContainers = 
      assignNodeGroupLocalContainers(node, application, priority);

    // Rack-local
    int rackLocalContainers = 
        assignRackLocalContainers(node, application, priority);

    // Off-switch
    int offSwitchContainers =
        assignOffSwitchContainers(node, application, priority);
    LOG.debug("assignContainersOnNode:" +
        " node=" + node.getRMNode().getNodeAddress() + 
        " application=" + application.getApplicationId().getId() +
        " priority=" + priority.getPriority() + 
        " #assigned=" + 
        (nodeLocalContainers + + nodegroupLocalContainers + 
            rackLocalContainers + offSwitchContainers));
    return (nodeLocalContainers + nodegroupLocalContainers
        + rackLocalContainers + offSwitchContainers);
  }

  private int assignNodeGroupLocalContainers(FiCaSchedulerNode node, 
      FiCaSchedulerApp application, Priority priority) {
    int assignedContainers = 0;
    if (!node.isNodeGroupAware()) {
      return 0;
    }
    if (node.getNodeGroupName() == null)
      return 0;
    ResourceRequest request = 
        application.getResourceRequest(priority, node.getNodeGroupName());
    if (request != null) {
    // Don't allocate on this nodegroup if the application doens't need containers on this rack
    ResourceRequest rackRequest =
        application.getResourceRequest(priority, node.getRackName());
    if (rackRequest.getNumContainers() <= 0) {
      return 0;
    }

    int assignableContainers = 
        Math.min(
            getMaxAllocatableContainers(application, priority, node, 
                NodeType.NODEGROUP_LOCAL), 
            request.getNumContainers());
    assignedContainers = 
        assignContainer(node, application, priority, 
            assignableContainers, request, NodeType.NODEGROUP_LOCAL);
    }
    return assignedContainers;
  }

  @Override
  protected synchronized void addNode(RMNode nodeManager) {
    this.nodes.put(nodeManager.getNodeID(), 
        new FiCaSchedulerNodeWithNodeGroup(nodeManager, usePortForNodeName));
    Resources.addTo(clusterResource, nodeManager.getTotalCapability());
  }
  
  @Override
  protected int getMaxAllocatableContainers(FiCaSchedulerApp application,
      Priority priority, FiCaSchedulerNode node, NodeType type) {
    ResourceRequest offSwitchRequest = 
      application.getResourceRequest(priority, ResourceRequest.ANY);
    int maxContainers = offSwitchRequest.getNumContainers();

    if (type == NodeType.OFF_SWITCH) {
      return maxContainers;
    }

    if (type == NodeType.RACK_LOCAL) {
      ResourceRequest rackLocalRequest = 
        application.getResourceRequest(priority, node.getRMNode().getRackName());
      if (rackLocalRequest == null) {
        return maxContainers;
      }

      maxContainers = Math.min(maxContainers, rackLocalRequest.getNumContainers());
    }
    
    if (type == NodeType.NODEGROUP_LOCAL) {
      ResourceRequest nodegroupLocalRequest = null;
      if (node.isNodeGroupAware()) {
        nodegroupLocalRequest = 
          application.getResourceRequest(priority, node.getNodeGroupName());
      }
      if (nodegroupLocalRequest == null) {
        return maxContainers;
      }
      maxContainers = Math.min(maxContainers, nodegroupLocalRequest.getNumContainers());
    }

    if (type == NodeType.NODE_LOCAL) {
      ResourceRequest nodeLocalRequest = 
        application.getResourceRequest(priority, node.getRMNode().getNodeAddress());
      if (nodeLocalRequest != null) {
        maxContainers = Math.min(maxContainers, nodeLocalRequest.getNumContainers());
      }
    }

    return maxContainers;
  }
}
