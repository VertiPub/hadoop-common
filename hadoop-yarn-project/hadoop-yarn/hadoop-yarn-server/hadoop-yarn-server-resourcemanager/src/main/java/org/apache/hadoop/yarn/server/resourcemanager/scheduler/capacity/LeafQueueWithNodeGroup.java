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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.Comparator;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

public class LeafQueueWithNodeGroup extends LeafQueue {

  public LeafQueueWithNodeGroup(CapacitySchedulerContext cs,
      String queueName, CSQueue parent,
      CSQueue old) {
    super(cs, queueName, parent, old);
  }

  @Override
  protected CSAssignment assignContainersOnNode(Resource clusterResource, 
      FiCaSchedulerNode node, FiCaSchedulerApp application, 
      Priority priority, RMContainer reservedContainer) {

    Resource assigned = Resources.none();

    // Data-local
    ResourceRequest nodeLocalResourceRequest =
        application.getResourceRequest(priority, node.getNodeName());
    if (nodeLocalResourceRequest != null) {
      assigned = 
          assignNodeLocalContainers(clusterResource, nodeLocalResourceRequest, 
              node, application, priority, reservedContainer); 
      if (Resources.greaterThan(resourceCalculator, clusterResource,
          assigned, Resources.none())) {
        return new CSAssignment(assigned, NodeType.NODE_LOCAL);
      }
    }

    // NodeGroup-local
    ResourceRequest nodeGroupLocalResourceRequest = null;
    if (node.isNodeGroupAware()) {
      nodeGroupLocalResourceRequest = application.getResourceRequest(
          priority, node.getNodeGroupName());
    }

    if (nodeGroupLocalResourceRequest != null){
      assigned = 
          assignNodeGroupLocalContainers(clusterResource, nodeGroupLocalResourceRequest,
              node, application, priority, reservedContainer); 
      if (Resources.greaterThan(resourceCalculator, clusterResource,
          assigned, Resources.none())) {
        return new CSAssignment(assigned, NodeType.NODEGROUP_LOCAL);
      }
    }

    // Rack-local
    ResourceRequest rackLocalResourceRequest =
        application.getResourceRequest(priority, node.getRackName());
    if (rackLocalResourceRequest != null) {
      if (!rackLocalResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }

      assigned = 
          assignRackLocalContainers(clusterResource, rackLocalResourceRequest,
              node, application, priority, reservedContainer);
      if (Resources.greaterThan(resourceCalculator, clusterResource,
          assigned, Resources.none())) {
        return new CSAssignment(assigned, NodeType.RACK_LOCAL);
      }
    }

    // Off-switch
    ResourceRequest offSwitchResourceRequest =
        application.getResourceRequest(priority, ResourceRequest.ANY);
    if (offSwitchResourceRequest != null) {
      if (!offSwitchResourceRequest.getRelaxLocality()) {
        return SKIP_ASSIGNMENT;
      }

      return new CSAssignment(
          assignOffSwitchContainers(clusterResource, offSwitchResourceRequest,
              node, application, priority, reservedContainer), 
              NodeType.OFF_SWITCH);
    }

    return SKIP_ASSIGNMENT;
  }

  private Resource assignNodeGroupLocalContainers(
      Resource clusterResource, ResourceRequest nodeGroupLocalResourceRequest,
      FiCaSchedulerNode node, FiCaSchedulerApp application,
      Priority priority, RMContainer reservedContainer) {
    if (canAssign(application, priority, node, NodeType.NODEGROUP_LOCAL,
        reservedContainer)) {
      return assignContainer(clusterResource, node, application, priority,
          nodeGroupLocalResourceRequest, NodeType.NODEGROUP_LOCAL, reservedContainer);
    }
    return Resources.none();
  }


  @Override 
  boolean canAssign(FiCaSchedulerApp application, Priority priority, 
      FiCaSchedulerNode node, NodeType type, RMContainer reservedContainer) {

    // Reserved... 
    if (reservedContainer != null) {
      return true;
    }

    // Check if we need containers on this nodegroup
    if (type == NodeType.NODEGROUP_LOCAL) {
      // Now check if we need containers on this nodegroup...
      if (node.isNodeGroupAware()) {
        ResourceRequest nodegroupLocalRequest = 
            application.getResourceRequest(priority, node.getNodeGroupName());
        if (nodegroupLocalRequest != null) {
          return nodegroupLocalRequest.getNumContainers() > 0;
        }
      }
    }

    return super.canAssign(application, priority, node, type, null); 
  }

}
