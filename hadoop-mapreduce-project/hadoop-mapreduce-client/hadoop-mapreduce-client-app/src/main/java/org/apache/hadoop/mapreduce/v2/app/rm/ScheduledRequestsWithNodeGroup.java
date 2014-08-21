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

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.util.Iterator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.JobCounter;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.event.JobCounterUpdateEvent;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator.ScheduledRequests;
import org.apache.hadoop.mapreduce.v2.app.rm.RMContainerRequestor.ContainerRequest;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.RackResolver;

public class ScheduledRequestsWithNodeGroup extends ScheduledRequests {

  private final Map<String, LinkedList<TaskAttemptId>> mapsNodeGroupMapping = 
      new HashMap<String, LinkedList<TaskAttemptId>>();

  ScheduledRequestsWithNodeGroup(RMContainerAllocator rmContainerAllocator, 
      RMContainerAllocator rmContainerAllocator2) {
    rmContainerAllocator.super(rmContainerAllocator);
  }

  @Override
  void addMap(ContainerRequestEvent event) {
    ContainerRequest request = null;

    if (event.getEarlierAttemptFailed()) {
      earlierFailedMaps.add(event.getAttemptID());
      request = new ContainerRequest(event, RMContainerAllocator.PRIORITY_FAST_FAIL_MAP);
      RMContainerAllocator.LOG.info("Added "+event.getAttemptID()+" to list of failed maps");
    } else {
      for (String host : event.getHosts()) {
        LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
        if (list == null) {
          list = new LinkedList<TaskAttemptId>();
          mapsHostMapping.put(host, list);
        }
        list.add(event.getAttemptID());
        if (RMContainerAllocator.LOG.isDebugEnabled()) {
          RMContainerAllocator.LOG.debug("Added attempt req to host " + host);
        }
      }

      doNodeGroupMapping(event);

      for (String rack: event.getRacks()) {
        LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
        if (list == null) {
          list = new LinkedList<TaskAttemptId>();
          mapsRackMapping.put(rack, list);
        }
        list.add(event.getAttemptID());
        if (RMContainerAllocator.LOG.isDebugEnabled()) {
          RMContainerAllocator.LOG.debug("Added attempt req to rack " + rack);
        }
      }
      request = new ContainerRequest(event, RMContainerAllocator.PRIORITY_MAP);
    }
    maps.put(event.getAttemptID(), request);
    this.rmContainerAllocator.addContainerReq(request);
  }

  @SuppressWarnings("unchecked")
  protected ContainerRequest assignToNodeGroup(String host, LinkedList<TaskAttemptId> list, Container allocated) {
    ContainerRequest assigned = null;
    // Try to assign nodegroup-local
    String nodegroup = NetworkTopology.getLastHalf(RackResolver.resolve(host).getNetworkLocation());
    if (nodegroup != null)  
      list = mapsNodeGroupMapping.get(nodegroup);
    while (list != null && list.size() > 0) {
      TaskAttemptId tId = list.removeFirst();
      if (maps.containsKey(tId)) {
        assigned = maps.remove(tId);
        containerAssigned(allocated, assigned);
        JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
        jce.addCounterUpdate(JobCounter.NODEGROUP_LOCAL_MAPS, 1);
        this.rmContainerAllocator.eventHandler.handle(jce);
        this.rmContainerAllocator.nodegroupLocalAssigned++;
        RMContainerAllocator.LOG.info("Assigned based on nodegroup match " + nodegroup);
        break;
      }
    }  
    return assigned;
  }

  protected void doNodeGroupMapping(ContainerRequestEvent event) {
    if (event instanceof ContainerRequestWithNodeGroupEvent) {
      for (String nodegroup: ((ContainerRequestWithNodeGroupEvent)event).getNodeGroups()) {
        LinkedList<TaskAttemptId> list = mapsNodeGroupMapping.get(nodegroup);
        if (list == null) {
          list = new LinkedList<TaskAttemptId>();
          mapsNodeGroupMapping.put(nodegroup, list);
        }
        list.add(event.getAttemptID());
        RMContainerAllocator.LOG.info("Added attempt req to nodegroup " + nodegroup);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void assignMapsWithLocality(List<Container> allocatedContainers) {
    // try to assign to all nodes first to match node local
    // first by host, then by nodegroup, rack, followed by *  
    Iterator<Container> it = allocatedContainers.iterator();
    while(it.hasNext() && maps.size() > 0){
      Container allocated = it.next();
      Priority priority = allocated.getPriority();
      assert this.rmContainerAllocator.PRIORITY_MAP.equals(priority);
      // "if (maps.containsKey(tId))" below should be almost always true.
      // hence this while loop would almost always have O(1) complexity
      String host = allocated.getNodeId().getHost();
      LinkedList<TaskAttemptId> list = mapsHostMapping.get(host);
      while (list != null && list.size() > 0) {
        if (RMContainerAllocator.LOG.isDebugEnabled()) {
          RMContainerAllocator.LOG.debug("Host matched to the request list " + host);
        }
        TaskAttemptId tId = list.removeFirst();
        if (maps.containsKey(tId)) {
          ContainerRequest assigned = maps.remove(tId);
          containerAssigned(allocated, assigned);
          it.remove();
          JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.DATA_LOCAL_MAPS, 1);
          this.rmContainerAllocator.eventHandler.handle(jce);
          this.rmContainerAllocator.hostLocalAssigned++;
          if (RMContainerAllocator.LOG.isDebugEnabled()) {
            RMContainerAllocator.LOG.debug("Assigned based on host match " + host);
          }
          break;
        }
      }
    }
    
    // try to match all node-group local
    it = allocatedContainers.iterator();
    while(it.hasNext() && maps.size() > 0){
      Container allocated = it.next();
      Priority priority = allocated.getPriority();
      assert this.rmContainerAllocator.PRIORITY_MAP.equals(priority);
      // "if (maps.containsKey(tId))" below should be almost always true.
      // hence this while loop would almost always have O(1) complexity
      String host = allocated.getNodeId().getHost();
      String nodegroup = NetworkTopology.getLastHalf(RackResolver.resolve(host).getNetworkLocation());
      LinkedList<TaskAttemptId> list = mapsNodeGroupMapping.get(nodegroup);
      while (list != null && list.size() > 0) {
        TaskAttemptId tId = list.removeFirst();
        if (maps.containsKey(tId)) {
          ContainerRequest assigned = maps.remove(tId);
          containerAssigned(allocated, assigned);
          it.remove();
          JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.NODEGROUP_LOCAL_MAPS, 1);
          this.rmContainerAllocator.eventHandler.handle(jce);
          this.rmContainerAllocator.nodegroupLocalAssigned++;
          if (RMContainerAllocator.LOG.isDebugEnabled()) {
            RMContainerAllocator.LOG.debug("Assigned based on NodeGroup match " + nodegroup);
          }
          break;
        }
      }
    }
    
    // try to match all rack local
    it = allocatedContainers.iterator();
    while(it.hasNext() && maps.size() > 0){
      Container allocated = it.next();
      Priority priority = allocated.getPriority();
      assert this.rmContainerAllocator.PRIORITY_MAP.equals(priority);
      // "if (maps.containsKey(tId))" below should be almost always true.
      // hence this while loop would almost always have O(1) complexity
      String host = allocated.getNodeId().getHost();
      String rack = NetworkTopology.getFirstHalf(RackResolver.resolve(host).getNetworkLocation());
      LinkedList<TaskAttemptId> list = mapsRackMapping.get(rack);
      while (list != null && list.size() > 0) {
        TaskAttemptId tId = list.removeFirst();
        if (maps.containsKey(tId)) {
          ContainerRequest assigned = maps.remove(tId);
          containerAssigned(allocated, assigned);
          it.remove();
          JobCounterUpdateEvent jce =
            new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
          jce.addCounterUpdate(JobCounter.RACK_LOCAL_MAPS, 1);
          this.rmContainerAllocator.eventHandler.handle(jce);
          this.rmContainerAllocator.rackLocalAssigned++;
          if (RMContainerAllocator.LOG.isDebugEnabled()) {
            RMContainerAllocator.LOG.debug("Assigned based on rack match " + rack);
          }
          break;
        }
      }
    }
    
    // assign remaining
    it = allocatedContainers.iterator();
    while(it.hasNext() && maps.size() > 0){
      Container allocated = it.next();
      Priority priority = allocated.getPriority();
      assert this.rmContainerAllocator.PRIORITY_MAP.equals(priority);
      TaskAttemptId tId = maps.keySet().iterator().next();
      ContainerRequest assigned = maps.remove(tId);
      containerAssigned(allocated, assigned);
      it.remove();
      JobCounterUpdateEvent jce =
        new JobCounterUpdateEvent(assigned.attemptID.getTaskId().getJobId());
      jce.addCounterUpdate(JobCounter.OTHER_LOCAL_MAPS, 1);
      this.rmContainerAllocator.eventHandler.handle(jce);
      if (RMContainerAllocator.LOG.isDebugEnabled()) {
        RMContainerAllocator.LOG.debug("Assigned based on * match");
      }
    }
  }
  
}

