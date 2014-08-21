/**
 *
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

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNodeWithNodeGroup;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestLeafQueueWithNodeGroup {
  private static final Log LOG = LogFactory.getLog(TestLeafQueue.class);

  private final RecordFactory recordFactory = 
      RecordFactoryProvider.getRecordFactory(null);

  RMContext rmContext;
  CapacityScheduler cs;
  CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;

  CSQueue root;
  Map<String, CSQueue> queues = new HashMap<String, CSQueue>();

  final static int GB = 1024;
  final static String DEFAULT_RACK = "/default";

  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  @Before
  public void setUp() throws Exception {
    CapacityScheduler spyCs = new CapacityScheduler();
    cs = spy(spyCs);
    rmContext = TestUtils.getMockRMContext();

    csConf = 
        new CapacitySchedulerConfiguration();
    csConf.setBoolean("yarn.scheduler.capacity.user-metrics.enable", true);
    csConf.setInt("yarn.scheduler.capacity.node-locality-delay", -1);
    final String newRoot = "root" + System.currentTimeMillis();
    setupQueueConfiguration(csConf, newRoot);
    YarnConfiguration conf = new YarnConfiguration();
    cs.setConf(conf);

    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).
        thenReturn(Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).
        thenReturn(Resources.createResource(16*GB, 32));
    when(csContext.getClusterResource()).
        thenReturn(Resources.createResource(100 * 16 * GB, 100 * 32));
    when(csContext.getApplicationComparator()).
    thenReturn(CapacityScheduler.applicationComparator);
    when(csContext.getQueueComparator()).
        thenReturn(CapacityScheduler.queueComparator);
    when(csContext.getResourceCalculator()).
        thenReturn(resourceCalculator);
    RMContainerTokenSecretManager containerTokenSecretManager =
        new RMContainerTokenSecretManager(conf);
    containerTokenSecretManager.rollMasterKey();
    when(csContext.getContainerTokenSecretManager()).thenReturn(
        containerTokenSecretManager);

    root = 
        CapacityScheduler.parseQueue(csContext, csConf, null,
            CapacitySchedulerConfiguration.ROOT, 
            queues, queues, 
            TestUtils.spyHook);

    cs.setRMContext(rmContext);
    cs.init(csConf);
    cs.start();
  }

  private static final String A = "a";
  private static final String B = "b";
  private void setupQueueConfiguration(
      CapacitySchedulerConfiguration conf,
      final String newRoot) {
    // Set related implementation classes with NodeGroup.
    conf.set(YarnConfiguration.NET_TOPOLOGY_WITH_NODEGROUP, "true");
    conf.set(YarnConfiguration.RM_SCHEDULED_REQUESTS_CLASS_KEY, 
        "org.apache.hadoop.mapreduce.v2.app.rm.ScheduledRequestsWithNodeGroup");
    conf.set(YarnConfiguration.RM_SCHEDULER_ELEMENTS_FACTORY_IMPL, 
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerElementsFactoryWithNodeGroup");

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {newRoot});
    conf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT, 100);
    conf.setAcl(CapacitySchedulerConfiguration.ROOT,
      QueueACL.SUBMIT_APPLICATIONS, " ");

    final String Q_newRoot = CapacitySchedulerConfiguration.ROOT + "." + newRoot;
    conf.setQueues(Q_newRoot, new String[] {A, B});
    conf.setCapacity(Q_newRoot, 100);
    conf.setMaximumCapacity(Q_newRoot, 100);
    conf.setAcl(Q_newRoot, QueueACL.SUBMIT_APPLICATIONS, " ");

    
    final String Q_A = Q_newRoot + "." + A;
    conf.setCapacity(Q_A, 10);
    conf.setMaximumCapacity(Q_A, 20);
    conf.setAcl(Q_A, QueueACL.SUBMIT_APPLICATIONS, "*");
    
    final String Q_B = Q_newRoot + "." + B;
    conf.setCapacity(Q_B, 90);
    conf.setMaximumCapacity(Q_B, 99);
    conf.setAcl(Q_A, QueueACL.SUBMIT_APPLICATIONS, "*");
    
    LOG.info("Setup top-level queues a and b");
  }

  static LeafQueue stubLeafQueue(LeafQueue queue) {
    
    // Mock some methods for ease in these unit tests
    
    // 1. LeafQueue.createContainer to return dummy containers
    doAnswer(
        new Answer<Container>() {
          @Override
          public Container answer(InvocationOnMock invocation) 
              throws Throwable {
            final FiCaSchedulerApp application = 
                (FiCaSchedulerApp)(invocation.getArguments()[0]);
            final ContainerId containerId = 
                TestUtils.getMockContainerId(application);

            Container container = TestUtils.getMockContainer(
                containerId,
                ((FiCaSchedulerNode)(invocation.getArguments()[1])).getNodeID(), 
                (Resource)(invocation.getArguments()[2]),
                ((Priority)invocation.getArguments()[3]));
            return container;
          }
        }
      ).
      when(queue).createContainer(
              any(FiCaSchedulerApp.class), 
              any(FiCaSchedulerNode.class), 
              any(Resource.class),
              any(Priority.class)
              );

    // 2. Stub out LeafQueue.parent.completedContainer
    CSQueue parent = queue.getParent();
    doNothing().when(parent).completedContainer(
        any(Resource.class), any(FiCaSchedulerApp.class),
        any(FiCaSchedulerNode.class), 
        any(RMContainer.class), any(ContainerStatus.class), 
        any(RMContainerEventType.class), any(CSQueue.class));
    
    return queue;
  }

  @Test
  public void testLocalitySchedulingWithNodeGroup() throws Exception {
    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue)queues.get(A));
    
    // User
    String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = 
        TestUtils.getMockApplicationAttemptId(0, 0); 
    FiCaSchedulerApp app_0 = 
        spy(new FiCaSchedulerApp(appAttemptId_0, user_0, a, 
                mock(ActiveUsersManager.class), rmContext));
    a.submitApplicationAttempt(app_0, user_0);

    // Setup some nodes, nodegroups and racks
    String host_0 = "host_0";
    String rack_0 = "rack_0";
    String nodegroup_0 = "nodegroup_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNodeWithNodeGroup(
        host_0, nodegroup_0, rack_0, 0, 8*GB);

    String host_1 = "host_1";
    String rack_1 = "rack_1";
    String nodegroup_1 = "nodegroup_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNodeWithNodeGroup(
        host_1, nodegroup_1, rack_1, 0, 8*GB);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8*GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests and submit
    Priority priority = TestUtils.createMockPriority(1);
    List<ResourceRequest> app_0_requests_0 = new ArrayList<ResourceRequest>();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_0, 1*GB, 1,
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(nodegroup_0, 1*GB, 1,
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_0, 1*GB, 1,
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1,
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(nodegroup_1, 1*GB, 1,
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1,
            true, priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 3, // one extra 
            true, priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);

    // Start testing...
    CSAssignment assignment = null;

    // NODE_LOCAL - node_0
    assignment = a.assignContainers(clusterResource, node_0);

    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_0), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(2, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType());

    // NODE_LOCAL - node_1
    assignment = a.assignContainers(clusterResource, node_1);
    verify(app_0).allocate(eq(NodeType.NODE_LOCAL), eq(node_1), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(1, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODE_LOCAL, assignment.getType());

    // Add 2 more request to check for NodeGroup_LOCAL and Rack_LOCAL
    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1, true,
            priority, recordFactory));
    app_0_requests_0.add(
            TestUtils.createResourceRequest(nodegroup_1, 1*GB, 1, true,
                priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, true,
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true,// one extra 
            priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);
    assertEquals(1, app_0.getTotalRequiredResources(priority));

    String host_2 = "host_2"; // on nodegroup_1
    FiCaSchedulerNode node_2 = TestUtils.getMockNodeWithNodeGroup(
        host_2, nodegroup_1, rack_1, 0, 8*GB);

    assignment = a.assignContainers(clusterResource, node_2);

    // Check NodeGroup-LOCAL scheduling
    verify(app_0).allocate(eq(NodeType.NODEGROUP_LOCAL), eq(node_2), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));

    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(0, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.NODEGROUP_LOCAL, assignment.getType());

    app_0_requests_0.clear();
    app_0_requests_0.add(
        TestUtils.createResourceRequest(host_1, 1*GB, 1, true,
            priority, recordFactory));
    app_0_requests_0.add(
            TestUtils.createResourceRequest(nodegroup_1, 1*GB, 1, true,
                priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(rack_1, 1*GB, 1, true,
            priority, recordFactory));
    app_0_requests_0.add(
        TestUtils.createResourceRequest(ResourceRequest.ANY, 1*GB, 1, true,// one extra 
            priority, recordFactory));
    app_0.updateResourceRequests(app_0_requests_0);
    assertEquals(1, app_0.getTotalRequiredResources(priority));

    String host_3 = "host_3"; // on nodegroup_1
    FiCaSchedulerNode node_3 = TestUtils.getMockNodeWithNodeGroup(
        host_3, nodegroup_0, rack_1, 0, 8*GB);

    assignment = a.assignContainers(clusterResource, node_3);
    // Check RACK-LOCAL scheduling
    verify(app_0).allocate(eq(NodeType.RACK_LOCAL), eq(node_3), 
        any(Priority.class), any(ResourceRequest.class), any(Container.class));
    assertEquals(0, app_0.getSchedulingOpportunities(priority)); // should reset
    assertEquals(0, app_0.getTotalRequiredResources(priority));
    assertEquals(NodeType.RACK_LOCAL, assignment.getType());
  }

  @After
  public void tearDown() throws Exception {
  }
}
