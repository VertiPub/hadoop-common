package org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica;

import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeImplWithNodeGroup;

public class FiCaSchedulerNodeWithNodeGroup extends FiCaSchedulerNode {

  public FiCaSchedulerNodeWithNodeGroup(RMNode node, boolean usePortForNodeName) {
    super(node, usePortForNodeName);
  }

  @Override
  public boolean isNodeGroupAware() {
    return true;
  }

  @Override
  public String getNodeGroupName() {
    if (!(rmNode instanceof RMNodeImplWithNodeGroup)) {
      return null;
    }
    RMNodeImplWithNodeGroup rmNodeWithNodeGroup = 
        (RMNodeImplWithNodeGroup) rmNode;
    return rmNodeWithNodeGroup.getNodeGroupName();
  }

}
