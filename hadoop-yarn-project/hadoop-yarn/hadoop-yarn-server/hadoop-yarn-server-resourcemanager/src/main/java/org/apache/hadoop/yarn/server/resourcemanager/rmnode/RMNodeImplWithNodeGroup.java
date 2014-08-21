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

package org.apache.hadoop.yarn.server.resourcemanager.rmnode;

import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;

public class RMNodeImplWithNodeGroup extends RMNodeImpl {

  public RMNodeImplWithNodeGroup(NodeId nodeId, RMContext context,
      String hostName, int cmPort, int httpPort, Node node,
          ResourceOption resourceOption, String nodeManagerVersion) {
    super(nodeId, context, hostName, cmPort, httpPort, node, resourceOption, nodeManagerVersion);
  }

  @Override
  public String getRackName() {
    return NetworkTopology.getFirstHalf(node.getNetworkLocation());
  }

  public String getNodeGroupName() {
    return NetworkTopology.getLastHalf(node.getNetworkLocation());
  }

}
