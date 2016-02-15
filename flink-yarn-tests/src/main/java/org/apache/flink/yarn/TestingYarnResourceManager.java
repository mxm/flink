/*
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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;


/**
 * A testing YARN resource manager which may alter the default yarn resource manager's behavior.
 */
public class TestingYarnResourceManager extends YarnResourceManager {

	public TestingYarnResourceManager(
		Configuration flinkConfig,
		LeaderRetrievalService leaderRetrievalService,
		YarnConfiguration yarnConfig,
		String applicationMasterHostName,
		String webInterfaceURL,
		ContaineredTaskManagerParameters taskManagerParameters,
		ContainerLaunchContext taskManagerLaunchContext,
		int yarnHeartbeatIntervalMillis,
		int maxFailedContainers,
		int numInitialTaskManagers) {

		super(flinkConfig,
			yarnConfig,
			leaderRetrievalService,
			applicationMasterHostName,
			webInterfaceURL,
			taskManagerParameters,
			taskManagerLaunchContext,
			yarnHeartbeatIntervalMillis,
			maxFailedContainers,
			numInitialTaskManagers);
	}

	/**
	 * Overwrite messages here if desired
	 */
	@Override
	protected void handleMessage(Object message) {
		super.handleMessage(message);
	}

	//
	// Testing messages go here
	//

}
