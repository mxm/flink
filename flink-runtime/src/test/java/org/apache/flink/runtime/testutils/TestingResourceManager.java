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

package org.apache.flink.runtime.testutils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.standalone.StandaloneResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.util.Collection;

/**
 * A testing resource manager which may alter the default standalone resource master's behavior.
 */
public class TestingResourceManager extends StandaloneResourceManager {

	public TestingResourceManager(Configuration flinkConfig, LeaderRetrievalService leaderRetriever) {
		super(flinkConfig, leaderRetriever);
	}

	/**
	 * Overwrite messages here if desired
	 */
	@Override
	protected void handleMessage(Object message) {

		if (message instanceof GetRegisteredResources) {
			sender().tell(new GetRegisteredResourcesReply(getRegisteredTaskManagers()), self());
		} else if (message instanceof FailResource) {
			ResourceID resourceID = ((FailResource) message).resourceID;
			notifyWorkerFailed(resourceID, "Failed for test case.");
		} else {
			super.handleMessage(message);
		}
	}

	/**
	 * Testing messages
	 */
	public static class GetRegisteredResources {}

	public static class GetRegisteredResourcesReply {

		public Collection<ResourceID> resources;

		public GetRegisteredResourcesReply(Collection<ResourceID> resources) {
			this.resources = resources;
		}

	}

	/**
	 * Fails all resources that the resource manager has registered
	 */
	public static class FailResource {

		public ResourceID resourceID;

		public FailResource(ResourceID resourceID) {
			this.resourceID = resourceID;
		}
	}
}
