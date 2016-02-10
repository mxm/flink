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

package org.apache.flink.runtime.resourcemanager;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.messages.RegisterResource;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceFailed;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceSuccessful;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.RegistrationMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testutils.TestingResourceManager;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import scala.Option;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ResourceManagerTest {

	private static ActorSystem system;

	private static ActorGateway fakeJobManager;
	private static ActorGateway resourceManager;

	private static Configuration config = new Configuration();

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(config);
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	/**
	 * Tests the registration and reconciliation of the ResourceManager with the JobManager
	 */
	@Test
	public void testJobManagerRegistration() {
		new JavaTestKit(system){{
		new Within(duration("10 seconds")) {
		@Override
		protected void run() {
			fakeJobManager = TestingUtils.createForwardingJobManager(system, getTestActor(), Option.<String>empty());
			resourceManager = TestingUtils.createResourceManager(system, fakeJobManager.actor(), config);

			expectMsgClass(RegisterResourceManager.class);

			List<ResourceID> resourceList = new ArrayList<>();
			resourceList.add(ResourceID.generate());
			resourceList.add(ResourceID.generate());
			resourceList.add(ResourceID.generate());

			resourceManager.tell(new RegisterResourceManagerSuccessful(resourceList), fakeJobManager);

			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			TestingResourceManager.GetRegisteredResourcesReply reply =
				expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			for (ResourceID id : resourceList) {
				if (!reply.resources.contains(id)) {
					Assert.fail("Expected to find all resources that were provided during registration.");
				}
			}
		}};
		}};
	}

	/**
	 * Tests delayed or erroneous registration of the ResourceManager with the JobManager
	 */
	@Test
	public void testDelayedJobManagerRegistration() {
		new JavaTestKit(system){{
		new Within(duration("10 seconds")) {
		@Override
		protected void run() {

			// set a short timeout for lookups
			Configuration shortTimeoutConfig = config.clone();
			shortTimeoutConfig.setString(ConfigConstants.AKKA_LOOKUP_TIMEOUT, "1 s");

			fakeJobManager = TestingUtils.createForwardingJobManager(system, getTestActor(), Option.<String>empty());
			resourceManager = TestingUtils.createResourceManager(system, fakeJobManager.actor(), shortTimeoutConfig);

			// wait for registration message
			RegisterResourceManager msg = expectMsgClass(RegisterResourceManager.class);
			// give wrong response
			getLastSender().tell(new JobManagerMessages.LeaderSessionMessage(null, new Object()),
				fakeJobManager.actor());

			// expect another retry and let it time out
			expectMsgClass(RegisterResourceManager.class);

			// wait for next try after timeout
			expectMsgClass(RegisterResourceManager.class);

		}};
		}};
	}

	/**
	 * Tests the registration and accounting of resources at the ResourceManager.
	 */
	@Test
	public void testTaskManagerRegistration() {
		new JavaTestKit(system){{
		new Within(duration("10 seconds")) {
		@Override
		protected void run() {

			fakeJobManager = TestingUtils.createForwardingJobManager(system, getTestActor(), Option.<String>empty());
			resourceManager = TestingUtils.createResourceManager(system, fakeJobManager.actor(), config);

			// register with JM
			expectMsgClass(RegisterResourceManager.class);
			resourceManager.tell(new RegisterResourceManagerSuccessful(Collections.<ResourceID>emptyList()), fakeJobManager);

			ResourceID resourceID = ResourceID.generate();

			// Send task manager registration
			resourceManager.tell(new RegisterResource(
				ActorRef.noSender(),
				new RegistrationMessages.RegisterTaskManager(resourceID,
					Mockito.mock(InstanceConnectionInfo.class),
					null,
					1)),
				fakeJobManager);

			expectMsgClass(RegisterResourceSuccessful.class);

			// check for number registration of registered resources
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			TestingResourceManager.GetRegisteredResourcesReply reply =
				expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			Assert.assertEquals(1, reply.resources.size());

			// Send task manager registration again
			resourceManager.tell(new RegisterResource(
					ActorRef.noSender(),
					new RegistrationMessages.RegisterTaskManager(resourceID,
						Mockito.mock(InstanceConnectionInfo.class),
						null,
						1)),
				fakeJobManager);

			expectMsgClass(RegisterResourceSuccessful.class);

			// check for number registration of registered resources
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			reply = expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			Assert.assertEquals(1, reply.resources.size());


			// Send invalid null resource id to throw an exception during resource registration
			resourceManager.tell(new RegisterResource(
					ActorRef.noSender(),
					new RegistrationMessages.RegisterTaskManager(null,
						Mockito.mock(InstanceConnectionInfo.class),
						null,
						1)),
				fakeJobManager);

			expectMsgClass(RegisterResourceFailed.class);

			// check for number registration of registered resources
			resourceManager.tell(new TestingResourceManager.GetRegisteredResources(), fakeJobManager);
			reply = expectMsgClass(TestingResourceManager.GetRegisteredResourcesReply.class);

			Assert.assertEquals(1, reply.resources.size());
		}};
		}};
	}
}
