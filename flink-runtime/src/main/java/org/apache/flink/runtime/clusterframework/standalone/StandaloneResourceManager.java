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

package org.apache.flink.runtime.clusterframework.standalone;

import akka.actor.ActorRef;
import akka.actor.Cancellable;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.TaskManagerInfo;
import org.apache.flink.runtime.clusterframework.messages.LookupResourceReply;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.RegistrationMessages.RegisterTaskManager;

import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A standalone implementation of the resource manager. Used when the system is started in
 * standalone mode (via scripts), rather than via a resource framework like YARN or Mesos.
 */
public class StandaloneResourceManager extends FlinkResourceManager {
	

	public StandaloneResourceManager(Configuration flinkConfig, LeaderRetrievalService leaderRetriever) {
		super(flinkConfig, leaderRetriever);
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------
	
	@Override
	protected void initialize() throws Exception {
	}

	@Override
	protected void shutdownApplication(ApplicationStatus finalStatus, String optionalDiagnostics) {
	}

	@Override
	protected void fatalError(String message, Throwable error) {
		log.error("FATAL ERROR IN RESOURCE MANAGER: " + message, error);
		LOG.error("Shutting down process");
		
		// kill this process
		System.exit(EXIT_CODE_FATAL_ERROR);
	}

	@Override
	protected List<ResourceID> reacceptRegisteredTaskManagers(List<ResourceID> toConsolidate)
	{
		return toConsolidate;
	}

	@Override
	protected void requestNewWorkers(int numWorkers) {
		// cannot request new workers
	}

	@Override
	protected ResourceID workerRegistered(RegisterTaskManager registerMessage) {
		return new RegisteredStandaloneTaskManager(registerMessage.resourceId(), registerId,
			registerMessage.taskManagerActor(), registerMessage.numberOfSlots());
	}

	@Override
	protected void workerUnRegistered(RegisteredStandaloneTaskManager worker) {
		// nothing to do, we only wait for the worker to re-register
	}

	@Override
	protected void releaseRegisteredWorker(RegisteredStandaloneTaskManager registeredWorker) {
		// cannot release any workers, they simply stay
	}

	@Override
	protected void releasePendingWorker(ResourceID resourceID) {
		// no pending workers
	}

	@Override
	protected int getNumWorkerRequestsPending() {
		// this one never has pending requests and containers in launch
		return 0;
	}

	@Override
	protected int getNumWorkersPendingRegistration() {
		// this one never has pending requests and containers in launch
		return 0;
	}

	// ------------------------------------------------------------------------
	//  Actor messages
	// ------------------------------------------------------------------------

	@Override
	protected void handleMessage(Object message) {
		super.handleMessage(message);
	}

	@Override
	protected void handleResourceLookup(ActorRef sender, ResourceID resourceID) {
		// in standalone mode, always confirm availability of resource
		sender().tell(decorateMessage(
			new LookupResourceReply(true)
		), self());
	}

}
