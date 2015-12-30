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

package org.apache.flink.runtime.clusterframework;

import akka.actor.ActorRef;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.clusterframework.messages.InfoMessage;
import org.apache.flink.runtime.clusterframework.messages.ShutdownClusterAfterJob;

import org.apache.flink.runtime.clusterframework.messages.StartClusterMaster;
import org.apache.flink.runtime.messages.Messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 *
 * 
 *  - Actor started
 *  - Framework master activated (when corresponding JobManager becomes leader)
 *  - Framework master deactivated (when corresponding JobManager looses leadership)
 *  
 *  - stop application
 *  - fail application
 */
public abstract class AbstractFrameworkMaster<WorkerType extends RegisteredWorkerNode>
extends FlinkUntypedActor {
	
	/** The name under which the actor is registered */
	public static final String ACTOR_NAME = "resource_master";
	
	
	/** The logger, named for the actual implementing class */
	protected final Logger log = LoggerFactory.getLogger(getClass());
	
	
	private final Map<String, WorkerType> currentWorkers;
	
	private final List<ActorRef> messageListeners;
	
	private final TaskManagerConfig configForTaskManagers;
	
	
	/** The JobManager that the framework master manages resources for */
	private ActorRef jobManager;
	
	/** Out JobManager's leader session */
	private UUID leaderSessionID;
	
	
	
	
	private int minNumTaskManagers;

	private int desiredNumTaskManagers;
	
	
	private JobID shutdownClusterAfterJob;
	
	private boolean active;
	
	
	protected AbstractFrameworkMaster(
				Configuration flinkConfig,
				int numInitialTaskManagers) {

		this.currentWorkers = new HashMap<>();
		this.messageListeners = new ArrayList<>(2);
	}

	public int getNumberOfRegisteredTaskManagers() {
		return currentWorkers.size();
	}

	public int getDesiredNumberOfTaskManagers() {
		return currentWorkers.size();
	}

	// ------------------------------------------------------------------------
	//  Actor Behavior
	// ------------------------------------------------------------------------

	/**
	 * 
	 * This method receives the actor messages after they have been filtered for
	 * a match with the 
	 * 
	 * @param message The incoming actor message.
	 */
	@Override
	protected final void handleMessage(Object message) {
		// some sanity checks
		if (message == null) {
			log.debug("Ignoring null message");
		}
		
		// we accept messages if we they are either a 'StartClusterMaster' message, or this
		// framework master is currently active
		if (message instanceof StartClusterMaster) {
			// handle starting of the framework. this happens when the JobManager is up and
			// is granted leadership
			if (active) {
				// already active. this is a problem.
				log.error("Received 'StartResourceMaster' message even though ");
				fatalError("JobManager tried to activate the resource manager a second time", null);
			}
			else {
				StartClusterMaster scm = (StartClusterMaster) message;
				this.jobManager = scm.jobManager();
				this.leaderSessionID = scm.leaderSessionId();
				this.active = true;
				
				try {
					frameworkMasterActivated();
				}
				catch (Throwable t) {
					fatalError("Could not start resource framework", t);
				}
			}
		}
		else if (!active) {
			log.debug("Received message even though framework master is not active: {}", message);
		}
		else {
			processAcceptedMessage(message);
		}
	}

	protected void processAcceptedMessage(Object message) {
		if (message instanceof ShutdownClusterAfterJob) {
			shutdownClusterAfterJob((ShutdownClusterAfterJob) message);
		}
	}
	
	
	@Override
	protected final UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	// ------------------------------------------------------------------------
	//  Actions
	// ------------------------------------------------------------------------

	protected void shutdownClusterAfterJob(ShutdownClusterAfterJob message) {
		JobID jobId = message.jobId();

		log.info("Cluster will shut down after job {} has finished.", jobId);
		this.shutdownClusterAfterJob = jobId;

		// acknowledge
		sender().tell(Messages.Acknowledge.get(), getSelf());
	}

	// ------------------------------------------------------------------------
	//  Callbacks
	// ------------------------------------------------------------------------
	
	protected void sendInfoMessage(String message) {
		if (messageListeners.size() > 1) {
			Object msg = decorateMessage(new InfoMessage(message));
			for (ActorRef actor : messageListeners) {
				actor.tell(msg, self());
			}
		}
	}
	
	protected void newWorkerNodeAvailable(WorkerType node) {
		
	}

	protected void fatalError(String message, Throwable error) {
		
	}
	
	protected void registerMessageListener(ActorRef listener) {
		if (listener != null) {
			messageListeners.add(listener);
		}
	}

	protected void removeMessageListener(ActorRef listener) {
		messageListeners.remove(listener);
	}
	
	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------

	protected abstract void frameworkMasterActivated() throws Exception;
	
	protected abstract void requestNewWorkers(int numWorkers);

	
}
