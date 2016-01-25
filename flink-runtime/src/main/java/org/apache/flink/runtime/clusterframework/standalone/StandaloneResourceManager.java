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
public class StandaloneResourceManager extends FlinkResourceManager<RegisteredStandaloneTaskManager> {
	
	/** The maximum pause between two heartbeats after which a TaskManager is considered failed */ 
	private final long maxHeartbeatPause;

	/** The interval in which the actor checks for failed TaskManagers */
	private final long cleanupInterval;
	
	/** The scheduler that triggers periodic cleanup of dead TaskManagers */
	private Cancellable periodicCleanupScheduler;

	public StandaloneResourceManager(Configuration flinkConfig, LeaderRetrievalService leaderRetriever) {
		super(flinkConfig, leaderRetriever);

		final long heartbeatInterval = config.getLong(
			ConfigConstants.STANDALONE_HEARTBEAT_MAX_PAUSE_KEY,
			ConfigConstants.DEFAULT_STANDALONE_HEARTBEAT_INTERVAL);

		this.maxHeartbeatPause = config.getLong(
			ConfigConstants.STANDALONE_HEARTBEAT_MAX_PAUSE_KEY,
			ConfigConstants.DEFAULT_STANDALONE_HEARTBEAT_MAX_PAUSE);
		
		this.cleanupInterval = config.getLong(
			ConfigConstants.STANDALONE_CLEANUP_INTERVAL_KEY,
			heartbeatInterval);
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------
	
	@Override
	protected void initialize() throws Exception {
		// the only thing we do is start the periodic check for dead TaskManagers
		// to maintain actor thread safety, we start a periodic scheduler that
		// sends a message that triggers the cleanup
		
		// cancel and pre-existing scheduler
		if (periodicCleanupScheduler != null) {
			periodicCleanupScheduler.cancel();
		}
		
		final ActorRef self = self();
		final FiniteDuration interval = new FiniteDuration(cleanupInterval, TimeUnit.MILLISECONDS);

		periodicCleanupScheduler = context().system().scheduler().schedule(
			interval, interval,
			new Runnable() {
				@Override
				public void run() {
					self.tell(decorateMessage(PerformCleanupMessage.get()), ActorRef.noSender());
				}
			}, context().dispatcher());
	}

	@Override
	protected void shutdownApplication(ApplicationStatus finalStatus, String optionalDiagnostics) {
		// cancel the periodic cleanup
		if (periodicCleanupScheduler != null) {
			periodicCleanupScheduler.cancel();
			periodicCleanupScheduler = null;
		}
	}

	@Override
	protected void fatalError(String message, Throwable error) {
		log.error("FATAL ERROR IN RESOURCE MANAGER: " + message, error);
		LOG.error("Shutting down process");
		
		// kill this process
		System.exit(EXIT_CODE_FATAL_ERROR);
	}

	@Override
	protected List<RegisteredStandaloneTaskManager> reacceptRegisteredTaskManagers(
		Collection<TaskManagerInfo> toConsolidate)
	{
		List<RegisteredStandaloneTaskManager> accepted = new ArrayList<>(toConsolidate.size());
		for (TaskManagerInfo tm : toConsolidate) {
			accepted.add(new RegisteredStandaloneTaskManager(
				tm.resourceId(), tm.registeredTaskManagerId(),
				tm.taskManagerActor(), tm.numSlots()));
		}
		return accepted;
	}

	@Override
	protected void requestNewWorkers(int numWorkers) {
		// cannot request new workers
	}

	@Override
	protected RegisteredStandaloneTaskManager workerRegistered(RegisterTaskManager registerMessage) {
		final InstanceID registerId = new InstanceID();
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
		if (message instanceof HeartbeatMessage) {
			receiveHeartbeat((HeartbeatMessage) message, sender());
		}
		else if (message instanceof PerformCleanupMessage) {
			cleanupDeadWorkers();
		}
		else {
			super.handleMessage(message);
		}
	}
	
	private void receiveHeartbeat(HeartbeatMessage heartbeat, ActorRef sender) {
		RegisteredStandaloneTaskManager worker = getRegisteredTaskManager(heartbeat.workerId());
		if (worker != null) {
			if (worker.registeredTaskManagerId().equals(heartbeat.registrationId())) {
				worker.recordHeartbeat();
				sender.tell(HeartbeatAcknowledgement.get(), self());
			}
			else {
				log.debug("Received heartbeat for expired registration: {}", heartbeat);
			}
		}
		else {
			log.error("Received heartbeat for unknown worker: " + heartbeat);
		}
	}
	
	private void cleanupDeadWorkers() {
		final long now = System.currentTimeMillis();

		// we need to collect dead TaskManagers in a separate collection first,
		// to prevent ConcurrentModificationException in the map of registered workers 
		List<RegisteredStandaloneTaskManager> failedWorkers = new ArrayList<>(2);
		
		// check all currently registered TaskManagers
		for (RegisteredStandaloneTaskManager tm : allRegisteredTaskManagers()) {
			if (tm.isConsideredFailed(now, maxHeartbeatPause)) {
				failedWorkers.add(tm);
			}
		}
		
		if (!failedWorkers.isEmpty()) {
			// actually notify of the loss of the workers
			String msg = "Received no heartbeat for " + maxHeartbeatPause + " msecs";
			for (RegisteredStandaloneTaskManager tm : failedWorkers) {
				notifyWorkerFailed(tm, msg);
			}
		}
	}

}
