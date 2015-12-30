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

import org.apache.flink.runtime.clusterframework.RegisteredTaskManager;
import org.apache.flink.runtime.instance.InstanceID;

/**
 * A standalone TaskManager (i.e., a TaskManager that was not started through some resource
 * management framework), registered as an available resource.
 */
public class RegisteredStandaloneTaskManager extends RegisteredTaskManager {
	
	/** The timestamp of the last received heartbeat */
	private long lastHeartbeatTime;


	public RegisteredStandaloneTaskManager(
			String resourceId,
			InstanceID registeredTaskManagerId,
			ActorRef taskManagerActor,
			int numSlots)
	{
		super(resourceId, registeredTaskManagerId, taskManagerActor, numSlots);
		recordHeartbeat();
	}

	/**
	 * Records that this TaskManager has sent a heartbeat.
	 */
	public void recordHeartbeat() {
		this.lastHeartbeatTime = System.currentTimeMillis();
	}

	/**
	 * Checks whether the last heartbeat was so long ago that the TaskManager should be
	 * considered failed.
	 * 
	 * @return True, if the TaskManager should be considered failed, false otherwise.
	 */
	public boolean isConsideredFailed(long now, long maxHeartbeatPause) {
		long pause = now - lastHeartbeatTime;
		return pause > maxHeartbeatPause;
	}
}
