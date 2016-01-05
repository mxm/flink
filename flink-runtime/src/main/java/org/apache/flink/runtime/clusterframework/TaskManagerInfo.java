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

import org.apache.flink.runtime.instance.InstanceID;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Information about a TaskManager that is currently registered.
 */
public class TaskManagerInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String resourceId;

	private final InstanceID registeredTaskManagerId;
	
	private final ActorRef taskManagerActor;
	
	private final int numSlots;

	public TaskManagerInfo(
					String resourceId, InstanceID registeredTaskManagerId, 
					ActorRef taskManagerActor, int numSlots) {
		this.resourceId = requireNonNull(resourceId);
		this.registeredTaskManagerId = requireNonNull(registeredTaskManagerId);
		this.taskManagerActor = requireNonNull(taskManagerActor);
		this.numSlots = numSlots;
	}
	
	// ------------------------------------------------------------------------

	public String resourceId() {
		return resourceId;
	}

	public InstanceID registeredTaskManagerId() {
		return registeredTaskManagerId;
	}

	public ActorRef taskManagerActor() {
		return taskManagerActor;
	}
	
	public int numSlots() {
		return numSlots;
	}
	
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "RegisteredWorkerNode {" +
			"resourceId='" + resourceId + '\'' +
			", registeredTaskManagerId=" + registeredTaskManagerId +
			", taskManagerActor=" + taskManagerActor.path() +
			", numSlots=" + numSlots +
			'}';
	}
}
