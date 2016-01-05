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

package org.apache.flink.runtime.clusterframework.messages;

import akka.actor.ActorRef;

import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Message set to the JobManager by the Resource Manager to inform
 * about a newly available TaskManager.
 */
public class TaskManagerAvailable implements RequiresLeaderSessionID, Serializable {
	private static final long serialVersionUID = 1L;
	
	private final String resourceId;
	
	private final InstanceID registrationId;
	
	private final ActorRef taskManagerActor;
	
	private final InstanceConnectionInfo connectionInfo;
	
	private final HardwareDescription taskManagerResources;
	
	private final int numSlots;

	public TaskManagerAvailable(
					String resourceId, InstanceID registrationId,
					ActorRef taskManagerActor,
					InstanceConnectionInfo connectionInfo, HardwareDescription taskManagerResources,
					int numSlots)
	{
		this.resourceId = requireNonNull(resourceId);
		this.registrationId = requireNonNull(registrationId);
		this.taskManagerActor = requireNonNull(taskManagerActor);
		this.connectionInfo = requireNonNull(connectionInfo);
		this.taskManagerResources = requireNonNull(taskManagerResources);
		this.numSlots = numSlots;
	}
	
	// ------------------------------------------------------------------------

	public String resourceId() {
		return resourceId;
	}

	public InstanceID registrationId() {
		return registrationId;
	}

	public ActorRef taskManagerActor() {
		return taskManagerActor;
	}

	public InstanceConnectionInfo connectionInfo() {
		return connectionInfo;
	}

	public HardwareDescription taskManagerResources() {
		return taskManagerResources;
	}

	public int numSlots() {
		return numSlots;
	}

	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "TaskManagerAvailable{" +
			"resourceId='" + resourceId + '\'' +
			", registrationId=" + registrationId +
			", taskManagerActor=" + taskManagerActor +
			", connectionInfo=" + connectionInfo +
			", taskManagerResources=" + taskManagerResources +
			", numSlots=" + numSlots +
			'}';
	}
}
