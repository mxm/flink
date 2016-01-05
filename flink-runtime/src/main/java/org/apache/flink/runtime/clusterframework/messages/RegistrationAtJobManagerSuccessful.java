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

import org.apache.flink.runtime.clusterframework.TaskManagerInfo;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Message that informs the resource manager that the JobManager accepted its registration.
 * Carries information about the JobManager, and about the TaskManagers that the JobManager
 * still has registered.
 */
public class RegistrationAtJobManagerSuccessful implements RequiresLeaderSessionID, Serializable {
	
	private static final long serialVersionUID = 817011779310941753L;
	
	/** The JobManager actor reference */ 
	private final ActorRef jobManager;
	
	/** The port of the JobManager's BLOB service */
	private final int blobServerPort;
	
	/** The list of registered TaskManagers that the JobManager currently knows */
	private final List<TaskManagerInfo> currentlyRegisteredTaskManagers;


	/**
	 * Creates a new message with an empty list of known TaskManagers.
	 * @param jobManager
	 *         The JobManager actor reference
	 * @param blobServerPort
	 *         The port of the JobManager's BLOB service
	 */
	public RegistrationAtJobManagerSuccessful(ActorRef jobManager, int blobServerPort) {
		this(jobManager, blobServerPort, Collections.<TaskManagerInfo>emptyList());
	}

	/**
	 * Creates a new message with a list of existing known TaskManagers.
	 * 
	 * @param jobManager
	 *         The JobManager actor reference.
	 * @param blobServerPort
	 *         The port of the JobManager's BLOB service.
	 * @param currentlyRegisteredTaskManagers
	 *         The list of TaskManagers that the JobManager currently knows. 
	 */
	public RegistrationAtJobManagerSuccessful(
					ActorRef jobManager, int blobServerPort,
					List<TaskManagerInfo> currentlyRegisteredTaskManagers)
	{
		this.jobManager = requireNonNull(jobManager);
		this.blobServerPort = blobServerPort;
		this.currentlyRegisteredTaskManagers = requireNonNull(currentlyRegisteredTaskManagers);
	}
	
	// ------------------------------------------------------------------------

	public ActorRef jobManager() {
		return jobManager;
	}

	public int blobServerPort() {
		return blobServerPort;
	}
	
	public List<TaskManagerInfo> currentlyRegisteredTaskManagers() {
		return currentlyRegisteredTaskManagers;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "RegistrationAtJobManagerSuccessful {"
			+ " jobManager=" + jobManager
			+ " , blobServerPort=" + blobServerPort
			+ " }";
	}
}
