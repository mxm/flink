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

import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;

import java.io.Serializable;

import static java.util.Objects.requireNonNull;

/**
 * Message set to the JobManager by the Resource Manager to inform
 * about a failed TaskManager.
 */
public class TaskManagerFailed implements RequiresLeaderSessionID, Serializable {
	private static final long serialVersionUID = 1L;
	
	/** The ID under which the resource is registered (for example container ID) */
	private final String resourceId;
	
	/** The ID under which the TaskManager process registered itself */
	private final InstanceID registrationId;
	
	/** Optional message with details, for logging and debugging */ 
	private final String message;
	
	/**
	 * @param resourceId The ID under which the resource is registered (for example container ID).
	 * @param registrationId The TaskManager registration ID
	 */
	public TaskManagerFailed(String resourceId, InstanceID registrationId) {
		this(resourceId, registrationId, null);
	}

	/**
	 * @param resourceId The ID under which the resource is registered (for example container ID).
	 * @param registrationId The TaskManager registration ID
	 * @param message Optional message with details, for logging and debugging.
	 */
	public TaskManagerFailed(String resourceId, InstanceID registrationId, String message) {
		this.resourceId = requireNonNull(resourceId);
		this.registrationId = requireNonNull(registrationId);
		this.message = message;
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID under which the resource is registered (for example container ID).
	 * @return The resource ID
	 */
	public String resourceId() {
		return resourceId;
	}

	/**
	 * Gets the ID under which the TaskManager process registered itself.
	 * @return The TaskManager registration ID
	 */
	public InstanceID registrationId() {
		return registrationId;
	}

	/**
	 * Gets the optional message with details, for logging and debugging.
	 * @return Optional message, may be null
	 */
	public String message() {
		return message;
	}
	
	// ------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return "TaskManagerFailed{" +
			"resourceId='" + resourceId + '\'' +
			", registrationId=" + registrationId +
			", message='" + registrationId + "'}";
	}
}
