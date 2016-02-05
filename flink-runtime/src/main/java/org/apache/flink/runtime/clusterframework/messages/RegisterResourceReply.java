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
import org.apache.flink.runtime.messages.RegistrationMessages;
import org.apache.flink.runtime.messages.RequiresLeaderSessionID;

/**
 * Answer to RegisterResourceReply to indicate whether the requested resource is registered.
 */
public class RegisterResourceReply implements RequiresLeaderSessionID, java.io.Serializable {
	private static final long serialVersionUID = 1L;

	private final boolean isRegistered;
	private final ActorRef taskManager;
	private final RegistrationMessages.RegisterTaskManager registrationMessage;
	/** Optional (error) message */
	private final String message;

	public RegisterResourceReply(boolean isRegistered, ActorRef taskManager,
								 RegistrationMessages.RegisterTaskManager registrationMessage) {
		this(isRegistered, taskManager, registrationMessage, "");
	}

	public RegisterResourceReply(boolean isRegistered, ActorRef taskManager,
								 RegistrationMessages.RegisterTaskManager registrationMessage, String message) {
		this.isRegistered = isRegistered;
		this.taskManager = taskManager;
		this.registrationMessage = registrationMessage;
		this.message = message;
	}

	public boolean isRegistered() {
		return isRegistered;
	}

	public String getMessage() {
		return message;
	}

	public ActorRef getTaskManager() {
		return taskManager;
	}

	public RegistrationMessages.RegisterTaskManager getRegistrationMessage() {
		return registrationMessage;
	}

	@Override
	public String toString() {
		return "RegisterResourceReply{" +
			"isRegistered=" + isRegistered +
			", taskManager=" + taskManager +
			", registrationMessage=" + registrationMessage +
			", message='" + message + '\'' +
			'}';
	}
}
