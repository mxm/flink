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

import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * Message sent by the JobManager to the cluster framework master to signal it to
 * begin its work.
 */
public class StartClusterMaster implements java.io.Serializable {

	private static final long serialVersionUID = 3570335964593710943L;
	
	private final ActorRef jobManager;
	
	private final UUID leaderSessionId;

	
	public StartClusterMaster(ActorRef jobManager, UUID leaderSessionId) {
		this.jobManager = requireNonNull(jobManager);
		this.leaderSessionId = requireNonNull(leaderSessionId);
	}


	public ActorRef jobManager() {
		return jobManager;
	}

	public UUID leaderSessionId() {
		return leaderSessionId;
	}

	@Override
	public String toString() {
		return "StartClusterMaster { jobManager='" + jobManager + 
			"',  leaderSessionId='" + leaderSessionId + "' }";
	}
}
