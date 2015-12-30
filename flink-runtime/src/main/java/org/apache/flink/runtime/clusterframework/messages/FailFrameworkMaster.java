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

import static java.util.Objects.requireNonNull;

/**
 * Message sent to the Flink's framework master so signal that something fatal has failed.
 * This message will cause the framework master to do some cleanup and fail. If the
 * framework supports and form of high availability, the master should be restarted in some
 * way.
 *
 * NOTE: This message is not serializable, because the exception may not be serializable.
 */
public class FailFrameworkMaster {
	
	private final String message;
	
	private final Throwable error;

	
	public FailFrameworkMaster(String message) {
		this(message, null);
	}
	
	public FailFrameworkMaster(String message, Throwable error) {
		this.message = requireNonNull(message);
		this.error = error;
	}

	
	public String message() {
		return message;
	}
	
	public Throwable error() {
		return error;
	}

	@Override
	public String toString() {
		return "FailFrameworkMaster { message: '" + message + "', error: '"
			+ error.getMessage() + "' }";
	}
}
