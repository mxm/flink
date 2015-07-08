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

package org.apache.flink.runtime.accumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * Main accumulator registry which encapsulates internal and user-defined accumulators.
 */
public class AccumulatorRegistry {

	protected static final Logger LOG = LoggerFactory.getLogger(AccumulatorRegistry.class);

	protected final JobID jobID;
	protected final ExecutionAttemptID taskID;

	private final Internal internalRegistry = new Internal();
	private final External externalRegistry = new External();

	public AccumulatorRegistry(JobID jobID, ExecutionAttemptID taskID) {
		this.jobID = jobID;
		this.taskID = taskID;
	}

	public Internal getInternal() {
		return internalRegistry;
	}

	public External getExternal() {
		return externalRegistry;
	}

	/**
	 * Abstract class which contains the core logic for accumulator registries.
	 */
	abstract class Registry {

		/* Accumulator values stored for the executing task. */
		Map<String, Accumulator<?, ?>> accumulators;

		public AccumulatorEvent getSnapshot() {
			try {
				return new AccumulatorEvent(jobID, taskID, accumulators);
			} catch (IOException e) {
				LOG.warn("Failed to serialize accumulators for task.", e);
				return null;
			}
		}

	}

	/**
	 * Registry for per-task user-defined accumulators.
	 */
	public class External extends Registry {

		/**
		 * Sets a user accumulator map (can only be called once).
		 * @param accumulatorMap the accumulator map
		 */
		public void setMap(Map<String, Accumulator<?, ?>> accumulatorMap) {
			if(accumulators == null) {
				accumulators = accumulatorMap;
			} else {
				throw new IllegalStateException("The map for has already been registered.");
			}
		}

	}

	/**
	 * Registry for keeping track of internal metrics (e.g. bytes and records in/out)
	 */
	public class Internal extends Registry {

		/**
		 * The built-in Flink accumulators
		 */
		public static final String NUM_RECORDS_IN  = "num_records_in";
		public static final String NUM_RECORDS_OUT = "num_records_out";
		public static final String NUM_BYTES_IN  = "num_bytes_in";
		public static final String NUM_BYTES_OUT = "num_bytes_out";


		/**
		 * Creates an accumulator map once.
		 */
		public void createMap() {
			if(accumulators == null) {
				accumulators = new HashMap<String, Accumulator<?, ?>>();
			} else {
				throw new IllegalStateException("The map for has already been registered.");
			}
		}

		/**
		 * Creates a long counter for a previously registered task accumulator map.
		 * @param name the name of the counter
		 * @return the counter
		 */
		public LongCounter createLongCounter(String name) {
			if(accumulators != null) {
				LongCounter longCounter = new LongCounter();
				accumulators.put(name, longCounter);
				return longCounter;
			} else {
				throw new IllegalStateException("The map for hasn't been registered yet.");
			}
		}


	}

}
