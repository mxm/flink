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

package org.apache.flink.test.accumulators;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Status;
import akka.testkit.JavaTestKit;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.taskmanager.TaskManager;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.*;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


/**
 * Test the availability of accumulator results during runtime.
 */
@SuppressWarnings("serial")
public class LiveAccumulatorITCase {

	private static ActorSystem system;
	private static ActorRef jobManager;

	// name of accumulator
	private static String NAME = "test";
	// time to wait between changing the accumulator value
	private static long WAIT_TIME = TaskManager.HEARTBEAT_INTERVAL().toMillis() + 500;

	// number of heartbeat intervals to check
	private static int NUM_ITERATIONS = 3;
	// numer of retries in case the expected value is not seen
	private static int NUM_RETRIES = 3;

	private static List<String> inputData = new ArrayList<String>(NUM_ITERATIONS);;


	@Before
	public void before() throws Exception {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
		TestingCluster testingCluster = TestingUtils.startTestingCluster(1, 1, TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT());
		jobManager = testingCluster.getJobManager();

		// generate test data
		for (int i=0; i < NUM_ITERATIONS; i++) {
			inputData.add(i, String.valueOf(i+1));
		}
	}

	@After
	public void after() throws Exception {
		JavaTestKit.shutdownActorSystem(system);
	}

	@Test
	public void testProgram() throws Exception {

		new JavaTestKit(system) {{

			/** The program **/
			ExecutionEnvironment env = new PlanExtractor();
			DataSet<String> input = env.fromCollection(inputData);
			input.flatMap(new WaitingUDF()).output(new DiscardingOutputFormat<Integer>());
			env.execute();

			/** Extract job graph **/
			JobGraph jobGraph = getOptimizedPlan(((PlanExtractor) env).plan);

			jobManager.tell(new JobManagerMessages.SubmitJob(jobGraph, false), getRef());
			expectMsgClass(Status.Success.class);

			/* Check for accumulator values */
			int i = 0, retries = 0;
			int expected = 0;
			while(i < NUM_ITERATIONS) {
				// wait for heartbeat interval
				Thread.sleep(WAIT_TIME);

				jobManager.tell(new RequestAccumulatorValues(jobGraph.getJobID()), getRef());
				Map<String, Accumulator<?, ?>> accumulators =
						expectMsgClass(RequestAccumulatorValuesResponse.class).accumulators();

				if (accumulators.containsKey(NAME) && expected != ((IntCounter)accumulators.get(NAME)).getLocalValue()) {
					if (retries < NUM_RETRIES) {
						// try again
						retries += 1;
						//System.out.println("retrying for the " + retries + " time.");
						continue;
					} else {
						fail("Failed in round #" + i + " after " + retries + " retries.");
					}
				} else {
					// passed
					//System.out.println("passed round #" + i);
					retries = 0;
				}
				i += 1;
				expected += i;
			}

			expectMsgClass(JobManagerMessages.JobResultSuccess.class);
		}};
	}


	/**
	 * UDF that waits for at least the heartbeat interval's duration.
	 */
	private static class WaitingUDF extends RichFlatMapFunction<String, Integer> {

		private IntCounter counter = new IntCounter();

		@Override
		public void open(Configuration parameters) throws Exception {
			getRuntimeContext().addAccumulator(NAME, counter);
		}

		@Override
		public void flatMap(String value, Collector<Integer> out) throws Exception {
			/* Wait here to check the accumulator value in the meantime */
			Thread.sleep(WAIT_TIME);
			int val = Integer.valueOf(value);
			counter.add(val);
			out.collect(val);
		}
	}

	/**
	 * Helpers to generate the JobGraph
	 */
	private static JobGraph getOptimizedPlan(Plan plan) {
		Optimizer pc = new Optimizer(new DataStatistics(), new Configuration());
		JobGraphGenerator jgg = new JobGraphGenerator();
		OptimizedPlan op = pc.compile(plan);
		return jgg.compileJobGraph(op);
	}

	private static class PlanExtractor extends LocalEnvironment {

		private Plan plan = null;

		@Override
		public JobExecutionResult execute(String jobName) throws Exception {
			plan = createProgramPlan();
			return new JobExecutionResult(new JobID(), -1, null);
		}

	}
}
