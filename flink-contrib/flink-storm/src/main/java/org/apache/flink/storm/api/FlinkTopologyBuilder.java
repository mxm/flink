/*
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
package org.apache.flink.storm.api;

import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.IRichStateSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.storm.util.SplitStreamMapper;
import org.apache.flink.storm.util.SplitStreamType;
import org.apache.flink.storm.util.StormConfig;
import org.apache.flink.storm.util.StormStreamSelector;
import org.apache.flink.storm.wrappers.BoltWrapper;
import org.apache.flink.storm.wrappers.SpoutWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * {@link FlinkTopologyBuilder} translates a {@link TopologyBuilder} to a Flink program.
 * <strong>CAUTION: {@link IRichStateSpout StateSpout}s are currently not supported.</strong>
 */
public class FlinkTopologyBuilder {

	/** All declared streams and output schemas by operator ID */
	private final HashMap<String, HashMap<String, Fields>> outputStreams = new HashMap<String, HashMap<String, Fields>>();
	/** All spouts&bolts declarers by their ID */
	private final HashMap<String, FlinkOutputFieldsDeclarer> declarers = new HashMap<String, FlinkOutputFieldsDeclarer>();

	private final TopologyBuilder builder;

	// needs to be a class member for internal testing purpose
	private final StormTopology stormTopology;

	private final Map<String, IRichSpout> spouts;
	private final Map<String, IRichBolt> bolts;

	public FlinkTopologyBuilder(TopologyBuilder builder) {
		this.builder = builder;
		this.stormTopology = builder.createTopology();
		// extract the spouts and bolts
		this.spouts = getPrivateField("_spouts");
		this.bolts = getPrivateField("_bolts");
	}

	/**
	 * Creates a Flink program that uses the specified spouts and bolts.
	 */
	public StreamExecutionEnvironment translateTopology() {
		return translateTopology(Collections.emptyMap());
	}

	@SuppressWarnings("unchecked")
	private <T> Map<String, T> getPrivateField(String field) {
		try {
			Field f = builder.getClass().getDeclaredField(field);
			f.setAccessible(true);
			return copyObject((Map<String, T>) f.get(builder));
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException("Couldn't get " + field + " from TopologyBuilder", e);
		}
	}

	private <T> T copyObject(T object) {
		try {
			return InstantiationUtil.deserializeObject(
					InstantiationUtil.serializeObject(object),
					getClass().getClassLoader()
			);
		} catch (IOException | ClassNotFoundException e) {
			throw new RuntimeException("Failed to copy object.");
		}
	}

	/**
	 * Creates a Flink program that uses the specified spouts and bolts.
	 * @param configMap The Storm config to make available during runtime
	 */
	public StreamExecutionEnvironment translateTopology(Map configMap) {

		// Creates local or remote environment implictly - the Flink style
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// Storm defaults to parallelism 1
		env.setParallelism(1);
		// Pass Storm config
		env.getConfig().setGlobalJobParameters(new StormConfig(configMap));

		/* Translation of topology */

		final HashMap<String, HashMap<String, DataStream<Tuple>>> availableInputs = new HashMap<String, HashMap<String, DataStream<Tuple>>>();

		for (final Entry<String, IRichSpout> spout : spouts.entrySet()) {
			final String spoutId = spout.getKey();
			final IRichSpout userSpout = spout.getValue();

			final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
			userSpout.declareOutputFields(declarer);
			final HashMap<String,Fields> sourceStreams = declarer.outputStreams;
			this.outputStreams.put(spoutId, sourceStreams);
			declarers.put(spoutId, declarer);


			final HashMap<String, DataStream<Tuple>> outputStreams = new HashMap<String, DataStream<Tuple>>();
			final DataStreamSource<?> source;

			if (sourceStreams.size() == 1) {
				final SpoutWrapper<Tuple> spoutWrapperSingleOutput = new SpoutWrapper<Tuple>(userSpout);
				spoutWrapperSingleOutput.setStormTopology(stormTopology);

				final String outputStreamId = (String) sourceStreams.keySet().toArray()[0];

				DataStreamSource<Tuple> src = env.addSource(spoutWrapperSingleOutput, spoutId,
						declarer.getOutputType(outputStreamId));

				outputStreams.put(outputStreamId, src);
				source = src;
			} else {
				final SpoutWrapper<SplitStreamType<Tuple>> spoutWrapperMultipleOutputs = new SpoutWrapper<SplitStreamType<Tuple>>(
						userSpout);
				spoutWrapperMultipleOutputs.setStormTopology(stormTopology);

				@SuppressWarnings({ "unchecked", "rawtypes" })
				DataStreamSource<SplitStreamType<Tuple>> multiSource = env.addSource(
						spoutWrapperMultipleOutputs, spoutId,
						(TypeInformation) TypeExtractor.getForClass(SplitStreamType.class));

				SplitStream<SplitStreamType<Tuple>> splitSource = multiSource
						.split(new StormStreamSelector<Tuple>());
				for (String streamId : sourceStreams.keySet()) {
					outputStreams.put(streamId, splitSource.select(streamId).map(new SplitStreamMapper<Tuple>()));
				}
				source = multiSource;
			}
			availableInputs.put(spoutId, outputStreams);

			int dop = 1;
			final ComponentCommon common = stormTopology.get_spouts().get(spoutId).get_common();
			if (common.is_set_parallelism_hint()) {
				dop = common.get_parallelism_hint();
				source.setParallelism(dop);
			} else {
				common.set_parallelism_hint(1);
			}
		}

		final HashMap<String, Set<Entry<GlobalStreamId, Grouping>>> unprocessdInputsPerBolt =
				new HashMap<String, Set<Entry<GlobalStreamId, Grouping>>>();

		/* Because we do not know the order in which an iterator steps over a set, we might process a consumer before
		 * its producer
		 * ->thus, we might need to repeat multiple times
		 */
		boolean makeProgress = true;
		while (bolts.size() > 0) {
			if (!makeProgress) {
				throw new RuntimeException(
						"Unable to build Topology. Could not connect the following bolts: "
								+ bolts.keySet());
			}
			makeProgress = false;

			final Iterator<Entry<String, IRichBolt>> boltsIterator = bolts.entrySet().iterator();
			while (boltsIterator.hasNext()) {

				final Entry<String, IRichBolt> bolt = boltsIterator.next();
				final String boltId = bolt.getKey();
				final IRichBolt userBolt = copyObject(bolt.getValue());

				final ComponentCommon common = stormTopology.get_bolts().get(boltId).get_common();

				Set<Entry<GlobalStreamId, Grouping>> unprocessedInputs = unprocessdInputsPerBolt.get(boltId);
				if (unprocessedInputs == null) {
					unprocessedInputs = new HashSet<Entry<GlobalStreamId, Grouping>>();
					unprocessedInputs.addAll(common.get_inputs().entrySet());
					unprocessdInputsPerBolt.put(boltId, unprocessedInputs);
				}

				// connect each available producer to the current bolt
				final Iterator<Entry<GlobalStreamId, Grouping>> inputStreamsIterator = unprocessedInputs.iterator();
				while (inputStreamsIterator.hasNext()) {

					final Entry<GlobalStreamId, Grouping> stormInputStream = inputStreamsIterator.next();
					final String producerId = stormInputStream.getKey().get_componentId();
					final String inputStreamId = stormInputStream.getKey().get_streamId();

					final HashMap<String, DataStream<Tuple>> producer = availableInputs.get(producerId);
					if (producer != null) {
						makeProgress = true;

						DataStream<Tuple> inputStream = producer.get(inputStreamId);
						if (inputStream != null) {
							final FlinkOutputFieldsDeclarer declarer = new FlinkOutputFieldsDeclarer();
							userBolt.declareOutputFields(declarer);
							final HashMap<String, Fields> boltOutputStreams = declarer.outputStreams;
							this.outputStreams.put(boltId, boltOutputStreams);
							this.declarers.put(boltId, declarer);

							// if producer was processed already
							final Grouping grouping = stormInputStream.getValue();
							if (grouping.is_set_shuffle()) {
								// Storm uses a round-robin shuffle strategy
								inputStream = inputStream.rebalance();
							} else if (grouping.is_set_fields()) {
								// global grouping is emulated in Storm via an empty fields grouping list
								final List<String> fields = grouping.get_fields();
								if (fields.size() > 0) {
									FlinkOutputFieldsDeclarer prodDeclarer = this.declarers.get(producerId);
									inputStream = inputStream.keyBy(prodDeclarer
											.getGroupingFieldIndexes(inputStreamId,
													grouping.get_fields()));
								} else {
									inputStream = inputStream.global();
								}
							} else if (grouping.is_set_all()) {
								inputStream = inputStream.broadcast();
							} else if (!grouping.is_set_local_or_shuffle()) {
								throw new UnsupportedOperationException(
										"Flink only supports (local-or-)shuffle, fields, all, and global grouping");
							}

							final SingleOutputStreamOperator<?, ?> outputStream;

							if (boltOutputStreams.size() < 2) { // single output stream or sink
								String outputStreamId = null;
								if (boltOutputStreams.size() == 1) {
									outputStreamId = (String) boltOutputStreams.keySet().toArray()[0];
								}
								final TypeInformation<Tuple> outType = declarer
										.getOutputType(outputStreamId);

								final BoltWrapper<Tuple, Tuple> boltWrapperSingleOutput = new BoltWrapper<Tuple, Tuple>(
										userBolt, this.outputStreams.get(producerId).get(
												inputStreamId));
								boltWrapperSingleOutput.setStormTopology(stormTopology);

								final SingleOutputStreamOperator<Tuple, ?> outStream = inputStream
										.transform(boltId, outType, boltWrapperSingleOutput);

								if (outType != null) {
									// only for non-sink nodes
									final HashMap<String, DataStream<Tuple>> op = new HashMap<String, DataStream<Tuple>>();
									op.put(outputStreamId, outStream);
									availableInputs.put(boltId, op);
								}
								outputStream = outStream;
							} else {
								final BoltWrapper<Tuple, SplitStreamType<Tuple>> boltWrapperMultipleOutputs = new BoltWrapper<Tuple, SplitStreamType<Tuple>>(
										userBolt, this.outputStreams.get(producerId).get(
												inputStreamId));
								boltWrapperMultipleOutputs.setStormTopology(stormTopology);

								@SuppressWarnings({ "unchecked", "rawtypes" })
								final TypeInformation<SplitStreamType<Tuple>> outType = (TypeInformation) TypeExtractor
										.getForClass(SplitStreamType.class);

								final SingleOutputStreamOperator<SplitStreamType<Tuple>, ?> multiStream = inputStream
										.transform(boltId, outType, boltWrapperMultipleOutputs);

								final SplitStream<SplitStreamType<Tuple>> splitStream = multiStream
										.split(new StormStreamSelector<Tuple>());

								final HashMap<String, DataStream<Tuple>> op = new HashMap<String, DataStream<Tuple>>();
								for (String outputStreamId : boltOutputStreams.keySet()) {
									op.put(outputStreamId,
											splitStream.select(outputStreamId).map(
													new SplitStreamMapper<Tuple>()));
								}
								availableInputs.put(boltId, op);
								outputStream = multiStream;
							}

							if (common.is_set_parallelism_hint()) {
								int dop = common.get_parallelism_hint();
								outputStream.setParallelism(dop);
							} else {
								common.set_parallelism_hint(1);
							}

							inputStreamsIterator.remove();
						} else {
							throw new RuntimeException("Cannot connect '" + boltId + "' to '"
									+ producerId + "'. Stream '" + inputStreamId + "' not found.");
						}
					}
				}

				if (unprocessedInputs.size() == 0) {
					// all inputs are connected; processing bolt completed
					boltsIterator.remove();
				}
			}
		}
		return env;
	}

	// for internal testing purpose only
	public StormTopology getStormTopology() {
		return this.stormTopology;
	}
}
