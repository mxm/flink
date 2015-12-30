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

package org.apache.flink.yarn;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.AbstractFrameworkMaster;

import org.apache.flink.yarn.messages.ContainersAllocated;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class YarnFrameworkMaster extends AbstractFrameworkMaster {

	private static final FiniteDuration FAST_YARN_HEARTBEAT_INTERVAL = new FiniteDuration(500, TimeUnit.MILLISECONDS);

	private static final FiniteDuration DEFAULT_YARN_HEARTBEAT_INTERVAL = new FiniteDuration(5, TimeUnit.SECONDS);
	
	
	private final Map<String, Container> containersInLaunch;
	
	private final FiniteDuration yarnHeartbeatInterval;
	
	private final String webInterfaceURL;
	

	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;
	
	private NMClient nodeManagerClient;
	
	private ContainerLaunchContextFactory launchContextFactory;
	
	private int numPendingContainerRequests;
	
	
	
	public YarnFrameworkMaster(
			Configuration flinkConfig, 
			YarnConfiguration yarnConfig,
			FileSystem hadoopFileSystem,
			File taskManagerConfigFile,
			int numInitialTaskManagers,
			String webInterfaceURL) {
		
		super(flinkConfig, numInitialTaskManagers);
		
		this.webInterfaceURL = webInterfaceURL;
		
		// heartbeat interval
		this.yarnHeartbeatInterval = flinkConfig.containsKey(ConfigConstants.YARN_HEARTBEAT_DELAY_SECONDS) ?
			new FiniteDuration(flinkConfig.getInteger(ConfigConstants.YARN_HEARTBEAT_DELAY_SECONDS, 5), TimeUnit.SECONDS) :
			DEFAULT_YARN_HEARTBEAT_INTERVAL;
	}

	// ------------------------------------------------------------------------
	//  Actor life cycle
	// ------------------------------------------------------------------------

	@Override
	protected void processAcceptedMessage(Object message) {
		if (message instanceof ContainersAllocated) {
			containersAllocated(((ContainersAllocated) message).containers());
		}
		else {
			// message handled by the generic resource master code
			super.processAcceptedMessage(message);
		}
	}

	// ------------------------------------------------------------------------
	//  YARN specific behavior
	// ------------------------------------------------------------------------

	@Override
	protected void frameworkMasterActivated() throws Exception {
		log.info("Starting YARN resource master");

			val memoryLimit = Utils.calculateHeapSize(memoryPerTaskManager, flinkConfiguration)

			val applicationMasterHost = env.get(Environment.NM_HOST.key)
			require(applicationMasterHost != null, s"Application master (${Environment.NM_HOST} not set.")

			val yarnExpiryInterval: FiniteDuration = FiniteDuration(
				conf.getInt(
					YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
					YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS),
				MILLISECONDS)

			if (YARN_HEARTBEAT_DELAY.gteq(yarnExpiryInterval)) {
				log.warn(s"The heartbeat interval of the Flink Application master " +
					s"($YARN_HEARTBEAT_DELAY) is greater than YARN's expiry interval " +
					s"($yarnExpiryInterval). The application is likely to be killed by YARN.")
			}

			numTaskManagers = env.get(FlinkYarnClientBase.ENV_TM_COUNT).toInt
			maxFailedContainers = flinkConfiguration.
				getInteger(ConfigConstants.YARN_MAX_FAILED_CONTAINERS, numTaskManagers)

			log.info(s"Yarn session with $numTaskManagers TaskManagers. Tolerating " +
				s"$maxFailedContainers failed TaskManagers")

			val remoteFlinkJarPath = env.get(FlinkYarnClientBase.FLINK_JAR_PATH)
			val fs = FileSystem.get(conf)
			val appId = env.get(FlinkYarnClientBase.ENV_APP_ID)
			val currDir = env.get(Environment.PWD.key())
			val clientHomeDir = env.get(FlinkYarnClientBase.ENV_CLIENT_HOME_DIR)
			val shipListString = env.get(FlinkYarnClientBase.ENV_CLIENT_SHIP_FILES)
			val yarnClientUsername = env.get(FlinkYarnClientBase.ENV_CLIENT_USERNAME)


			// wrap default client in asynchronous handler thread
			val rmClientAsync: AMRMClientAsync[ContainerRequest] = AMRMClientAsync.createAMRMClientAsync(
				FAST_YARN_HEARTBEAT_DELAY.toMillis.toInt,
				AMRMClientAsyncHandler)

			// inject client into handler to adjust the heartbeat interval and make requests
			AMRMClientAsyncHandler.setClient(rmClientAsync)

			rmClientAsync.init(conf)
			rmClientAsync.start()

			rmClientOption = Some(rmClientAsync)

			val nm = NMClient.createNMClient()
			nm.init(conf)
			nm.start()
			nm.cleanupRunningContainersOnStop(true)

			nmClientOption = Some(nm)

			// Register with ResourceManager
			val url = s"http://$applicationMasterHost:$webServerPort"
			log.info(s"Registering ApplicationMaster with tracking url $url.")

			val actorSystemPort = AkkaUtils.getAddress(system).port.getOrElse(-1)

			val response = rmClientAsync.registerApplicationMaster(
				applicationMasterHost,
				actorSystemPort,
				url)

			val containersFromPreviousAttempts = getContainersFromPreviousAttempts(response)

			log.info(s"Retrieved ${containersFromPreviousAttempts.length} TaskManagers from previous " +
				s"attempts.")

			runningContainersList ++= containersFromPreviousAttempts
			runningContainers = runningContainersList.length

			// Make missing container requests to ResourceManager
			runningContainers until numTaskManagers foreach {
				i =>
				val containerRequest = getContainerRequest(memoryPerTaskManager)
				log.info(s"Requesting initial TaskManager container $i.")
				numPendingRequests += 1
				// these are initial requests. The reallocation setting doesn't affect this.
				rmClientAsync.addContainerRequest(containerRequest)
			}

			val flinkJar = Records.newRecord(classOf[LocalResource])
			val flinkConf = Records.newRecord(classOf[LocalResource])

			// register Flink Jar with remote HDFS
			val remoteJarPath = new Path(remoteFlinkJarPath)
			Utils.registerLocalResource(fs, remoteJarPath, flinkJar)

			// register conf with local fs
			Utils.setupLocalResource(conf, fs, appId, new Path(s"file://$currDir/flink-conf-modified" +
				s".yaml"), flinkConf, new Path(clientHomeDir))
			log.info(s"Prepared local resource for modified yaml: $flinkConf")

			val hasLogback = new File(s"$currDir/logback.xml").exists()
			val hasLog4j = new File(s"$currDir/log4j.properties").exists()

			// prepare files to be shipped
			val resources = shipListString.split(",") flatMap {
				pathStr =>
				if (pathStr.isEmpty) {
					None
				} else {
					val resource = Records.newRecord(classOf[LocalResource])
					val path = new Path(pathStr)
					Utils.registerLocalResource(fs, path, resource)
					Some((path.getName, resource))
				}
			} toList

			val taskManagerLocalResources = ("flink.jar", flinkJar) ::("flink-conf.yaml",
				flinkConf) :: resources toMap

			failedContainers = 0

			containerLaunchContext = Some(
				createContainerLaunchContext(
					memoryLimit,
					hasLogback,
					hasLog4j,
					yarnClientUsername,
					conf,
					taskManagerLocalResources)
			)

		} recover {
			case t: Throwable =>
				log.error("Could not start yarn session.", t)
				self ! decorateMessage(
				StopYarnSession(
					FinalApplicationStatus.FAILED,
					s"ApplicationMaster failed while starting. Exception Message: ${t.getMessage}")
			)
		}
	}
	
	@Override
	protected void requestNewWorkers(int numWorkers) {
		for (int i = 0; i < numWorkers; i++) {
			numPendingContainerRequests++;
			log.info("Requesting new TaskManager container with {} megabytes memory. Pending requests: {}",
				taskManagerMemoryMB, numPendingContainerRequests);
			
			resourceManagerClient.addContainerRequest(createContainerRequest());
		}
	}

	// ------------------------------------------------------------------------
	//  YARN Framework Master specific actor messages
	// ------------------------------------------------------------------------
	
	protected void containersAllocated(List<Container> containers) {
		for (Container container : containers) {
			numPendingContainerRequests--;
			LOG.info("Received new container: {} - Remaining pending container requests: {}",
				container.getId(), numPendingContainerRequests);
			
			// decide whether to return the container, or whether to start a TaskManager
			final int targetNum = getDesiredNumberOfTaskManagers();
			
			if (getNumberOfRegisteredTaskManagers() + containersInLaunch.size() < targetNum) {
				// start a TaskManager
				String containerIdString = container.getId().toString();
				ContainerLaunchContext launchContext = launchContextFactory.createLaunchContext(containerIdString);
				try {
					nodeManagerClient.startContainer(container, launchContext);
				}
				catch (Throwable t) {
					// failed to launch the container. return container and request a new one
					// TODO
					return;
				}
				
				containersInLaunch.put(containerIdString, container);
				
				String message = "Launching TaskManager in container " + container.getId()
					+ " on host " + container.getNodeId().getHost();
				log.info(message);
				sendInfoMessage(message);
			}
			else {
				// return excessive container
				log.info("Returning excessive container {}", container.getId()); 
				resourceManagerClient.releaseAssignedContainer(container.getId());
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private AMRMClient.ContainerRequest createContainerRequest() {
		// Priority for worker containers - priorities are intra-application
		Priority priority = Priority.newInstance(0);
		
		// Resource requirements for worker containers
		// NOTE: VCores are hard-coded to one currently (YARN is not accounting for CPUs)
		Resource capability = Resource.newInstance(taskManagerMemoryMB, 1);
		
		return new AMRMClient.ContainerRequest(capability, null, null, priority);
	}
	
	@SuppressWarnings("unchecked")
	private List<Container> getContainersFromPreviousAttempts(RegisterApplicationMasterResponse response) {
		try {
			Method m = RegisterApplicationMasterResponse.class
				.getMethod("getContainersFromPreviousAttempts");
			
			return (List<Container>) m.invoke(response);
		}
		catch (NoSuchMethodException e) {
			// that happens in earlier Hadoop versions
			log.info("Cannot reconnect to previously allocated containers. " +
				"This YARN version does not support 'getContainersFromPreviousAttempts()'");
		}
		catch (Throwable t) {
			log.error("Error invoking 'getContainersFromPreviousAttempts()'", t);
		}
		
		return Collections.emptyList();
	}
}
