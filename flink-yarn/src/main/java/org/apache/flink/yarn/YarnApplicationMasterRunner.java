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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import akka.actor.Props;
import org.apache.flink.client.CliFrontend;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.AbstractFrameworkMaster;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.jobmanager.MemoryArchivist;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.runtime.webmonitor.WebMonitor;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * This class is the executable entry point for the YARN application master.
 * It starts actor system and the actors for {@link org.apache.flink.runtime.jobmanager.JobManager}
 * and {@link org.apache.flink.yarn.YarnFrameworkMaster}.
 * 
 * The JobManager handles Flink job execution, while the YarnFrameworkMaster handles container
 * allocation and failure detection.
 */
public class YarnApplicationMasterRunner {

	/** Logger */
	private static final Logger LOG = LoggerFactory.getLogger(YarnApplicationMasterRunner.class);

	private static final FiniteDuration TASKMANAGER_REGISTRATION_TIMEOUT = new FiniteDuration(5, TimeUnit.MINUTES);
	
	/** The process environment variables */
	private static final Map<String, String> ENV = System.getenv();
	
	/** The exit code returned if the initialization of the application master failed */
	private static final int INIT_ERROR_EXIT_CODE = 31;


	// ------------------------------------------------------------------------
	//  Program entry point
	// ------------------------------------------------------------------------

	/**
	 * The entry point for the YARN application master. 
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN ApplicationMaster / JobManager", args);
		SignalHandler.register(LOG);

		// run and exit with the proper code
		int returnCode = new YarnApplicationMasterRunner().run(args);
		System.exit(returnCode);
	}
	
	/**
	 * The instance entry point for the YARN application master. Obtains user group
	 * information and calls the main work method {@link #runApplicationMaster()} as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 * @return The process exit code.
	 */
	protected int run(String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);
			
			final String yarnClientUsername = ENV.get(YarnConfigKeys.ENV_CLIENT_USERNAME);
			require(yarnClientUsername != null, "YARN client user name environment variable {} not set",
				YarnConfigKeys.ENV_CLIENT_USERNAME);
		
			final UserGroupInformation currentUser;
			try {
				currentUser = UserGroupInformation.getCurrentUser();
			} catch (Throwable t) {
				throw new Exception("Cannot access UserGroupInformation information for current user", t);
			}
		
			LOG.info("YARN daemon runs as user {}. Running Flink ApplicationMaster/JobManager as user {}",
				currentUser.getShortUserName(), yarnClientUsername);
	
			UserGroupInformation ugi = UserGroupInformation.createRemoteUser(yarnClientUsername);
			
			// transfer all security tokens, for example for authenticated HDFS and HBase access
			for (Token<?> token : currentUser.getTokens()) {
				ugi.addToken(token);
			}
	
			// run the actual work in a secured privileged action
			return ugi.doAs(new PrivilegedAction<Integer>() {
				@Override
				 public Integer run() {
					return runApplicationMaster();
				}
			});
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN Application Master initialization failed", t);
			return INIT_ERROR_EXIT_CODE;
		}
	}

	// ------------------------------------------------------------------------
	//  Core work method
	// ------------------------------------------------------------------------
	
	/**
	 * The main work method, must run as a privileged action.
	 * 
	 * @return The return code for the Java process. 
	 */
	protected int runApplicationMaster() {
		ActorSystem actorSystem = null;
		WebMonitor webMonitor = null;
		
		try {
			// ---- (1) load and parse / validate all configurations
			
			final String currDir = ENV.get(Environment.PWD.key());
			require(currDir != null, "Current working directory variable (%s) not set", Environment.PWD.key());
	
			// Note that we use the "appMasterHostname" given by YARN here, to make sure
			// we use the hostnames given by YARN consistently throughout akka.
			// for akka "localhost" and "localhost.localdomain" are different actors.
			final String appMasterHostname = ENV.get(Environment.NM_HOST.key());
			require(appMasterHostname != null,
				"ApplicationMaster hostname variable %s not set", Environment.NM_HOST.key());
	
			LOG.info("YARN assigned hostname for application master: {}", appMasterHostname);
			
			final Map<String, String> dynamicProperties = 
				CliFrontend.getDynamicProperties(ENV.get(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES));
			LOG.debug("YARN dynamic properties: {}", dynamicProperties);
			
			final int numInitialTaskManagers;
			final int slotsPerTaskManager;
			
			try {
				numInitialTaskManagers = Integer.parseInt(ENV.get(YarnConfigKeys.ENV_TM_COUNT));
			} catch (NumberFormatException e) {
				throw new RuntimeException("Invalid value for " + YarnConfigKeys.ENV_TM_COUNT + " : "
					+ e.getMessage());
			}
			try {
				slotsPerTaskManager = Integer.parseInt(ENV.get(YarnConfigKeys.ENV_SLOTS));
			} catch (NumberFormatException e) {
				throw new RuntimeException("Invalid value for " + YarnConfigKeys.ENV_SLOTS + " : "
					+ e.getMessage());
			}
			
			// Flink configuration
			final Configuration config = createConfiguration(currDir, dynamicProperties);
			
			// Hadoop/Yarn configuration (loads config data automatically from classpath files)
			final YarnConfiguration yarnConfig = new YarnConfiguration();
			final org.apache.hadoop.fs.FileSystem hadoopFileSystem =
				org.apache.hadoop.fs.FileSystem.get(yarnConfig);
			
			
			// ---- (2) start the actor system
			
			// try to start the actor system, JobManager and JobManager actor system
			// using the port range definition from the config.
			final String amPortRange = config.getString(
					ConfigConstants.YARN_APPLICATION_MASTER_PORT,
					ConfigConstants.DEFAULT_YARN_APPLICATION_MASTER_PORT);
			
			actorSystem = BootstrapTools.startActorSystem(config, appMasterHostname, amPortRange, LOG);
			
			final String akkaHostname = AkkaUtils.getAddress(actorSystem).host().get();
			final int akkaPort = (Integer) AkkaUtils.getAddress(actorSystem).port().get();

			LOG.info("Actor system bound to hostname {}.", akkaHostname);
			
			// ---- (3) Generate the configuration for the TaskManagers
			
			final Configuration taskManagerConfig = BootstrapTools.generateTaskManagerConfiguration(
					config, akkaHostname, akkaPort, slotsPerTaskManager, TASKMANAGER_REGISTRATION_TIMEOUT);
			LOG.debug("TaskManager configuration: {}", taskManagerConfig);
			
			final File taskManagerConfigFile = new File(currDir, UUID.randomUUID() + "-taskmanager-cong.yaml");
			LOG.debug("Writing TaskManager configuration to {}", taskManagerConfigFile.getAbsolutePath());
			BootstrapTools.writeConfiguration(taskManagerConfig, taskManagerConfigFile);
			
			// ---- (4) start the actors and components in this order:
			
			// 1) Web Monitor (we need its port to register)
			// 2) Resource framework master for YARN (needs to run before JobManager)
			// 3) JobManager & Archive
			
			webMonitor = BootstrapTools.startWebMonitorIfConfigured(config, actorSystem, LOG);
			final String webMonitorURL = webMonitor == null ? null :
				"http://" + appMasterHostname + ":" + webMonitor.getServerPort();
			
			Props resourceMasterProps = Props.create(
				YarnFrameworkMaster.class,
				config, yarnConfig, hadoopFileSystem, taskManagerConfigFile,
				numInitialTaskManagers, webMonitorURL);
			
			ActorRef resourceMaster = actorSystem.actorOf(resourceMasterProps, AbstractFrameworkMaster.ACTOR_NAME);
			
			
			
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN Application Master initialization failed", t);
			
			if (actorSystem != null) {
				try {
					actorSystem.shutdown();
				} catch (Throwable tt) {
					LOG.error("Error shutting down actor system", tt);
				}
			}

			if (webMonitor != null) {
				try {
					webMonitor.stop();
				} catch (Throwable ignored) {}
			}
			
			return INIT_ERROR_EXIT_CODE;
		}
		
		// everything started, we can wait until all is done or the process is killed
		LOG.info("YARN Application Master started");
		
		// wait until everything is done
		actorSystem.awaitTermination();

		// if we get here, everything work out jolly all right, and we even exited smoothly
		if (webMonitor != null) {
			try {
				webMonitor.stop();
			} catch (Throwable ignored) {}
		}
		return 0;
	}

	// ------------------------------------------------------------------------
	//  For testing, this allows to override the actor classes used for
	//  JobManager and the archive of completed jobs
	// ------------------------------------------------------------------------
	
	protected Class<? extends JobManager> getJobManagerClass() {
		return JobManager.class;
	}

	protected Class<? extends MemoryArchivist> getArchivistClass() {
		return MemoryArchivist.class;
	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------
	
	private static void require(boolean condition, String message, Object... values) {
		if (!condition) {
			throw new RuntimeException(String.format(message, values));
		}
	}
	
	private static Configuration createConfiguration(String baseDirectory, Map<String, String> additional) {
		LOG.info("Loading config from directory " + baseDirectory);

		GlobalConfiguration.loadConfiguration(baseDirectory);
		Configuration configuration = GlobalConfiguration.getConfiguration();

		configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, baseDirectory);

		// add dynamic properties to JobManager configuration.
		for (Map.Entry<String, String> property : additional.entrySet()) {
			configuration.setString(property.getKey(), property.getValue());
		}

		// if a web monitor shall be started, set the port to random binding
		if (configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0) >= 0) {
			configuration.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0);
		}
		
		return configuration;
	}
}
