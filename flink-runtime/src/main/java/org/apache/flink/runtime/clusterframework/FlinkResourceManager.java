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

package org.apache.flink.runtime.clusterframework;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.dispatch.OnComplete;
import akka.pattern.Patterns;
import akka.util.Timeout;

import com.google.common.base.Preconditions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.clusterframework.messages.CheckAndAllocateContainers;
import org.apache.flink.runtime.clusterframework.messages.FatalErrorOccurred;
import org.apache.flink.runtime.clusterframework.messages.InfoMessage;
import org.apache.flink.runtime.clusterframework.messages.RegisterInfoMessageListenerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.RegisterResource;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceFailed;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManagerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceSuccessful;
import org.apache.flink.runtime.clusterframework.messages.NewLeaderAvailable;
import org.apache.flink.runtime.clusterframework.messages.RegisterInfoMessageListener;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RemoveResource;
import org.apache.flink.runtime.clusterframework.messages.ResourceRemoved;
import org.apache.flink.runtime.clusterframework.messages.SetWorkerPoolSize;
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.clusterframework.messages.TriggerRegistrationAtJobManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;

import org.apache.flink.runtime.messages.RegistrationMessages;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.Future;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 *
 * <h1>Worker allocation steps</h1>
 *
 * <ol>
 *     <li>The resource manager decides to request more workers. This can happen in order
 *         to fill the initial pool, or as a result of the JobManager requesting more workers.</li>
 *
 *     <li>The resource master calls {@link #requestNewWorkers(int)}, which triggers requests
 *         for more containers. After that, the {@link #getNumWorkerRequestsPending()}
 *         should reflect the pending requests.</li>
 *
 *     <li>The concrete framework may acquire containers and then trigger to start TaskManagers
 *         in those containers. That should be reflected in {@link #getNumWorkersPendingRegistration()}.</li>
 *
 *     <li>At some point, the TaskManager processes will have started and send a registration
 *         message to the JobManager. The JobManager will perform
 *         a lookup with the ResourceManager to check if it really started this TaskManager.
 *         The method {@link #workerRegistered(ResourceID)} will be called
 *         to inform about a registered worker.</li>
 * </ol>
 *
 */
public abstract class FlinkResourceManager<WorkerType extends ResourceID> extends FlinkUntypedActor {

	/** The exit code with which the process is stopped in case of a fatal error */
	protected static final int EXIT_CODE_FATAL_ERROR = -13;

	/** The default name of the resource manager actor */
	public static final String RESOURCE_MANAGER_NAME = "resourcemanager";

	// ------------------------------------------------------------------------

	/** The logger, named for the actual implementing class */
	protected final Logger log = LoggerFactory.getLogger(getClass());

	/** The Flink configuration object */
	protected final Configuration config;

	/** The timeout for actor messages sent to the JobManager / TaskManagers */
	private final FiniteDuration messageTimeout;

	/** The service to find the right leader JobManager (to support high availability) */
	private final LeaderRetrievalService leaderRetriever;

	/** The currently registered resources */
	private final Map<ResourceID, WorkerType> registeredWorkers;

	/** List of listeners for info messages */
	private ActorRef infoMessageListener;

	/** The JobManager that the framework master manages resources for */
	private ActorRef jobManager;

	/** Our JobManager's leader session */
	private UUID leaderSessionID;

	/** The size of the worker pool that the resource master strives to maintain */
	private int designatedPoolSize;

	// ------------------------------------------------------------------------


	/**
	 * Creates a AbstractFrameworkMaster actor.
	 *
	 * @param flinkConfig The Flink configuration object.
	 */
	protected FlinkResourceManager(Configuration flinkConfig, LeaderRetrievalService leaderRetriever) {
		this.config = requireNonNull(flinkConfig);
		this.leaderRetriever = requireNonNull(leaderRetriever);
		this.registeredWorkers = new HashMap<>();

		FiniteDuration lt;
		try {
			lt = AkkaUtils.getLookupTimeout(config);
		}
		catch (Exception e) {
			lt = new FiniteDuration(
				Duration.apply(ConfigConstants.DEFAULT_AKKA_LOOKUP_TIMEOUT).toMillis(),
				TimeUnit.MILLISECONDS);
		}
		this.messageTimeout = lt;
	}

	// ------------------------------------------------------------------------
	//  Actor Behavior
	// ------------------------------------------------------------------------

	@Override
	public void preStart() {
		// we start our leader retrieval service to make sure we get informed
		// about JobManager leader changes
		try {
			leaderRetriever.start(new LeaderRetrievalListener() {

				@Override
				public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
					self().tell(
						new NewLeaderAvailable(leaderAddress, leaderSessionID),
						ActorRef.noSender());
				}

				@Override
				public void handleError(Exception e) {
					self().tell(
						new FatalErrorOccurred("Leader retrieval service failed", e),
						ActorRef.noSender());
				}
			});
		}
		catch (Throwable t) {
			self().tell(
				new FatalErrorOccurred("Could not start leader retrieval service", t),
				ActorRef.noSender());
		}
		// framework specific initialization
		try {
			initialize();
		}
		catch (Throwable t) {
			self().tell(
				new FatalErrorOccurred("Error while initializing the resource manager", t),
				ActorRef.noSender());
		}
	}

	@Override
	public void postStop() {
		try {
			leaderRetriever.stop();
		}
		catch (Throwable t) {
			LOG.error("Could not cleanly shut down leader retrieval service", t);
		}
	}

	/**
	 *
	 * This method receives the actor messages after they have been filtered for
	 * a match with the leader session.
	 *
	 * @param message The incoming actor message.
	 */
	@Override
	protected void handleMessage(Object message) {
		try {
			// --- messages about worker allocation and pool sizes

			if (message instanceof CheckAndAllocateContainers) {
				checkWorkersPool();
			}
			else if (message instanceof SetWorkerPoolSize) {
				SetWorkerPoolSize msg = (SetWorkerPoolSize) message;
				adjustDesignatedNumberOfWorkers(msg.numberOfWorkers());
			}
			else if (message instanceof RemoveResource) {
				RemoveResource msg = (RemoveResource) message;
				removeRegisteredResource(msg.resourceId());
			}

			// --- lookup of registered resources

			else if (message instanceof RegisterResource) {
				RegisterResource msg = (RegisterResource) message;
				handleRegisterResource(sender(), msg.getTaskManager(), msg.getRegisterMessage());
			}

			// --- messages about JobManager leader status and registration

			else if (message instanceof NewLeaderAvailable) {
				NewLeaderAvailable msg = (NewLeaderAvailable) message;
				newJobManagerLeaderAvailable(msg.leaderAddress(), msg.leaderSessionId());
			}
			else if (message instanceof TriggerRegistrationAtJobManager) {
				TriggerRegistrationAtJobManager msg = (TriggerRegistrationAtJobManager) message;
				triggerConnectingToJobManager(msg.jobManagerAddress());
			}
			else if (message instanceof RegisterResourceManagerSuccessful) {
				RegisterResourceManagerSuccessful msg = (RegisterResourceManagerSuccessful) message;
				jobManagerLeaderConnected(sender(), msg.currentlyRegisteredTaskManagers());
			}

			// --- end of application

			else if (message instanceof StopCluster) {
				StopCluster msg = (StopCluster) message;
				shutdownCluster(msg.finalStatus(), msg.message());
			}

			// --- miscellaneous messages

			else if (message instanceof RegisterInfoMessageListener) {
				registerMessageListener(sender());
				sender().tell(decorateMessage(
					RegisterInfoMessageListenerSuccessful.get()),
					self());
			}

			// --- unknown messages

			else {
				LOG.error("Discarding unknown message: {}", message);
			}
		}
		catch (Throwable t) {
			// fatal error, needs master recovery
			fatalError("Error processing actor message", t);
		}
	}

	@Override
	protected final UUID getLeaderSessionID() {
		return leaderSessionID;
	}

	// ------------------------------------------------------------------------
	//  Status
	// ------------------------------------------------------------------------

	/**
	 * Gets the current designated worker pool size, meaning the number of workers
	 * that the resource master strives to maintain. The actual number of workers
	 * may be lower (if worker requests are still pending) or higher (if workers have
	 * not yet been released).
	 *
	 * @return The designated worker pool size.
	 */
	public int getDesignatedWorkerPoolSize() {
		return designatedPoolSize;
	}

	/**
	 * Gets the number of currently registered TaskManagers.
	 *
	 * @return The number of currently registered TaskManagers.
	 */
	public int getNumberOfRegisteredTaskManagers() {
		return registeredWorkers.size();
	}

	/**
	 * Gets the currently registered resources.
	 * @return
	 */
	public Collection<WorkerType> getRegisteredTaskManagers() {
		return registeredWorkers.values();
	}

	/**
	 * Gets the registered worker for a given resource ID, if one is available.
	 *
	 * @param resourceId The resource ID for the worker.
	 * @return True if already registered, otherwise false
	 */
	public boolean isRegistered(ResourceID resourceId) {
		return registeredWorkers.containsKey(resourceId);
	}

	/**
	 * Gets an iterable for all currently registered TaskManagers.
	 *
	 * @return All currently registered TaskManagers.
	 */
	public Collection<WorkerType> allRegisteredWorkers() {
		return registeredWorkers.values();
	}

	/**
	 * Register a resource on which a TaskManager has been started
	 * @param jobManager The sender (JobManager) of the message
	 * @param taskManager The task manager who wants to register
	 * @param msg The task manager's registration message
	 */
	private void handleRegisterResource(ActorRef jobManager, ActorRef taskManager,
				RegistrationMessages.RegisterTaskManager msg) {

		ResourceID resourceID = msg.resourceId();
		try {
			Preconditions.checkNotNull(resourceID);
			WorkerType newWorker = workerRegistered(msg.resourceId());
			WorkerType oldWorker = registeredWorkers.put(resourceID, newWorker);
			if (oldWorker != null) {
				LOG.warn("Worker {} had been registered before.", resourceID);
			}
			jobManager.tell(decorateMessage(
				new RegisterResourceSuccessful(taskManager, msg)),
				self());
		} catch (Exception e) {
			LOG.error("TaskManager resource registration failed.", e);

			// tell the TaskManager about the failure
			String eStr = ExceptionUtils.stringifyException(e);
			sender().tell(decorateMessage(
				new RegisterResourceFailed(taskManager, resourceID, eStr)), self());
		}
	}

	/**
	 * Releases the given resource. Note that this does not automatically shrink
	 * the designated worker pool size.
	 *
	 * @param resourceId The TaskManager's resource id.
	 */
	private void removeRegisteredResource(ResourceID resourceId) {

		WorkerType worker = registeredWorkers.remove(resourceId);
		if (worker != null) {
			releaseRegisteredWorker(worker);
		} else {
			LOG.warn("Resource {} could not be released", resourceId);
		}
	}


	// ------------------------------------------------------------------------
	//  Registration and consolidation with JobManager Leader
	// ------------------------------------------------------------------------

	/**
	 * Called as soon as we discover (via leader election) that a JobManager lost leadership
	 * or a different one gained leadership.
	 *
	 * @param leaderAddress The address (Akka URL) of the new leader. Null if there is currently no leader.
	 * @param leaderSessionID The unique session ID marking the leadership session.
	 */
	protected void newJobManagerLeaderAvailable(String leaderAddress, UUID leaderSessionID) {
		log.debug("Received new leading JobManager {}. Connecting.", leaderAddress);

		// disconnect from the current leader (no-op if no leader yet)
		jobManagerLostLeadership();

		// a null leader address means that only a leader disconnect
		// happened, without a new leader yet
		if (leaderAddress != null) {
			// the leaderSessionID implicitly filters out success and failure messages
			// that come after leadership changed again
			this.leaderSessionID = leaderSessionID;
			triggerConnectingToJobManager(leaderAddress);
		}
	}

	/**
	 * Causes the resource manager to announce itself at the new leader JobManager and
	 * obtains its connection information and currently known TaskManagers.
	 *
	 * @param leaderAddress The akka actor URL of the new leader JobManager.
	 */
	private void triggerConnectingToJobManager(String leaderAddress) {
		LOG.info("Trying to associate with JobManager leader " + leaderAddress);

		final Object registerMessage = decorateMessage(new RegisterResourceManager(self()));
		final Object retryMessage = decorateMessage(new TriggerRegistrationAtJobManager(leaderAddress));

		// send the registration message to the JobManager
		ActorSelection jobManagerSel = context().actorSelection(leaderAddress);
		Future<Object> future = Patterns.ask(jobManagerSel, registerMessage, new Timeout(messageTimeout));

		future.onComplete(new OnComplete<Object>() {

			@Override
			public void onComplete(Throwable failure, Object msg) {
				if (msg != null) {
					if (msg instanceof LeaderSessionMessage &&
						((LeaderSessionMessage) msg).message() instanceof RegisterResourceManagerSuccessful)
					{
						self().tell(msg, ActorRef.noSender());
					}
					else {
						LOG.error("Invalid response type to registration at JobManager: {}", msg);
						self().tell(retryMessage, ActorRef.noSender());
					}
				}
				else {
					// no success
					LOG.error("Resource manager could not register at JobManager");
					self().tell(retryMessage, ActorRef.noSender());
				}
			}

		}, context().dispatcher());
	}

	/**
	 * This method disassociates from the current leader JobManager. All currently registered
	 * TaskManagers are put under "awaiting registration".
	 */
	private void jobManagerLostLeadership() {
		if (jobManager != null) {
			LOG.info("Associated JobManager {} lost leader status", jobManager);

			jobManager = null;
			leaderSessionID = null;
			infoMessageListener = null;

			registeredWorkers.clear();
		}
	}

	/**
	 * Callback when we're informed about a new leading JobManager.
	 * @param newJobManagerLeader The ActorRef of the new jobManager
	 * @param workers The existing workers the JobManager has registered.
	 */
	private void jobManagerLeaderConnected(
						ActorRef newJobManagerLeader,
						Collection<ResourceID> workers) {

		if (jobManager == null) {
			LOG.info("Resource Manager associating with leading JobManager {} - leader session {}",
						newJobManagerLeader, leaderSessionID);

			jobManager = newJobManagerLeader;

			if (workers.size() > 0) {
				LOG.info("Received TaskManagers that were registered at the leader JobManager. " +
						"Trying to consolidate.");

				// keep track of which TaskManagers are not handled
				List<ResourceID> toHandle = new ArrayList<>(workers.size());
				for (ResourceID resourceID : workers) {
					toHandle.add(resourceID);
				}

				try {
					// ask the framework to tell us which ones we should keep for now
					Collection<WorkerType> consolidated = reacceptRegisteredWorkers(workers);
					log.info("Consolidated {} TaskManagers", consolidated.size());

					// put the consolidated TaskManagers into our bookkeeping
					for (WorkerType worker : consolidated) {
						registeredWorkers.put(worker, worker);
						toHandle.remove(worker);
					}
				}
				catch (Throwable t) {
					LOG.error("Error during consolidation of known TaskManagers", t);
					// the framework should release the remaining unclear resources
					for (ResourceID id : toHandle) {
						releasePendingWorker(id);
					}
				}

			}
		} else {
			String msg = "Attempting to associate with new JobManager leader " + newJobManagerLeader
				+ " without previously disassociating from current leader " + jobManager;
			fatalError(msg, new Exception(msg));
		}
	}

	// ------------------------------------------------------------------------
	//  Cluster Shutdown
	// ------------------------------------------------------------------------

	private void shutdownCluster(ApplicationStatus status, String diagnostics) {
		LOG.info("Shutting down cluster with status {} : {}", status, diagnostics);

		// TODO RM
//
//		// the shutdown consists of the following steps:
//		// 1) send message to job manager
//		// 2) await reply
//
//		ShutdownTaskManager shutdownMessage = new ShutdownTaskManager(
//			"The cluster is being shutdown with application status " + status);
//
//		Timeout timeout = new Timeout(messageTimeout);
//		Deadline deadline = messageTimeout.fromNow();
//
//		List<Future<?>> futures = new ArrayList<>(registeredWorkers.size());
//		for (ResourceID resourceID : registeredWorkers) {
//			futures.add(
//				Patterns.ask(tm.taskManagerActor(), decorateMessage(shutdownMessage),
//				timeout));
//		}
//
//		// now we wait on the futures until all nodes responded, or the
//		// futures time out. That gives the nodes a chance to shut down cleanly
//		try {
//			for (Future<?> f : futures) {
//				Await.ready(f, deadline.timeLeft());
//			}
//		} catch (InterruptedException ignored) {
//			// we make only a best effort to cleanly shut down. if we are interrupted,
//			// the framework logic must act as a failsafe to eventually deallocate the container
//		} catch (TimeoutException e) {
//			log.error("Failed to shutdown all workers.");
//		}
//
//		// shut the resource master down.
//		shutdownApplication(status, diagnostics);
//
//		// we shut down the whole process now.
//		int exitCode = status.processExitCode();
//		System.exit(exitCode);
	}

	// ------------------------------------------------------------------------
	//  Worker pool size management
	// ------------------------------------------------------------------------

	/**
	 * This method causes the resource framework master to <b>synchronously</b>re-examine
	 * the set of available and pending workers containers, and allocate containers
	 * if needed.
	 *
	 * This method does not automatically release workers, because it is not visible to
	 * this resource master which workers can be released. Instead, the JobManager must
	 * explicitly release individual workers.
	 */
	private void checkWorkersPool() {
		// see how many workers we want, and whether we have enough
		int allAvailableAndPending = registeredWorkers.size() +
			getNumWorkersPendingRegistration() + getNumWorkerRequestsPending();

		int missing = designatedPoolSize - allAvailableAndPending;
		if (missing > 0) {
			requestNewWorkers(missing);
		}
	}

	/**
	 * Sets the designated worker pool size. If this size is larger than the current pool
	 * size, then the resource manager will try to acquire more TaskManagers.
	 *
	 * @param num The number of workers in the pool.
	 */
	private void adjustDesignatedNumberOfWorkers(int num) {
		if (num >= 0) {
			log.info("Adjusting designated worker pool size to {}", num);
			designatedPoolSize = num;
			checkWorkersPool();
		} else {
			log.warn("Ignoring invalid designated worker pool size: " + num);
		}
	}

	// ------------------------------------------------------------------------
	//  Callbacks
	// ------------------------------------------------------------------------

	/**
	 * This method causes the resource framework master to <b>asynchronously</b>re-examine
	 * the set of available and pending workers containers, and release or allocate
	 * containers if needed. The method sends an actor message which will trigger the
	 * re-examination.
	 */
	public void triggerCheckWorkers() {
		self().tell(
			decorateMessage(
				CheckAndAllocateContainers.get()),
			ActorRef.noSender());
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * worker has failed.
	 *
	 * @param resourceID Id of the worker that has failed.
	 * @param message An informational message that explains why the worker failed.
	 */
	public void notifyWorkerFailed(ResourceID resourceID, String message) {
		WorkerType worker = registeredWorkers.remove(resourceID);
		if (worker != null) {
			jobManager.tell(
				decorateMessage(
					new ResourceRemoved(resourceID, message)),
				self());
		}
	}

	// ------------------------------------------------------------------------
	//  Framework specific behavior
	// ------------------------------------------------------------------------

	/**
	 * Initializes the framework specific components.
	 *
	 * @throws Exception Exceptions during initialization cause the resource manager to fail.
	 *                   If the framework is able to recover this resource manager, it will be
	 *                   restarted.
	 */
	protected abstract void initialize() throws Exception;

	/**
	 * The framework specific code for shutting down the application. This should report the
	 * application's final status and shut down the resource manager cleanly.
	 *
	 * This method also needs to make sure all pending containers that are not registered
	 * yet are returned.
	 *
	 * @param finalStatus The application status to report.
	 * @param optionalDiagnostics An optional diagnostics message.
	 */
	protected abstract void shutdownApplication(ApplicationStatus finalStatus, String optionalDiagnostics);

	/**
	 * Notifies the resource master of a fatal error.
	 *
	 * <p><b>IMPORTANT:</b> This should not cleanly shut down this master, but exit it in
	 * such a way that a high-availability setting would restart this or fail over
	 * to another master.
	 */
	protected abstract void fatalError(String message, Throwable error);

	/**
	 * Requests to allocate a certain number of new workers.
	 *
	 * @param numWorkers The number of workers to allocate.
	 */
	protected abstract void requestNewWorkers(int numWorkers);

	/**
	 * Trigger a release of a pending worker.
	 * @param resourceID The worker resource id
	 */
	protected abstract void releasePendingWorker(ResourceID resourceID);

	/**
	 * Trigger a release of a registered worker.
	 * @param resourceID The worker resource id
	 */
	protected abstract void releaseRegisteredWorker(WorkerType resourceID);

	/**
	 * Callback when a worker was registered.
	 * @param resourceID The worker resource id
	 */
	protected abstract WorkerType workerRegistered(ResourceID resourceID) throws Exception;

	/**
	 * This method is called when the resource manager starts after a failure and reconnects to
	 * the leader JobManager, who still has some workers registered. The method is used to consolidate
	 * the view between resource manager and JobManager. The resource manager gets the list of TaskManagers
	 * that the JobManager considers available and should return a list or nodes that the
	 * resource manager considers available.
	 *
	 * After that, the JobManager is informed of loss of all TaskManagers that are not part of the
	 * returned list.
	 *
	 * It is possible that the resource manager initially confirms some TaskManagers to be alive, even
	 * through they are in an uncertain status, if it later sends necessary failure notifications
	 * via calling {@link #notifyWorkerFailed(ResourceID, String)}.
	 *
	 * @param registered The list of TaskManagers that the JobManager knows.
	 * @return The subset of TaskManagers that the resource manager can confirm to be alive.
	 */
	protected abstract Collection<WorkerType> reacceptRegisteredWorkers(Collection<ResourceID> registered);

	/**
	 * Gets the number of requested workers that have not yet been granted.
	 *
	 * @return The number pending worker requests.
	 */
	protected abstract int getNumWorkerRequestsPending();

	/**
	 * Gets the number of containers that have been started, but where the TaskManager
	 * has not yet registered at the job manager.
	 *
	 * @return The number of started containers pending TaskManager registration.
	 */
	protected abstract int getNumWorkersPendingRegistration();

	// ------------------------------------------------------------------------
	//  Info messaging
	// ------------------------------------------------------------------------

	protected void sendInfoMessage(String message) {
		if (infoMessageListener != null) {
			infoMessageListener.tell(decorateMessage(new InfoMessage(message)), self());
		}
	}

	protected void registerMessageListener(ActorRef listener) {
		infoMessageListener = listener;
	}


	// ------------------------------------------------------------------------
	//  Startup
	// ------------------------------------------------------------------------

	public static void startResourceManager() {

	}

	// TODO RM
//	/**
//	 * Starts and runs the TaskManager.
//	 *
//	 * This method first tries to select the network interface to use for the TaskManager
//	 * communication. The network interface is used both for the actor communication
//	 * (coordination) as well as for the data exchange between task managers. Unless
//	 * the hostname/interface is explicitly configured in the configuration, this
//	 * method will try out various interfaces and methods to connect to the JobManager
//	 * and select the one where the connection attempt is successful.
//	 *
//	 * After selecting the network interface, this method brings up an actor system
//	 * for the TaskManager and its actors, starts the TaskManager's services
//	 * (library cache, shuffle network stack, ...), and starts the TaskManager itself.
//	 *
//	 * @param configuration The configuration for the TaskManager.
//	 * @param taskManagerClass The actor class to instantiate.
//	 *                         Allows to use TaskManager subclasses for example for YARN.
//	 */
//	@throws(classOf[Exception])
//	def selectNetworkInterfaceAndRunTaskManager(
//		configuration: Configuration,
//		resourceID: ResourceID,
//		taskManagerClass: Class[_ <:TaskManager])
//	: Unit = {
//
//		val (taskManagerHostname, actorSystemPort) = selectNetworkInterfaceAndPort(configuration)
//
//		runTaskManager(
//			taskManagerHostname,
//			resourceID,
//			actorSystemPort,
//			configuration,
//			taskManagerClass)
//	}
//
//	@throws(classOf[IOException])
//		@throws(classOf[IllegalConfigurationException])
//	def selectNetworkInterfaceAndPort(
//		configuration: Configuration)
//	: (String, Int) = {
//
//		var taskManagerHostname = configuration.getString(
//			ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, null)
//
//		if (taskManagerHostname != null) {
//			LOG.info("Using configured hostname/address for TaskManager: " + taskManagerHostname)
//		}
//		else {
//			val leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(configuration)
//			val lookupTimeout = AkkaUtils.getLookupTimeout(configuration)
//
//			val taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(
//				leaderRetrievalService,
//				lookupTimeout)
//
//			taskManagerHostname = taskManagerAddress.getHostName()
//			LOG.info(s"TaskManager will use hostname/address '$taskManagerHostname' " +
//				s"(${taskManagerAddress.getHostAddress()}) for communication.")
//		}
//
//		// if no task manager port has been configured, use 0 (system will pick any free port)
//		val actorSystemPort = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0)
//		if (actorSystemPort < 0 || actorSystemPort > 65535) {
//			throw new IllegalConfigurationException("Invalid value for '" +
//				ConfigConstants.TASK_MANAGER_IPC_PORT_KEY +
//				"' (port for the TaskManager actor system) : " + actorSystemPort +
//				" - Leave config parameter empty or use 0 to let the system choose a port automatically.")
//		}
//
//		(taskManagerHostname, actorSystemPort)
//	}
//
//	/**
//	 * Starts and runs the TaskManager. Brings up an actor system for the TaskManager and its
//	 * actors, starts the TaskManager's services (library cache, shuffle network stack, ...),
//	 * and starts the TaskManager itself.
//	 *
//	 * This method will also spawn a process reaper for the TaskManager (kill the process if
//	 * the actor fails) and optionally start the JVM memory logging thread.
//	 *
//	 * @param taskManagerHostname The hostname/address of the interface where the actor system
//	 *                         will communicate.
//	 * @param resourceID The id of the resource which the task manager will run on.
//	 * @param actorSystemPort The port at which the actor system will communicate.
//	 * @param configuration The configuration for the TaskManager.
//	 */
//	@throws(classOf[Exception])
//	def runTaskManager(
//		taskManagerHostname: String,
//		resourceID: ResourceID,
//		actorSystemPort: Int,
//		configuration: Configuration)
//	: Unit = {
//
//		runTaskManager(
//			taskManagerHostname,
//			resourceID,
//			actorSystemPort,
//			configuration,
//			classOf[TaskManager])
//	}


	/**
	 * Starts the resource manager actors.
	 * @param configuration The configuration for the resource manager
	 * @param actorSystem The actor system to start the resource manager in
	 * @param leaderRetriever The leader retriever service to intialize the resource manager
	 * @param resourceManagerClass The class of the ResourceManager to be started
	 * @return ActorRef of the resource manager
	 */
	public static ActorRef startResourceManagerActors(
		Configuration configuration,
		ActorSystem actorSystem,
		LeaderRetrievalService leaderRetriever,
		Class<? extends FlinkResourceManager<?>> resourceManagerClass
	) {
		return startResourceManagerActors(
			configuration, actorSystem, leaderRetriever, resourceManagerClass,
			RESOURCE_MANAGER_NAME + "-" + UUID.randomUUID());
	}

	/**
	 * Starts the resource manager actors.
	 * @param configuration The configuration for the resource manager
	 * @param actorSystem The actor system to start the resource manager in
	 * @param leaderRetriever The leader retriever service to intialize the resource manager
	 * @param resourceManagerClass The class of the ResourceManager to be started
	 * @param resourceManagerActorName The name of the resource manager actor.
	 * @return ActorRef of the resource manager
	 */
	public static ActorRef startResourceManagerActors(
		Configuration configuration,
		ActorSystem actorSystem,
		LeaderRetrievalService leaderRetriever,
		Class<? extends FlinkResourceManager<?>> resourceManagerClass,
		String resourceManagerActorName
	) {

		Props resourceMasterProps = Props.create(resourceManagerClass, configuration, leaderRetriever);

		return actorSystem.actorOf(resourceMasterProps, resourceManagerActorName);
	}
}
