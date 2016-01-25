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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.FlinkUntypedActor;
import org.apache.flink.runtime.clusterframework.messages.CheckAndAllocateContainers;
import org.apache.flink.runtime.clusterframework.messages.FatalErrorOccurred;
import org.apache.flink.runtime.clusterframework.messages.GetClusterStatusResponse;
import org.apache.flink.runtime.clusterframework.messages.InfoMessage;
import org.apache.flink.runtime.clusterframework.messages.NewLeaderAvailable;
import org.apache.flink.runtime.clusterframework.messages.RegisterInfoMessageListener;
import org.apache.flink.runtime.clusterframework.messages.RegisterResourceManager;
import org.apache.flink.runtime.clusterframework.messages.RegistrationAtJobManagerSuccessful;
import org.apache.flink.runtime.clusterframework.messages.ReleaseTaskManager;
import org.apache.flink.runtime.clusterframework.messages.SetWorkerPoolSize;
import org.apache.flink.runtime.clusterframework.messages.ShutdownTaskManager;
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.clusterframework.messages.TaskManagerAvailable;
import org.apache.flink.runtime.clusterframework.messages.TaskManagerRemoved;
import org.apache.flink.runtime.clusterframework.messages.TriggerRegistrationAtJobManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages.LeaderSessionMessage;
import org.apache.flink.runtime.messages.Messages;
import org.apache.flink.runtime.messages.RegistrationMessages.RegisterTaskManager;
import org.apache.flink.runtime.messages.RegistrationMessages.AcknowledgeRegistration;
import org.apache.flink.runtime.messages.RegistrationMessages.AlreadyRegistered;
import org.apache.flink.runtime.messages.RegistrationMessages.RefuseRegistration;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 *
 *  
 *  
 * <h1>Worker allocation steps</h1>
 *
 * <ol>
 *     <li>The resource manager decides to request more workers. This can happen in order
 *         to fill the initial pool, or as a result of the JobManager requesting more workers.</li>
 *         
 *     <li>The resource master calls {@link #requestNewWorkers(int)}, which triggers requests
 *         for more containers/workers. After that, the {@link #getNumWorkerRequestsPending()}
 *         should reflect the pending requests.</li>
 *         
 *     <li>The concrete framework may acquire containers and then trigger to start TaskManagers
 *         in those containers. That should be reflected in {@link #getNumWorkersPendingRegistration()}.</li>
 *         
 *     <li>At some point, the TaskManager processes will have started and send a registration
 *         message. The method {@link #workerRegistered(RegisterTaskManager)} will be called
 *         to create a tracking worker object for the registered worker.</li>
 *         
 *     <li>The JobManager will be notified that a new worker is available.</li>
 * </ol>
 */
public abstract class FlinkResourceManager<WorkerType extends RegisteredTaskManager> extends FlinkUntypedActor {

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

	/** The currently registered workers, keyed by the container/worker id */
	private final Map<ResourceID, WorkerType> registeredWorkers;

	/** List of listeners for info messages */
	private ActorRef infoMessageListener;

	/** The JobManager that the framework master manages resources for */
	private ActorRef jobManager;

	/** Our JobManager's leader session */
	private UUID leaderSessionID;

	/** The port of the JobManager's blob server, to announce to the TaskManagers */
	private int jobManagerBlobServerPort;

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
			else if (message instanceof ReleaseTaskManager) {
				ReleaseTaskManager msg = (ReleaseTaskManager) message;
				releaseTaskManager(msg.resourceId(), msg.registrationId());
			}

			// --- registration from TaskManagers

			else if (message instanceof RegisterTaskManager) {
				handleTaskManagerRegistration((RegisterTaskManager) message);
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
			else if (message instanceof RegistrationAtJobManagerSuccessful) {
				RegistrationAtJobManagerSuccessful msg = (RegistrationAtJobManagerSuccessful) message;
				jobManagerLeaderConnected(msg.jobManager(), msg.blobServerPort(),
					msg.currentlyRegisteredTaskManagers());
			}

			// --- end of application
			
			else if (message instanceof StopCluster) {
				StopCluster msg = (StopCluster) message;
				shutdownCluster(msg.finalStatus(), msg.message());
			}

			// --- miscellaneous messages
			
			else if (message instanceof RegisterInfoMessageListener) {
				registerMessageListener(sender());
				sender().tell(decorateMessage(Acknowledge.get()), self());
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
	 * Gets the registered worker for a given resource ID, if one is available.
	 * 
	 * @param resourceId The resource ID for the worker. 
	 * @return The worker for that resource ID, or null, if none is registered under that ID.
	 */
	public WorkerType getRegisteredTaskManager(ResourceID resourceId) {
		return registeredWorkers.get(resourceId);
	}

	/**
	 * Gets an iterable for all currently registered TaskManagers.
	 * 
	 * @return All currently registered TaskManagers.
	 */
	public Collection<WorkerType> allRegisteredTaskManagers() {
		return registeredWorkers.values();
	}
	
	// ------------------------------------------------------------------------
	//  Registration and Failure of TaskManagers
	// ------------------------------------------------------------------------
	
	private void handleTaskManagerRegistration(RegisterTaskManager registerMessage) {
		// handle this only if we are associated with the current leader.
		// else the TaskManager will keep retrying 
		if (jobManager != null) {
			// we are currently associated with a JobManager, so we can handle this
			WorkerType current = registeredWorkers.get(registerMessage.resourceId());
			
			if (current != null) {
				// we already have a worker with that resource ID
				sender().tell(decorateMessage(
						new AlreadyRegistered(
							current.registeredTaskManagerId(), jobManagerBlobServerPort)),
					self());
			}
			else {
				try {
					log.info("TaskManager {} / {} trying to register", 
						registerMessage.resourceId(), registerMessage.connectionInfo());
					
					WorkerType registeredTm = workerRegistered(registerMessage);
					if (registeredTm != null) {
						// add this to our bookkeeping
						registeredWorkers.put(registeredTm.resourceId(), registeredTm);
	
						// let the JobManager know about the resource
						jobManager.tell(decorateMessage(new TaskManagerAvailable(
								registeredTm.resourceId(), registeredTm.registeredTaskManagerId(),
								sender(), registerMessage.connectionInfo(), registerMessage.resources(),
								registerMessage.numberOfSlots())),
							self());
						
						// let the TaskManager know that registration was successful
						sender().tell(decorateMessage(
							new AcknowledgeRegistration(
								registeredTm.registeredTaskManagerId(), jobManagerBlobServerPort)),
							self());

						LOG.info("Accepted TaskManager {} / {}. Current number of registered TaskManagers: {}",
							registeredTm.resourceId(), registerMessage.connectionInfo(), registeredWorkers.size());
					}
				}
				catch (Exception e) {
					LOG.error("TaskManager registration failed", e);
					
					// tell the TaskManager about the failure
					String eStr = ExceptionUtils.stringifyException(e);
					sender().tell(decorateMessage(
						new RefuseRegistration("Registration failed: " + eStr)),
						self());
				}
			}
		}
	}

	/**
	 * Releases the given TaskManager. Note that this does not automatically shrink
	 * the designated worker pool size.
	 *
	 * @param resourceId The TaskManager's resource id.
	 * @param registrationId The TaskManager's registration id.
	 */
	private void releaseTaskManager(ResourceID resourceId, InstanceID registrationId) {
		try {
			WorkerType worker = registeredWorkers.remove(resourceId);
			if (worker != null) {
				if (worker.registeredTaskManagerId().equals(registrationId)) {
					// tell the TaskManager process to cleanly quit
					worker.taskManagerActor().tell(
						new ShutdownTaskManager("TaskManager was released as a resource."),
						self());
					
					// make sure the resource framework deallocates the resource 
					releaseRegisteredWorker(worker);
				}
				else {
					// we are in an inconsistent state. not sure how this happened, but we
					// definitely need to fix it from here
					LOG.error("Inconsistent registration of TaskManager resource {}. " +
						"Forcing TaskManager to re-register.", resourceId);
					forceTaskManagerToReconnect(worker, "Inconsistent registration of TaskManager");
				}
			}
			else {
				// may happen if the container failed and the JobManager requested a
				// release at the same time
				LOG.debug("Received request to release unknown TaskManager with resource id {}",
					resourceId);
			}
		}
		finally {
			// in any case, we acknowledge the receipt of that message, because we handled it
			sender().tell(Acknowledge.get(), ActorRef.noSender());
		}
	}

	private void forceTaskManagerToReconnect(WorkerType worker, String reason) {
		// out of our bookkeeping
		registeredWorkers.remove(worker.resourceId());

		// framework should know the worker is pending re-registration
		workerUnRegistered(worker);

		// make the TaskManager go back to registration attempts
		worker.taskManagerActor().tell(
			decorateMessage(new Messages.Disconnect(reason)), ActorRef.noSender());
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
						((LeaderSessionMessage) msg).message() instanceof RegistrationAtJobManagerSuccessful)
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
			jobManagerBlobServerPort = -1;
			infoMessageListener = null;
			
			// we move all currently registered TaskManagers to the "registration pending" status
			LOG.info("Setting {} currently registered TaskManagers to await their re-registration " +
				"with the new leader JobManager", registeredWorkers.size());
			
			for (WorkerType worker : registeredWorkers.values()) {
				workerUnRegistered(worker);
			}
			registeredWorkers.clear();
		}
	}

	/**
	 * // TODO
	 * @param newJobManagerLeader
	 * @param newJobManagerBlobServerPort
	 * @param taskManagerInfos
	 */
	private void jobManagerLeaderConnected(
						ActorRef newJobManagerLeader, int newJobManagerBlobServerPort,
						Collection<TaskManagerInfo> taskManagerInfos) {
		
		if (jobManager == null) {
			LOG.info("Resource Manager associating with leading JobManager {} - leader session {}",
						newJobManagerLeader, leaderSessionID);
			
			jobManager = newJobManagerLeader;
			jobManagerBlobServerPort = newJobManagerBlobServerPort;

			if (taskManagerInfos.size() > 0) {
				LOG.info("Received TaskManagers that were registered at the leader JobManager. " +
						"Trying to consolidate.");
				
				// keep track of which TaskManagers are not handled
				Map<ResourceID, TaskManagerInfo> toHandle = new HashMap<>();
				for (TaskManagerInfo tmInfo : taskManagerInfos) {
					toHandle.put(tmInfo.resourceId(), tmInfo);
				}
				
				try {
					// ask the framework to tell us which ones we should keep for now
					List<WorkerType> consolidated = reacceptRegisteredTaskManagers(toHandle.values());
					log.info("Consolidated {} TaskManagers", consolidated.size());
					
					// put the consolidated TaskManagers into our bookkeeping
					for (WorkerType tm : consolidated) {
						registeredWorkers.put(tm.resourceId(), tm);
						toHandle.remove(tm.resourceId());
						// TaskManager is already registered with Resource Manager. Otherwise, the
						// JobManager wouldn't know about the particular TaskManager.
						// We might have to update the resource manager address, in case it crashed
						// and was restarted on a different node.
						tm.taskManagerActor().tell(decorateMessage(new AcknowledgeRegistration(
							tm.registeredTaskManagerId(), newJobManagerBlobServerPort
						)), self());
					}
				}
				catch (Throwable t) {
					LOG.error("Error during consolidation of known TaskManagers", t);
					// the framework should release the remaining unclear resources
					for (ResourceID id : toHandle.keySet()) {
						releasePendingWorker(id);
					}
				}

				if (toHandle.isEmpty()) {
					log.info("Consolidated all TaskManager registrations");;
				}
				else {
					log.info("Failed to consolidate {} TaskManagers", toHandle.size());
					
					// whatever is left to handle (because it was not known, or because of an exception)
					// will be sent to the JobManager as failed
					String msg = "Could not consolidate TaskManager after Resource Manager failure/re-association";
					
					for (TaskManagerInfo failed : toHandle.values()) {
						jobManager.tell(decorateMessage(new TaskManagerRemoved(
								failed.resourceId(), failed.registeredTaskManagerId(), msg)),
							self());
					}
				}
			}
		}
		else {
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

		// the shutdown consists of the following steps:
		// 1) notify all workers to shut down (messages to task managers)
		// 2) await the replies

		// send the messages to the workers. note that this means only currently registered
		// TaskManagers get this shutdown call to cleanly exit. All not registered TaskManagers
		// need to be killed by the framework as part of releasing the container/resource.

		ShutdownTaskManager shutdownMessage = new ShutdownTaskManager(
			"The cluster is being shutdown with application status " + status);

		Timeout timeout = new Timeout(messageTimeout);
		Deadline deadline = messageTimeout.fromNow();

		List<Future<?>> futures = new ArrayList<>(registeredWorkers.size());
		for (WorkerType tm : registeredWorkers.values()) {
			futures.add(
				Patterns.ask(tm.taskManagerActor(), decorateMessage(shutdownMessage),
				timeout));
		}

		// now we wait on the futures until all nodes responded, or the
		// futures time out. That gives the nodes a chance to shut down cleanly
		try {
			for (Future<?> f : futures) {
				Await.ready(f, deadline.timeLeft());
			}
		} catch (InterruptedException ignored) {
			// we make only a best effort to cleanly shut down. if we are interrupted,
			// the framework logic must act as a failsafe to eventually deallocate the container
		} catch (TimeoutException e) {
			log.error("Failed to shutdown all workers.");
		}

		// shut the resource master down.
		shutdownApplication(status, diagnostics);

		// TODO respond to JobManager?

		// we shut down the whole process now.
		int exitCode = status.processExitCode();
		System.exit(exitCode);
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
		self().tell(decorateMessage(CheckAndAllocateContainers.get()), ActorRef.noSender());
	}

	/**
	 * This method should be called by the framework once it detects that a currently registered
	 * worker has failed.
	 * 
	 * @param worker The worker that has failed.
	 * @param message An informational message that explains why the worker failed.
	 */
	public void notifyWorkerFailed(WorkerType worker, String message) {
		registeredWorkers.remove(worker.resourceId());
		
		jobManager.tell(decorateMessage(new TaskManagerRemoved(
				worker.resourceId(), worker.registeredTaskManagerId(), message)),
			self());
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

	
	protected abstract void releasePendingWorker(ResourceID id);

	protected abstract void releaseRegisteredWorker(WorkerType worker);
	
	
	protected abstract WorkerType workerRegistered(RegisterTaskManager registerMessage) throws Exception;

	protected abstract void workerUnRegistered(WorkerType worker);

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
	 * via calling {@link #notifyWorkerFailed(RegisteredTaskManager, String)}.
	 * 
	 * @param registered The list of TaskManagers that the JobManager knows.
	 * @return The subset of TaskManagers that the resource manager can confirm to be alive.
	 */
	protected abstract List<WorkerType> reacceptRegisteredTaskManagers(Collection<TaskManagerInfo> registered);
	
	/**
	 * Gets the number of requested workers that have not yet been granted.
	 *
	 * @return The number pending worker requests.
	 */
	protected abstract int getNumWorkerRequestsPending();

	/**
	 * Gets the number of containers that have been started, but where the TaskManager
	 * has not yet registered at the resource manager.
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
	
	
	private GetClusterStatusResponse queryClusterStatus() {
		int numSlots = 0;
		for (WorkerType worker : allRegisteredTaskManagers()) {
			numSlots += worker.numSlots();
		}
		
		return new GetClusterStatusResponse(registeredWorkers.size(), numSlots);
	}

	// ------------------------------------------------------------------------
	//  Startup
	// ------------------------------------------------------------------------

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
		return startResourceManagerActors(configuration, actorSystem,
				leaderRetriever, resourceManagerClass, null);
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

		if (resourceManagerActorName != null) {
			return actorSystem.actorOf(resourceMasterProps, resourceManagerActorName);
		} else {
			return actorSystem.actorOf(resourceMasterProps);
		}
	}
}
