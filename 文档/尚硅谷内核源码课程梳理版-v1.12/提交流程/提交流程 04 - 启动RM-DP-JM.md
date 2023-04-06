# 提交流程 04 - 启动RM-DP-JM

[TOC]

**版本:1.12**

-----------------------------------------------

在 `提交流程03-启动AM.md` 文件中，说明了启动 am 的整体流程。本文档则描述启动 am 的具体实现过程。

先梳理 am、jobmanager、ResourceManager、Dispatcher、JobMaster 的关系:

	am里的整体进程是jobmanager，而jobmanager包含三个组件ResourceManager、Dispatcher、JobMaster

	在 Dispatcher 中启动 JobMaster

	------------------------------
	| am (jobmanager)            |
	|   ---------------------    |
	|   |  ResourceManager  |    |
	|   |  Dispatcher       |    |
	|   |  JobMaster        |    |
	|   ---------------------    |
	|                            |
	------------------------------

在 `startAppMaster()` 中，创建了 amContainer, 其中包含启动 am 的命令，当启动这个 amContainer 时，就会执行这条命令，进而执行 
`YarnJobClusterEntrypoint` 类的 main 方法，在这个 main 方法中创建和启动 ResourceManager、Dispatcher、JobMaster.


```java
public class YarnJobClusterEntrypoint extends JobClusterEntrypoint {
	// ------------------------------------------------------------------------
	//  The executable entry point for the Yarn Application Master Process
	//  for a single Flink job.
	//  yarn am 进程的可执行入口，在该 main 方法中会创建和启动 JobManager 里的组件（ResourceManager、Dispatcher、JobMaster）
	// ------------------------------------------------------------------------

	public static void main(String[] args) {
		...
		
		ClusterEntrypoint.runClusterEntrypoint(yarnJobClusterEntrypoint);
	}
}
```

```java
public abstract class ClusterEntrypoint implements AutoCloseableAsync, FatalErrorHandler {

	public static void runClusterEntrypoint(ClusterEntrypoint clusterEntrypoint) {
		// 入口类名
		final String clusterEntrypointName = clusterEntrypoint.getClass().getSimpleName();
		try {
			clusterEntrypoint.startCluster();
		} ...
	}
}
```

```java
	public void startCluster() throws ClusterEntrypointException {
		LOG.info("Starting {}.", getClass().getSimpleName());

		try {
			replaceGracefulExitWithHaltIfConfigured(configuration);
			PluginManager pluginManager = PluginUtils.createPluginManagerFromRootFolder(configuration);
			configureFileSystems(configuration, pluginManager);

			SecurityContext securityContext = installSecurityContext(configuration);

			securityContext.runSecured((Callable<Void>) () -> {
				runCluster(configuration, pluginManager);

				return null;
			});
		} ...
	}
```

```java
	private void runCluster(Configuration configuration, PluginManager pluginManager) throws Exception {
		synchronized (lock) {

			// 初始化服务：Rpc相关
			initializeServices(configuration, pluginManager);

			// write host information into configuration
			configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
			configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

			final DispatcherResourceManagerComponentFactory dispatcherResourceManagerComponentFactory = createDispatcherResourceManagerComponentFactory(configuration);

			// 创建和启动 JobManager里的组件：Dispatcher、ResourceManager、JobMaster
			clusterComponent = dispatcherResourceManagerComponentFactory.create(
				configuration,
				ioExecutor,
				commonRpcService,
				haServices,
				blobServer,
				heartbeatServices,
				metricRegistry,
				archivedExecutionGraphStore,
				new RpcMetricQueryServiceRetriever(metricRegistry.getMetricQueryServiceRpcService()),
				this);

			...
		}
	}
```

## 1 dispatcherResourceManagerComponentFactory.create()

DispatcherResourceManagerComponentFactory 是一个接口，查看其实现类的 create 方法

```java
public class DefaultDispatcherResourceManagerComponentFactory implements DispatcherResourceManagerComponentFactory {

	public DispatcherResourceManagerComponent create(
			Configuration configuration,
			Executor ioExecutor,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			MetricRegistry metricRegistry,
			ArchivedExecutionGraphStore archivedExecutionGraphStore,
			MetricQueryServiceRetriever metricQueryServiceRetriever,
			FatalErrorHandler fatalErrorHandler) throws Exception {

		LeaderRetrievalService dispatcherLeaderRetrievalService = null;
		LeaderRetrievalService resourceManagerRetrievalService = null;
		WebMonitorEndpoint<?> webMonitorEndpoint = null;
		ResourceManager<?> resourceManager = null;
		DispatcherRunner dispatcherRunner = null;

		try {
			dispatcherLeaderRetrievalService = highAvailabilityServices.getDispatcherLeaderRetriever();

			resourceManagerRetrievalService = highAvailabilityServices.getResourceManagerLeaderRetriever();

			final LeaderGatewayRetriever<DispatcherGateway> dispatcherGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				DispatcherGateway.class,
				DispatcherId::fromUuid,
				new ExponentialBackoffRetryStrategy(12, Duration.ofMillis(10), Duration.ofMillis(50)));

			final LeaderGatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever = new RpcGatewayRetriever<>(
				rpcService,
				ResourceManagerGateway.class,
				ResourceManagerId::fromUuid,
				new ExponentialBackoffRetryStrategy(12, Duration.ofMillis(10), Duration.ofMillis(50)));

			final ScheduledExecutorService executor = WebMonitorEndpoint.createExecutorService(
				configuration.getInteger(RestOptions.SERVER_NUM_THREADS),
				configuration.getInteger(RestOptions.SERVER_THREAD_PRIORITY),
				"DispatcherRestEndpoint");

			final long updateInterval = configuration.getLong(MetricOptions.METRIC_FETCHER_UPDATE_INTERVAL);
			final MetricFetcher metricFetcher = updateInterval == 0
				? VoidMetricFetcher.INSTANCE
				: MetricFetcherImpl.fromConfiguration(
					configuration,
					metricQueryServiceRetriever,
					dispatcherGatewayRetriever,
					executor);

			webMonitorEndpoint = restEndpointFactory.createRestEndpoint(
				configuration,
				dispatcherGatewayRetriever,
				resourceManagerGatewayRetriever,
				blobServer,
				executor,
				metricFetcher,
				highAvailabilityServices.getClusterRestEndpointLeaderElectionService(),
				fatalErrorHandler);

			log.debug("Starting Dispatcher REST endpoint.");
			webMonitorEndpoint.start();

			final String hostname = RpcUtils.getHostname(rpcService);

			// (1.1节)创建 ResourceManager：Yarn模式的 ResourceManager
			resourceManager = resourceManagerFactory.createResourceManager(
				configuration,
				ResourceID.generate(),
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				fatalErrorHandler,
				new ClusterInformation(hostname, blobServer.getPort()),
				webMonitorEndpoint.getRestBaseUrl(),
				metricRegistry,
				hostname,
				ioExecutor);

			final HistoryServerArchivist historyServerArchivist = HistoryServerArchivist.createHistoryServerArchivist(configuration, webMonitorEndpoint, ioExecutor);

			final PartialDispatcherServices partialDispatcherServices = new PartialDispatcherServices(
				configuration,
				highAvailabilityServices,
				resourceManagerGatewayRetriever,
				blobServer,
				heartbeatServices,
				() -> MetricUtils.instantiateJobManagerMetricGroup(metricRegistry, hostname),
				archivedExecutionGraphStore,
				fatalErrorHandler,
				historyServerArchivist,
				metricRegistry.getMetricQueryServiceGatewayRpcAddress(),
				ioExecutor);

			log.debug("Starting Dispatcher.");
			// (1.2节)创建和启动 Dispatcher => dispatcher会创建和启动JobMaster(1.3节)
			dispatcherRunner = dispatcherRunnerFactory.createDispatcherRunner(
				highAvailabilityServices.getDispatcherLeaderElectionService(),
				fatalErrorHandler,
				new HaServicesJobGraphStoreFactory(highAvailabilityServices),
				ioExecutor,
				rpcService,
				partialDispatcherServices);

			log.debug("Starting ResourceManager.");
			// (1.4节)启动 ResourceManager
			resourceManager.start();

			resourceManagerRetrievalService.start(resourceManagerGatewayRetriever);
			dispatcherLeaderRetrievalService.start(dispatcherGatewayRetriever);

// Component which starts a {@link Dispatcher}, {@link ResourceManager} and {@link WebMonitorEndpoint} in the same process.
			return new DispatcherResourceManagerComponent(
				dispatcherRunner,
				DefaultResourceManagerService.createFor(resourceManager),
				dispatcherLeaderRetrievalService,
				resourceManagerRetrievalService,
				webMonitorEndpoint,
				fatalErrorHandler);

		}...
	}
}
```

### 1.1 创建ResourceManager: resourceManagerFactory.createResourceManager()

通过层层调用，到达 ActiveResourceManagerFactory 抽象类下的 createResourceManager 方法

```java
	public ResourceManager<WorkerType> createResourceManager(
			Configuration configuration,
			ResourceID resourceId,
			RpcService rpcService,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			FatalErrorHandler fatalErrorHandler,
			ClusterInformation clusterInformation,
			@Nullable String webInterfaceUrl,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			ResourceManagerRuntimeServices resourceManagerRuntimeServices,
			Executor ioExecutor) throws Exception {

		return new ActiveResourceManager<>(
				createResourceManagerDriver(configuration, webInterfaceUrl, rpcService.getAddress()),
				configuration,
				rpcService,
				resourceId,
				highAvailabilityServices,
				heartbeatServices,
				resourceManagerRuntimeServices.getSlotManager(),
				ResourceManagerPartitionTrackerImpl::new,
				resourceManagerRuntimeServices.getJobLeaderIdService(),
				clusterInformation,
				fatalErrorHandler,
				resourceManagerMetricGroup,
				ioExecutor);
	}
```

通过层层调用，到达 ResourceManager 类的构造方法，从而创建了 ResourceManager.

```java
/**
 * ResourceManager implementation. The resource manager is responsible for resource de-/allocation
 * and bookkeeping.
 *
 * <p>It offers the following methods as part of its rpc interface to interact with him remotely:
 * <ul>
 *     <li>{@link #registerJobManager(JobMasterId, ResourceID, String, JobID, Time)} registers a {@link JobMaster} at the resource manager</li>
 *     <li>{@link #requestSlot(JobMasterId, SlotRequest, Time)} requests a slot from the resource manager</li>
 * </ul>
 */
public abstract class ResourceManager<WorkerType extends ResourceIDRetrievable>
		extends FencedRpcEndpoint<ResourceManagerId>
		implements ResourceManagerGateway, LeaderContender {

	public ResourceManager(
			RpcService rpcService,
			ResourceID resourceId,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			Time rpcTimeout,
			Executor ioExecutor) {

		super(rpcService, AkkaRpcServiceUtils.createRandomName(RESOURCE_MANAGER_NAME), null);

		this.resourceId = checkNotNull(resourceId);
		this.highAvailabilityServices = checkNotNull(highAvailabilityServices);
		this.heartbeatServices = checkNotNull(heartbeatServices);
		this.slotManager = checkNotNull(slotManager);
		this.jobLeaderIdService = checkNotNull(jobLeaderIdService);
		this.clusterInformation = checkNotNull(clusterInformation);
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
		this.resourceManagerMetricGroup = checkNotNull(resourceManagerMetricGroup);

		this.jobManagerRegistrations = new HashMap<>(4);
		this.jmResourceIdRegistrations = new HashMap<>(4);
		this.taskExecutors = new HashMap<>(8);
		this.taskExecutorGatewayFutures = new HashMap<>(8);

		this.jobManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();
		this.taskManagerHeartbeatManager = NoOpHeartbeatManager.getInstance();

		this.clusterPartitionTracker = checkNotNull(clusterPartitionTrackerFactory).get(
			(taskExecutorResourceId, dataSetIds) -> taskExecutors.get(taskExecutorResourceId).getTaskExecutorGateway()
				.releaseClusterPartitions(dataSetIds, rpcTimeout)
				.exceptionally(throwable -> {
					log.debug("Request for release of cluster partitions belonging to data sets {} was not successful.", dataSetIds, throwable);
					throw new CompletionException(throwable);
				})
		);
		this.ioExecutor = ioExecutor;
	}

}
```

### 1.2 创建并启动Dispatcher: dispatcherRunnerFactory.createDispatcherRunner()

该方法通过层层调用，进入到 `DefaultDispatcherGatewayServiceFactory` 类的 `create` 方法

```java
class DefaultDispatcherGatewayServiceFactory implements AbstractDispatcherLeaderProcess.DispatcherGatewayServiceFactory {
	
	public AbstractDispatcherLeaderProcess.DispatcherGatewayService create(
			DispatcherId fencingToken,
			Collection<JobGraph> recoveredJobs,
			JobGraphWriter jobGraphWriter) {

		final Dispatcher dispatcher;
		try {
			// 创建Dispatcher, 在 createDispatcher 方法中最终 new 一个 dispatcher 对象
			dispatcher = dispatcherFactory.createDispatcher(
				rpcService,
				fencingToken,
				recoveredJobs,
				(dispatcherGateway, scheduledExecutor, errorHandler) -> new NoOpDispatcherBootstrap(),
				PartialDispatcherServicesWithJobGraphStore.from(partialDispatcherServices, jobGraphWriter));
		} catch (Exception e) {
			throw new FlinkRuntimeException("Could not create the Dispatcher rpc endpoint.", e);
		}

		//启动 Dispatcher
		dispatcher.start();

		return DefaultDispatcherGatewayService.from(dispatcher);
	}
}
```

Dispatcher 类继承至 RpcEndpoint 类，这里的 start 方法是调用的 RpcEndpoint 类的 start 方法。

所以这里触发了启动一个 rpc 终端，这就告诉底层 rpc 服务: rpc 终端已准备好处理远程过程调用。 

```java
public abstract class RpcEndpoint implements RpcGateway, AutoCloseableAsync {
	/**
	 * Triggers start of the rpc endpoint. This tells the underlying rpc server that the rpc endpoint is ready
	 * to process remote procedure calls.
	 */
	public final void start() {
		// 终端的启动，实际上是由 自身网关（RpcServer）来启动的
		rpcServer.start();
	}
}
```

### 1.3 创建并启动JobMaster

`Dispatcher` 用来提交 Flink 应用程序执行，并为每个提交的作业启动一个新的 `JobMaster`。

所以，接下来查看 `Dispatcher` 类下的 `onStart` 方法。

这个 `onStart` 方法是其父类 `RpcEndpoint` 的方法，它在启动 `RpcEndpoint` 时被调用，也就是执行 `dispatcher.start();` 时调用。

```java
	/**
	 * User overridable callback which is called from {@link #internalCallOnStart()}.
	 *
	 * <p>This method is called when the RpcEndpoint is being started. The method is guaranteed
	 * to be executed in the main thread context and can be used to start the rpc endpoint in the
	 * context of the rpc endpoint's main thread.
	 *
	 * <p>IMPORTANT: This method should never be called directly by the user.
	 *
	 * @throws Exception indicating that the rpc endpoint could not be started. If an exception occurs,
	 * then the rpc endpoint will automatically terminate.
	 */
	protected void onStart() throws Exception {}
```

`Dispatcher` 类下的 `onStart` 方法:

```java
	//------------------------------------------------------
	// Lifecycle methods
	//------------------------------------------------------
	public void onStart() throws Exception {
		try {
			// 启动 dispatcher 服务
			startDispatcherServices();
		} ...

		// 启动JobMaster
		startRecoveredJobs();
		this.dispatcherBootstrap = this.dispatcherBootstrapFactory.create(
				getSelfGateway(DispatcherGateway.class),
				this.getRpcService().getScheduledExecutor() ,
				this::onFatalError);
	}
```

该方法通过层层调用，进入到 `Dispatcher` 类的 `createJobManagerRunner` 方法

```java
	CompletableFuture<JobManagerRunner> createJobManagerRunner(JobGraph jobGraph, long initializationTimestamp) {
		final RpcService rpcService = getRpcService();
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					// (1.3.1节)创建JobMaster
					// JobManagerRunner: Interface for a runner which executes a {@link JobMaster}.
					JobManagerRunner runner = jobManagerRunnerFactory.createJobManagerRunner(
						jobGraph,
						configuration,
						rpcService,
						highAvailabilityServices,
						heartbeatServices,
						jobManagerSharedServices,
						new DefaultJobManagerJobMetricGroupFactory(jobManagerMetricGroup),
						fatalErrorHandler,
						initializationTimestamp);
					// (1.3.2节)启动JobMaster
					runner.start();
					return runner;
				} catch (Exception e) {
					throw new CompletionException(new JobInitializationException(jobGraph.getJobID(), "Could not instantiate JobManager.", e));
				}
			},
			ioExecutor); // do not use main thread executor. Otherwise, Dispatcher is blocked on JobManager creation
	}
```

#### 1.3.1 jobManagerRunnerFactory.createJobManagerRunner()

```java
public interface JobManagerRunnerFactory {

	JobManagerRunner createJobManagerRunner(
		JobGraph jobGraph,
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		JobManagerSharedServices jobManagerServices,
		JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
		FatalErrorHandler fatalErrorHandler,
		long initializationTimestamp) throws Exception;
}
```

通过层层调用，进入到 `DefaultJobMasterServiceFactory` 类的 `createJobMasterService` 方法，在该方法中 new 了一个 JobMaster 对象。

```java
public class DefaultJobMasterServiceFactory implements JobMasterServiceFactory {
	public JobMaster createJobMasterService(
			JobGraph jobGraph,
			OnCompletionActions jobCompletionActions,
			ClassLoader userCodeClassloader,
			long initializationTimestamp) throws Exception {

		return new JobMaster(
			rpcService,
			jobMasterConfiguration,
			ResourceID.generate(),
			jobGraph,
			haServices,
			slotPoolFactory,
			jobManagerSharedServices,
			heartbeatServices,
			jobManagerJobMetricGroupFactory,
			jobCompletionActions,
			fatalErrorHandler,
			userCodeClassloader,
			schedulerNGFactory,
			shuffleMaster,
			lookup -> new JobMasterPartitionTrackerImpl(
				jobGraph.getJobID(),
				shuffleMaster,
				lookup
			),
			new DefaultExecutionDeploymentTracker(),
			DefaultExecutionDeploymentReconciler::new,
			initializationTimestamp);
	}
}
```
 
#### 1.3.2 runner.start()

进入 `runner.start()` 方法，它在 JobManagerRunner 接口下的一个方法，用来启动 JobMaster 的执行

```java
/**
 * Interface for a runner which executes a {@link JobMaster}.
 */
public interface JobManagerRunner extends AutoCloseableAsync {

	/**
	 * Start the execution of the {@link JobMaster}.
	 *
	 * @throws Exception if the JobMaster cannot be started
	 */
	void start() throws Exception;
}
```

查看其实现类 `JobManagerRunnerImpl`

```java
/**
 * The runner for the job manager. It deals with job level leader election and make underlying job manager
 * properly reacted.
 */
public class JobManagerRunnerImpl implements LeaderContender, OnCompletionActions, JobManagerRunner {
	public void start() throws Exception {
		try {
			leaderElectionService.start(this);
		} catch (Exception e) {
			log.error("Could not start the JobManager because the leader election service did not start.", e);
			throw new Exception("Could not start the leader election service.", e);
		}
	}

}
```

通过层层调用，进入到 `JobMasterService` 接口的 `start` 方法，在方法中，启动 JobMaster 服务。

```java
	/**
	 * Start the JobMaster service with the given {@link JobMasterId}.
	 *
	 * @param jobMasterId to start the service with
	 * @return Future which is completed once the JobMaster service has been started
	 * @throws Exception if the JobMaster service could not be started
	 */
	CompletableFuture<Acknowledge> start(JobMasterId jobMasterId) throws Exception;
```

查看其实现类 `JobMaster`，在方法中，启动 rpc 服务，开始运行作业。

```java
	/**
	 * Start the rpc service and begin to run the job.
	 *
	 * @param newJobMasterId The necessary fencing token to run the job
	 * @return Future acknowledge if the job could be started. Otherwise the future contains an exception
	 */
	public CompletableFuture<Acknowledge> start(final JobMasterId newJobMasterId) throws Exception {
		// make sure we receive RPC and async calls
		start();
		// 异步不阻塞 调用
		return callAsyncWithoutFencing(() -> startJobExecution(newJobMasterId), RpcUtils.INF_TIMEOUT);
	}
```

```java
	private Acknowledge startJobExecution(JobMasterId newJobMasterId) throws Exception {
		...

		// 真正启动JobMaster服务
		startJobMasterServices();

		log.info("Starting execution of job {} ({}) under job master id {}.", jobGraph.getName(), jobGraph.getJobID(), newJobMasterId);

		// 重置和启动调度器   【在这里面会将jobGraph转成executionGraph】
		resetAndStartScheduler();

		return Acknowledge.get();
	}
```

### 1.4 启动ResourceManager: resourceManager.start()

`ResourceManager` 类继承至 `RpcEndpoint` 类，这里的 start 方法是调用的 `RpcEndpoint` 类的 `start` 方法。

所以这里触发了启动一个 rpc 终端，这就告诉底层 rpc 服务: rpc 终端已准备好处理远程过程调用。 

```java
public abstract class RpcEndpoint implements RpcGateway, AutoCloseableAsync {
	/**
	 * Triggers start of the rpc endpoint. This tells the underlying rpc server that the rpc endpoint is ready
	 * to process remote procedure calls.
	 */
	public final void start() {
		// 终端的启动，实际上是由 自身网关（RpcServer）来启动的
		rpcServer.start();
	}
}
```

类似 Dispatcher 的启动，去查看 `ResourceManager` 类下的 `onStart` 方法

```java
	public final void onStart() throws Exception {
		try {
			startResourceManagerServices();
		} ...
	}

	private void startResourceManagerServices() throws Exception {
		try {
			leaderElectionService = highAvailabilityServices.getResourceManagerLeaderElectionService();

			// (1.4.1节)创建了Yarn的RM和NM的客户端，初始化并启动
			// flink的rm向yarn的rm申请资源，就是通过创建yarn的rm客户端来申请的
			initialize();

			// (1.4.2节)通过选举服务，启动ResourceManager
			leaderElectionService.start(this);

			jobLeaderIdService.start(new JobLeaderIdActionsImpl());

			registerTaskExecutorMetrics();
		} catch (Exception e) {
			handleStartResourceManagerServicesException(e);
		}
	}
```

#### 1.4.1 initialize()

该方法通过层层调用，进入到 `AbstractResourceManagerDriver` 类的 `initializeInternal` 方法

```java
public abstract class AbstractResourceManagerDriver<WorkerType extends ResourceIDRetrievable>
	implements ResourceManagerDriver<WorkerType> {

	public final void initialize(
			ResourceEventHandler<WorkerType> resourceEventHandler,
			ScheduledExecutor mainThreadExecutor,
			Executor ioExecutor) throws Exception {
		...

		initializeInternal();
	}

	/**
	 * Initialize the deployment specific components.
	 */
	protected abstract void initializeInternal() throws Exception;
	}
```

查看其实现类 `YarnResourceManagerDriver`

```java
public class YarnResourceManagerDriver extends AbstractResourceManagerDriver<YarnWorkerNode> {
	protected void initializeInternal() throws Exception {
		final YarnContainerEventHandler yarnContainerEventHandler = new YarnContainerEventHandler();
		try {
			// 创建Yarn的ResourceManager的客户端，并且初始化和启动
			resourceManagerClient = yarnResourceManagerClientFactory.createResourceManagerClient(
				yarnHeartbeatIntervalMillis,
				yarnContainerEventHandler);
			resourceManagerClient.init(yarnConfig);
			resourceManagerClient.start();

			final RegisterApplicationMasterResponse registerApplicationMasterResponse = registerApplicationMaster();
			getContainersFromPreviousAttempts(registerApplicationMasterResponse);
			taskExecutorProcessSpecContainerResourcePriorityAdapter =
				new TaskExecutorProcessSpecContainerResourcePriorityAdapter(
					registerApplicationMasterResponse.getMaximumResourceCapability(),
					ExternalResourceUtils.getExternalResources(flinkConfig, YarnConfigOptions.EXTERNAL_RESOURCE_YARN_CONFIG_KEY_SUFFIX));
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start resource manager client.", e);
		}

		// 创建yarn的 NodeManager的客户端，并且初始化和启动
		nodeManagerClient = yarnNodeManagerClientFactory.createNodeManagerClient(yarnContainerEventHandler);
		nodeManagerClient.init(yarnConfig);
		nodeManagerClient.start();
	}
}
```


#### 1.4.2 leaderElectionService.start(this)

```java
public interface LeaderElectionService {
	/**
	 * Starts the leader election service. This method can only be called once.
	 *
	 * @param contender LeaderContender which applies for the leadership
	 * @throws Exception
	 */
	void start(LeaderContender contender) throws Exception;
}
```

查看其实现类 `StandaloneLeaderElectionService`

```java
public class StandaloneLeaderElectionService implements LeaderElectionService {
	public void start(LeaderContender newContender) throws Exception {
		if (contender != null) {
			// Service was already started
			throw new IllegalArgumentException("Leader election service cannot be started multiple times.");
		}

		contender = Preconditions.checkNotNull(newContender);

		// directly grant leadership to the given contender
		// 直接给传入的竞争者领导权
		contender.grantLeadership(HighAvailabilityServices.DEFAULT_LEADER_ID);
	}
}
```

回到 ResourceManager 类

```java
	/**
	 * Callback method when current resourceManager is granted leadership.
	 *
	 * @param newLeaderSessionID unique leadershipID
	 */
	@Override
	public void grantLeadership(final UUID newLeaderSessionID) {

		final CompletableFuture<Boolean> acceptLeadershipFuture = clearStateFuture
			.thenComposeAsync((ignored) -> tryAcceptLeadership(newLeaderSessionID), getUnfencedMainThreadExecutor());

		...
	}

	private CompletableFuture<Boolean> tryAcceptLeadership(final UUID newLeaderSessionID) {
		if (leaderElectionService.hasLeadership(newLeaderSessionID)) {
			final ResourceManagerId newResourceManagerId = ResourceManagerId.fromUuid(newLeaderSessionID);

			log.info("ResourceManager {} was granted leadership with fencing token {}", getAddress(), newResourceManagerId);

			// clear the state if we've been the leader before
			if (getFencingToken() != null) {
				clearStateInternal();
			}

			setFencingToken(newResourceManagerId);
			// 这里
			startServicesOnLeadership();

			return prepareLeadershipAsync().thenApply(ignored -> true);
		} else {
			return CompletableFuture.completedFuture(false);
		}
	}

	// 在leader上启动rm
	private void startServicesOnLeadership() {
		// (1.4.2.1节)启动心跳服务：TaskManager、JobMaster
		startHeartbeatServices();

		// (1.4.2.2节)启动slotManager
		slotManager.start(getFencingToken(), getMainThreadExecutor(), new ResourceActionsImpl());

		onLeadership();
	}
```

##### 1.4.2.1 startHeartbeatServices()

```java
	private void startHeartbeatServices() {
		taskManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new TaskManagerHeartbeatListener(),
			getMainThreadExecutor(),
			log);

		jobManagerHeartbeatManager = heartbeatServices.createHeartbeatManagerSender(
			resourceId,
			new JobManagerHeartbeatListener(),
			getMainThreadExecutor(),
			log);
	}
```

##### 1.4.2.2 slotManager.start()

```java
// The slot manager is responsible for maintaining a view on all registered task manager slots,their allocation and all pending slot requests.
public interface SlotManager extends AutoCloseable {
	/**
	 * Starts the slot manager with the given leader id and resource manager actions.
	 *
	 * @param newResourceManagerId to use for communication with the task managers
	 * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
	 * @param newResourceActions to use for resource (de-)allocations
	 */
	void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions);
}
```

查看其实现类 SlotManagerImpl

```java
public class SlotManagerImpl implements SlotManager {
	public void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions) {
		LOG.info("Starting the SlotManager.");

		...

		started = true;

		taskManagerTimeoutsAndRedundancyCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				// 这里
				() -> checkTaskManagerTimeoutsAndRedundancy()),
			0L,
			taskManagerTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		slotRequestTimeoutCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				() -> checkSlotRequestTimeouts()),
			0L,
			slotRequestTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		registerSlotManagerMetrics();
	}
}
```

```java
void checkTaskManagerTimeoutsAndRedundancy() {
		if (!taskManagerRegistrations.isEmpty()) {
			long currentTime = System.currentTimeMillis();

			ArrayList<TaskManagerRegistration> timedOutTaskManagers = new ArrayList<>(taskManagerRegistrations.size());

			// first retrieve the timed out TaskManagers
			for (TaskManagerRegistration taskManagerRegistration : taskManagerRegistrations.values()) {
				if (currentTime - taskManagerRegistration.getIdleSince() >= taskManagerTimeout.toMilliseconds()) {
					// we collect the instance ids first in order to avoid concurrent modifications by the
					// ResourceActions.releaseResource call
					timedOutTaskManagers.add(taskManagerRegistration);
				}
			}

			int slotsDiff = redundantTaskManagerNum * numSlotsPerWorker - freeSlots.size();
			if (freeSlots.size() == slots.size()) {
				// No need to keep redundant taskManagers if no job is running.
				// 如果没有 job 在运行，释放 taskmanager
				releaseTaskExecutors(timedOutTaskManagers, timedOutTaskManagers.size());
			} else if (slotsDiff > 0) {
				// Keep enough redundant taskManagers from time to time.
				// 保证随时有足够的 taskmanager
				int requiredTaskManagers = MathUtils.divideRoundUp(slotsDiff, numSlotsPerWorker);
				allocateRedundantTaskManagers(requiredTaskManagers);
			} else {
				// second we trigger the release resource callback which can decide upon the resource release
				// 其次，我们触发释放资源回调，它可以决定资源的释放
				int maxReleaseNum = (-slotsDiff) / numSlotsPerWorker;
				releaseTaskExecutors(timedOutTaskManagers, Math.min(maxReleaseNum, timedOutTaskManagers.size()));
			}
		}
	}
```