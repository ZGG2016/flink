# 提交流程 07 - 注册分配提供slot

[TOC]

**版本:1.12**

-----------------------------------------------

在 `提交流程 05 - JM申请资源.md` 中，JobMaster 已向 Yarn 的 ResourceManager 申请了资源，即分配到了 container.

在 `提交流程 06 - 启动TaskManager.md` 中，已经启动并向 ResourceManager 成功注册了 TaskManager(TaskExecutor).

接下来 TaskExecutor 需要向 Flink 的 SlotManager 注册 slot, 然后 SlotManager 进行分配，分配后向 JobManager 提供 slot.

TaskExecutor 成功注册后，调用 `onRegistrationSuccess()`, 

```java
public class TaskExecutorToResourceManagerConnection
		extends RegisteredRpcConnection<ResourceManagerId, ResourceManagerGateway, TaskExecutorRegistrationSuccess> {
	protected void onRegistrationSuccess(TaskExecutorRegistrationSuccess success) {
		log.info("Successful registration at resource manager {} under registration id {}.",
			getTargetAddress(), success.getRegistrationId());
		
		// This method is called by the {@link RegisteredRpcConnection} when the registration is success.
		// 这个方法在注册成功时，由 RegisteredRpcConnection 调用
		registrationListener.onRegistrationSuccess(this, success);
	}		

}
```

进入到 `TaskExecutor.ResourceManagerRegistrationListener` 类下的 `onRegistrationSuccess` 方法

```java
	private final class ResourceManagerRegistrationListener implements RegistrationConnectionListener<TaskExecutorToResourceManagerConnection, TaskExecutorRegistrationSuccess> {

		@Override
		public void onRegistrationSuccess(TaskExecutorToResourceManagerConnection connection, TaskExecutorRegistrationSuccess success) {
			final ResourceID resourceManagerId = success.getResourceManagerId();
			final InstanceID taskExecutorRegistrationId = success.getRegistrationId();
			final ClusterInformation clusterInformation = success.getClusterInformation();
			final ResourceManagerGateway resourceManagerGateway = connection.getTargetGateway();

			runAsync(
				() -> {
					// filter out outdated connections
					//noinspection ObjectEquality
					if (resourceManagerConnection == connection) {
						try {
							establishResourceManagerConnection(
								resourceManagerGateway,
								resourceManagerId,
								taskExecutorRegistrationId,
								clusterInformation);
						} catch (Throwable t) {
							log.error("Establishing Resource Manager connection in Task Executor failed", t);
						}
					}
				});
		}
	}
```

```java
	private void establishResourceManagerConnection(
			ResourceManagerGateway resourceManagerGateway,
			ResourceID resourceManagerResourceId,
			InstanceID taskExecutorRegistrationId,
			ClusterInformation clusterInformation) {

		// Sends the given {@link SlotReport} to the ResourceManager.
		// 给 ResourceManager 发送 SlotReport   （这个方法在TaskExecutor类下）
		final CompletableFuture<Acknowledge> slotReportResponseFuture = resourceManagerGateway.sendSlotReport(
			getResourceID(),
			taskExecutorRegistrationId,
			taskSlotTable.createSlotReport(getResourceID()),
			taskManagerConfiguration.getTimeout());

		slotReportResponseFuture.whenCompleteAsync(
			(acknowledge, throwable) -> {
				if (throwable != null) {
					reconnectToResourceManager(new TaskManagerException("Failed to send initial slot report to ResourceManager.", throwable));
				}
			}, getMainThreadExecutor());

		// monitor the resource manager as heartbeat target
		...

		// set the propagated blob server address
		...

		// Container for the resource manager connection instances used by the {@link TaskExecutor}.
		establishedResourceManagerConnection = new EstablishedResourceManagerConnection(
			resourceManagerGateway,
			resourceManagerResourceId,
			taskExecutorRegistrationId);

		stopRegistrationTimeout();
	}
```

进入到 `ResourceManager` 类下的 `sendSlotReport` 方法

```java
	public CompletableFuture<Acknowledge> sendSlotReport(ResourceID taskManagerResourceId, InstanceID taskManagerRegistrationId, SlotReport slotReport, Time timeout) {
		final WorkerRegistration<WorkerType> workerTypeWorkerRegistration = taskExecutors.get(taskManagerResourceId);

		if (workerTypeWorkerRegistration.getInstanceID().equals(taskManagerRegistrationId)) {
			// ResourceManager中的slotManager管理slot
			// registerTaskManager: 在slotManager中注册一个新的TaskManager，这才能使得 task managers slots 可用被分配
			if (slotManager.registerTaskManager(workerTypeWorkerRegistration, slotReport)) {
				onWorkerRegistered(workerTypeWorkerRegistration.getWorker());
			}
			return CompletableFuture.completedFuture(Acknowledge.get());
		} else {
			return FutureUtils.completedExceptionally(new ResourceManagerException(String.format("Unknown TaskManager registration id %s.", taskManagerRegistrationId)));
		}
	}
```

进入到 `SlotManagerImpl` 类下的 `registerTaskManager` 方法

```java
public boolean registerTaskManager(final TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
		checkInit();

		LOG.debug("Registering TaskManager {} under {} at the SlotManager.", taskExecutorConnection.getResourceID().getStringWithMetadata(), taskExecutorConnection.getInstanceID());

		// 已经包含了，就不注册了
		// we identify task managers by their instance id
		if (taskManagerRegistrations.containsKey(taskExecutorConnection.getInstanceID())) {
			reportSlotStatus(taskExecutorConnection.getInstanceID(), initialSlotReport);
			return false;
		} else {
			// 检查有没有超过总额度
			if (isMaxSlotNumExceededAfterRegistration(initialSlotReport)) {
				LOG.info("The total number of slots exceeds the max limitation {}, release the excess resource.", maxSlotNum);
				resourceActions.releaseResource(taskExecutorConnection.getInstanceID(), new FlinkException("The total number of slots exceeds the max limitation."));
				return false;
			}

			// first register the TaskManager
			ArrayList<SlotID> reportedSlots = new ArrayList<>();

			for (SlotStatus slotStatus : initialSlotReport) {
				reportedSlots.add(slotStatus.getSlotID());
			}

			TaskManagerRegistration taskManagerRegistration = new TaskManagerRegistration(
				taskExecutorConnection,
				reportedSlots);

			taskManagerRegistrations.put(taskExecutorConnection.getInstanceID(), taskManagerRegistration);

			// next register the new slots
			for (SlotStatus slotStatus : initialSlotReport) {
				// 为给定的taskmanager在slotmanager中注册一个slot
				registerSlot(
					slotStatus.getSlotID(),
					slotStatus.getAllocationID(),
					slotStatus.getJobID(),
					slotStatus.getResourceProfile(),
					taskExecutorConnection);
			}

			return true;
		}

	}
```

```java
	/**
	 * Registers a slot for the given task manager at the slot manager. The slot is identified by
	 * the given slot id. The given resource profile defines the available resources for the slot.
	 * The task manager connection can be used to communicate with the task manager.
	 *
	 * @param slotId identifying the slot on the task manager
	 * @param allocationId which is currently deployed in the slot
	 * @param resourceProfile of the slot
	 * @param taskManagerConnection to communicate with the remote task manager
	 */
	private void registerSlot(
			SlotID slotId,
			AllocationID allocationId,
			JobID jobId,
			ResourceProfile resourceProfile,
			TaskExecutorConnection taskManagerConnection) {

		// 移除旧 slot
		if (slots.containsKey(slotId)) {
			// remove the old slot first
			removeSlot(
				slotId,
				new SlotManagerException(
					String.format(
						"Re-registration of slot %s. This indicates that the TaskExecutor has re-connected.",
						slotId)));
		}

		// (2.1节)创建和注册 这些新的 slot
		final TaskManagerSlot slot = createAndRegisterTaskManagerSlot(slotId, resourceProfile, taskManagerConnection);

		// 挂起的slot就是空闲的slot，等着被分配
		final PendingTaskManagerSlot pendingTaskManagerSlot;

		if (allocationId == null) {
			pendingTaskManagerSlot = findExactlyMatchingPendingTaskManagerSlot(resourceProfile);
		} else {
			pendingTaskManagerSlot = null;
		}

		if (pendingTaskManagerSlot == null) {
			// 更新slot状态
			updateSlot(slotId, allocationId, jobId);
		} 
		// 存在等着被分配的slot
		else {
			// 将即将被分配的slot从挂起slot里移出
			pendingSlots.remove(pendingTaskManagerSlot.getTaskManagerSlotId());
			// 取出来挂起的请求
			final PendingSlotRequest assignedPendingSlotRequest = pendingTaskManagerSlot.getAssignedPendingSlotRequest();

			// 分配slot
			if (assignedPendingSlotRequest == null) {
				// (2.2节)表示 挂起的请求都已经满足了，你暂时没事
				handleFreeSlot(slot);
			} else {
				// (2.3节)表示 你要被分配给某个请求
				assignedPendingSlotRequest.unassignPendingTaskManagerSlot();
				allocateSlot(slot, assignedPendingSlotRequest);
			}
		}
	}
```

### 2.1 createAndRegisterTaskManagerSlot()

```java
	/** Map for all registered slots. */
	private final HashMap<SlotID, TaskManagerSlot> slots;

	private TaskManagerSlot createAndRegisterTaskManagerSlot(SlotID slotId, ResourceProfile resourceProfile, TaskExecutorConnection taskManagerConnection) {
		// TaskManagerSlot: A TaskManagerSlot represents a slot located in a TaskManager
		final TaskManagerSlot slot = new TaskManagerSlot(
			slotId,
			resourceProfile,
			taskManagerConnection);
		slots.put(slotId, slot);
		return slot;
	}
```

### 2.2 handleFreeSlot()

```java
	/** Index of all currently free slots. */
	private final LinkedHashMap<SlotID, TaskManagerSlot> freeSlots;

	/**
	 * Handles a free slot. It first tries to find a pending slot request which can be fulfilled.
	 * If there is no such request, then it will add the slot to the set of free slots.
	 *
	 * @param freeSlot to find a new slot request for
	 */
	private void handleFreeSlot(TaskManagerSlot freeSlot) {
		Preconditions.checkState(freeSlot.getState() == SlotState.FREE);

		PendingSlotRequest pendingSlotRequest = findMatchingRequest(freeSlot.getResourceProfile());
		// 还存在挂起的请求就将空闲的slot分配给它，否则将其放到freeSlots
		if (null != pendingSlotRequest) {
			allocateSlot(freeSlot, pendingSlotRequest);
		} else {
			freeSlots.put(freeSlot.getSlotId(), freeSlot);
		}
	}
```

### 2.3 allocateSlot()

```java
	/**
	 * 为给定的slot请求分配给定的slot
	 * Allocates the given slot for the given slot request. This entails sending a registration
	 * message to the task manager and treating failures.
	 *
	 * @param taskManagerSlot to allocate for the given slot request 分配给给定slot请求的 slot
	 * @param pendingSlotRequest to allocate the given slot for
	 */
	private void allocateSlot(TaskManagerSlot taskManagerSlot, PendingSlotRequest pendingSlotRequest) {
		Preconditions.checkState(taskManagerSlot.getState() == SlotState.FREE);

		TaskExecutorConnection taskExecutorConnection = taskManagerSlot.getTaskManagerConnection();
		TaskExecutorGateway gateway = taskExecutorConnection.getTaskExecutorGateway();

		final CompletableFuture<Acknowledge> completableFuture = new CompletableFuture<>();
		final AllocationID allocationId = pendingSlotRequest.getAllocationId();
		final SlotID slotId = taskManagerSlot.getSlotId();
		final InstanceID instanceID = taskManagerSlot.getInstanceId();

		// 将 taskManagerSlot 分配给 pendingSlotRequest
		taskManagerSlot.assignPendingSlotRequest(pendingSlotRequest);
		pendingSlotRequest.setRequestFuture(completableFuture);

		returnPendingTaskManagerSlotIfAssigned(pendingSlotRequest);

		TaskManagerRegistration taskManagerRegistration = taskManagerRegistrations.get(instanceID);

		if (taskManagerRegistration == null) {
			throw new IllegalStateException("Could not find a registered task manager for instance id " +
				instanceID + '.');
		}
		// 将slot标记为已使用
		taskManagerRegistration.markUsed();

		// RPC call to the task manager
		// 分配完slot之后，通知 taskmanager 提供 slot 给 jobmaster
		CompletableFuture<Acknowledge> requestFuture = gateway.requestSlot(
			slotId,
			pendingSlotRequest.getJobId(),
			allocationId,
			pendingSlotRequest.getResourceProfile(),
			pendingSlotRequest.getTargetAddress(),
			resourceManagerId,
			taskManagerRequestTimeout);

		...
	}
```

```java
public class TaskExecutor extends RpcEndpoint implements TaskExecutorGateway {

	public CompletableFuture<Acknowledge> requestSlot(
		final SlotID slotId,
		final JobID jobId,
		final AllocationID allocationId,
		final ResourceProfile resourceProfile,
		final String targetAddress,
		final ResourceManagerId resourceManagerId,
		final Time timeout) {
		// TODO: Filter invalid requests from the resource manager by using the instance/registration Id

		log.info("Receive slot request {} for job {} from resource manager with leader id {}.",
			allocationId, jobId, resourceManagerId);
		...

		try {
			// 【在之前slotmanager已经告诉taskexecutor怎么分slot，在这里自己开始分】
			// (2.3.1节)根据 RM 的命令，分配自己(TaskExecutor)的slot  
			allocateSlot(
				slotId,
				jobId,
				allocationId,
				resourceProfile);
		} catch (SlotAllocationException sae) {
			return FutureUtils.completedExceptionally(sae);
		}
		// 一个 JobTable 任务管理着 TaskExecutor 上的作业的生命周期
		final JobTable.Job job;

		try {
			// (2.3.2节)
			job = jobTable.getOrCreateJob(jobId, () -> registerNewJobAndCreateServices(jobId, targetAddress));
		} catch (Exception e) {
			// free the allocated slot
			try {
				taskSlotTable.freeSlot(allocationId);
			} catch (SlotNotFoundException slotNotFoundException) {
				// slot no longer existent, this should actually never happen, because we've
				// just allocated the slot. So let's fail hard in this case!
				onFatalError(slotNotFoundException);
			}

			// release local state under the allocation id.
			localStateStoresManager.releaseLocalStateForAllocationId(allocationId);

			// sanity check
			if (!taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
				onFatalError(new Exception("Could not free slot " + slotId));
			}

			return FutureUtils.completedExceptionally(new SlotAllocationException("Could not create new job.", e));
		}

		// 如果job连接到jobmanager，返回true
		if (job.isConnected()) {
			// (2.3.3节)向JobManager提供 slot
			offerSlotsToJobManager(jobId);
		}

		return CompletableFuture.completedFuture(Acknowledge.get());
	}
}
```

#### 2.3.1 allocateSlot()

```java
	private void allocateSlot(SlotID slotId,JobID jobId,AllocationID allocationId,
			ResourceProfile resourceProfile) throws SlotAllocationException {
		if (taskSlotTable.isSlotFree(slotId.getSlotNumber())) {
			// 将具有给定索引的slot分配给给定job
			// 如果slot能被分配，返回true
			if (taskSlotTable.allocateSlot(slotId.getSlotNumber(), jobId, allocationId, resourceProfile, taskManagerConfiguration.getTimeout())) {
				log.info("Allocated slot for {}.", allocationId);
			} else {
				log.info("Could not allocate slot for {}.", allocationId);
				throw new SlotAllocationException("Could not allocate slot.");
			}
		} else if (!taskSlotTable.isAllocated(slotId.getSlotNumber(), jobId, allocationId)) {
			final String message = "The slot " + slotId + " has already been allocated for a different job.";

			log.info(message);

			final AllocationID allocationID = taskSlotTable.getCurrentAllocation(slotId.getSlotNumber());
			throw new SlotOccupiedException(message, allocationID, taskSlotTable.getOwningJob(allocationID));
		}
	}
```

```java
public class TaskSlotTableImpl<T extends TaskSlotPayload> implements TaskSlotTable<T> {
	public boolean allocateSlot(
			int index,
			JobID jobId,
			AllocationID allocationId,
			ResourceProfile resourceProfile,
			Time slotTimeout) {
		checkRunning();

		Preconditions.checkArgument(index < numberSlots);

		TaskSlot<T> taskSlot = allocatedSlots.get(allocationId);
		if (taskSlot != null) {
			LOG.info("Allocation ID {} is already allocated in {}.", allocationId, taskSlot);
			return false;
		}

		if (taskSlots.containsKey(index)) {
			TaskSlot<T> duplicatedTaskSlot = taskSlots.get(index);
			LOG.info("Slot with index {} already exist, with resource profile {}, job id {} and allocation id {}.",
				index,
				duplicatedTaskSlot.getResourceProfile(),
				duplicatedTaskSlot.getJobId(),
				duplicatedTaskSlot.getAllocationId());
			return duplicatedTaskSlot.getJobId().equals(jobId) &&
				duplicatedTaskSlot.getAllocationId().equals(allocationId);
		} else if (allocatedSlots.containsKey(allocationId)) {
			return true;
		}

		resourceProfile = index >= 0 ? defaultSlotResourceProfile : resourceProfile;

		if (!budgetManager.reserve(resourceProfile)) {
			LOG.info("Cannot allocate the requested resources. Trying to allocate {}, "
					+ "while the currently remaining available resources are {}, total is {}.",
				resourceProfile,
				budgetManager.getAvailableBudget(),
				budgetManager.getTotalBudget());
			return false;
		}

		taskSlot = new TaskSlot<>(index, resourceProfile, memoryPageSize, jobId, allocationId, memoryVerificationExecutor);
		if (index >= 0) {
			taskSlots.put(index, taskSlot);
		}

		// update the allocation id to task slot map
		allocatedSlots.put(allocationId, taskSlot);

		// register a timeout for this slot since it's in state allocated
		timerService.registerTimeout(allocationId, slotTimeout.getSize(), slotTimeout.getUnit());

		// add this slot to the set of job slots
		Set<AllocationID> slots = slotsPerJob.get(jobId);

		if (slots == null) {
			slots = new HashSet<>(4);
			slotsPerJob.put(jobId, slots);
		}

		slots.add(allocationId);

		return true;
	}
}
```

#### 2.3.2 registerNewJobAndCreateServices()

```java
	private TaskExecutorJobServices registerNewJobAndCreateServices(JobID jobId, String targetAddress) throws Exception {
		jobLeaderService.addJob(jobId, targetAddress);
		final PermanentBlobCache permanentBlobService = blobCacheService.getPermanentBlobService();
		// 注册job
		permanentBlobService.registerJob(jobId);

		return TaskExecutorJobServices.create(
			// 为给定的jobid, 注册一个新的类加载器租约
			// 这个 job 的用户代码类加载器将是有效的，只要这个job的有效租约存在
			libraryCacheManager.registerClassLoaderLease(jobId),
			() -> permanentBlobService.releaseJob(jobId));
	}
```

#### 2.3.3 offerSlotsToJobManager()

```java
	private void offerSlotsToJobManager(final JobID jobId) {
		jobTable
			.getConnection(jobId)
			.ifPresent(this::internalOfferSlotsToJobManager);
	}

	// ----------------------------
	private void internalOfferSlotsToJobManager(JobTable.Connection jobManagerConnection) {
		final JobID jobId = jobManagerConnection.getJobId();

		if (taskSlotTable.hasAllocatedSlots(jobId)) {
			log.info("Offer reserved slots to the leader of job {}.", jobId);

			final JobMasterGateway jobMasterGateway = jobManagerConnection.getJobManagerGateway();

			final Iterator<TaskSlot<Task>> reservedSlotsIterator = taskSlotTable.getAllocatedSlots(jobId);
			final JobMasterId jobMasterId = jobManagerConnection.getJobMasterId();

			final Collection<SlotOffer> reservedSlots = new HashSet<>(2);

			while (reservedSlotsIterator.hasNext()) {
				SlotOffer offer = reservedSlotsIterator.next().generateSlotOffer();
				reservedSlots.add(offer);
			}

			// 将给定的slots提供给jobManager
			CompletableFuture<Collection<SlotOffer>> acceptedSlotsFuture = jobMasterGateway.offerSlots(
				getResourceID(),
				reservedSlots,
				taskManagerConfiguration.getTimeout());

			acceptedSlotsFuture.whenCompleteAsync(
				handleAcceptedSlotOffers(jobId, jobMasterGateway, jobMasterId, reservedSlots),
				getMainThreadExecutor());
		} else {
			log.debug("There are no unassigned slots for the job {}.", jobId);
		}
	}
```

进入到 `JobMaster` 类的 `offerSlots`

```java
	public CompletableFuture<Collection<SlotOffer>> offerSlots(
			final ResourceID taskManagerId,
			final Collection<SlotOffer> slots,
			final Time timeout) {

		Tuple2<TaskManagerLocation, TaskExecutorGateway> taskManager = registeredTaskManagers.get(taskManagerId);

		if (taskManager == null) {
			return FutureUtils.completedExceptionally(new Exception("Unknown TaskManager " + taskManagerId));
		}

		final TaskManagerLocation taskManagerLocation = taskManager.f0;
		final TaskExecutorGateway taskExecutorGateway = taskManager.f1;

		final RpcTaskManagerGateway rpcTaskManagerGateway = new RpcTaskManagerGateway(taskExecutorGateway, getFencingToken());

		return CompletableFuture.completedFuture(
			// 给slotpool提供多个slots.
			// 提供的slot可以被拒绝或接收
			slotPool.offerSlots(
				taskManagerLocation,
				rpcTaskManagerGateway,
				slots));
	}
```

进入到 `SlotPoolImpl` 类的 `offerSlots`

```java
	public Collection<SlotOffer> offerSlots(
			TaskManagerLocation taskManagerLocation,
			TaskManagerGateway taskManagerGateway,
			Collection<SlotOffer> offers) {

		ArrayList<SlotOffer> result = new ArrayList<>(offers.size());

		for (SlotOffer offer : offers) {
			// SlotPool判断是否接收slot
			if (offerSlot(
				taskManagerLocation,
				taskManagerGateway,
				offer)) {

				result.add(offer);
			}
		}

		return result;
	}
```

```java
	/**
	 * 
	 * Slot offering by TaskExecutor with AllocationID. The AllocationID is originally generated by this pool and
	 * transfer through the ResourceManager to TaskManager. We use it to distinguish the different allocation
	 * we issued. Slot offering may be rejected if we find something mismatching or there is actually no pending
	 * request waiting for this slot (maybe fulfilled by some other returned slot).
	 *
	 * @param taskManagerLocation location from where the offer comes from
	 * @param taskManagerGateway TaskManager gateway
	 * @param slotOffer the offered slot
	 * @return True if we accept the offering
	 */
	boolean offerSlot(
			final TaskManagerLocation taskManagerLocation,
			final TaskManagerGateway taskManagerGateway,
			final SlotOffer slotOffer) {

		componentMainThreadExecutor.assertRunningInMainThread();

		// check if this TaskManager is valid
		final ResourceID resourceID = taskManagerLocation.getResourceID();
		final AllocationID allocationID = slotOffer.getAllocationId();

		if (!registeredTaskManagers.contains(resourceID)) {
			log.debug("Received outdated slot offering [{}] from unregistered TaskManager: {}",
					slotOffer.getAllocationId(), taskManagerLocation);
			return false;
		}

		// check whether we have already using this slot
		AllocatedSlot existingSlot;
		if ((existingSlot = allocatedSlots.get(allocationID)) != null ||
			(existingSlot = availableSlots.get(allocationID)) != null) {

			// we need to figure out if this is a repeated offer for the exact same slot,
			// or another offer that comes from a different TaskManager after the ResourceManager
			// re-tried the request

			// we write this in terms of comparing slot IDs, because the Slot IDs are the identifiers of
			// the actual slots on the TaskManagers
			// Note: The slotOffer should have the SlotID
			final SlotID existingSlotId = existingSlot.getSlotId();
			final SlotID newSlotId = new SlotID(taskManagerLocation.getResourceID(), slotOffer.getSlotIndex());

			if (existingSlotId.equals(newSlotId)) {
				log.info("Received repeated offer for slot [{}]. Ignoring.", allocationID);

				// return true here so that the sender will get a positive acknowledgement to the retry
				// and mark the offering as a success
				return true;
			} else {
				// the allocation has been fulfilled by another slot, reject the offer so the task executor
				// will offer the slot to the resource manager
				return false;
			}
		}
		// AllocatedSlot表示TaskExecutor分配给JobMaster的slot
		final AllocatedSlot allocatedSlot = new AllocatedSlot(
			allocationID,
			taskManagerLocation,
			slotOffer.getSlotIndex(),
			slotOffer.getResourceProfile(),
			taskManagerGateway);

		// use the slot to fulfill pending request, in requested order
		// 按照请求顺序，使用 slot 来满足挂起的请求
		t 

		// we accepted the request in any case. slot will be released after it idled for
		// too long and timed out
		return true;
	}
```

```java
	/**
	 * Tries to fulfill with the given allocated slot a pending slot request or add the
	 * allocated slot to the set of available slots if no matching request is available.
	 *
	 * @param allocatedSlot which shall be returned
	 */
	private void tryFulfillSlotRequestOrMakeAvailable(AllocatedSlot allocatedSlot) {
		Preconditions.checkState(!allocatedSlot.isUsed(), "Provided slot is still in use.");

		final PendingRequest pendingRequest = findMatchingPendingRequest(allocatedSlot);

		if (pendingRequest != null) {
			log.debug("Fulfilling pending slot request [{}] with slot [{}]",
				pendingRequest.getSlotRequestId(), allocatedSlot.getAllocationId());

			// Checks whether there exists a pending request with the given slot request id and removes it from the internal data structures.
			removePendingRequest(pendingRequest.getSlotRequestId());

			allocatedSlots.add(pendingRequest.getSlotRequestId(), allocatedSlot);
			pendingRequest.getAllocatedSlotFuture().complete(allocatedSlot);

			// this allocation may become orphan once its corresponding request is removed
			final Optional<AllocationID> allocationIdOfRequest = pendingRequest.getAllocationId();

			// the allocation id can be null if the request was fulfilled by a slot directly offered
			// by a reconnected TaskExecutor before the ResourceManager is connected
			if (allocationIdOfRequest.isPresent()) {
				maybeRemapOrphanedAllocation(allocationIdOfRequest.get(), allocatedSlot.getAllocationId());
			}
		} else {
			log.debug("Adding slot [{}] to available slots", allocatedSlot.getAllocationId());
			availableSlots.add(allocatedSlot, clock.relativeTimeMillis());
		}
	}
```