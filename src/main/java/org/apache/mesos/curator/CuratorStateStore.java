package org.apache.mesos.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.mesos.Protos;
import org.apache.mesos.dcos.DcosConstants;
import org.apache.mesos.offer.TaskException;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.state.SchemaVersionStore;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.apache.mesos.state.StateStoreUtils;
import org.apache.mesos.storage.CuratorPersister;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * CuratorStateStore is an implementation of {@link StateStore} which persists data in Zookeeper.
 *
 * The ZNode structure in Zookeeper is as follows:
 * <code>
 * rootPath/
 *     -> FrameworkID
 *     -> Tasks/
 *         -> [TaskName-0]/
 *             -> TaskInfo
 *             -> TaskStatus
 *         -> [TaskName-1]/
 *             -> TaskInfo
 *         -> [TaskName-2]/
 *             -> TaskInfo
 *             -> TaskStatus
 *         -> ...
 * </code>
 *
 * Note that for frameworks which don't use custom executors, the same structure is used, except
 * where ExecutorName values are equal to TaskName values.
 */
public class CuratorStateStore implements StateStore {

    private static final Logger logger = LoggerFactory.getLogger(CuratorStateStore.class);

    /**
     * @see CuratorSchemaVersionStore#CURRENT_SCHEMA_VERSION
     */
    private static final int MIN_SUPPORTED_SCHEMA_VERSION = 1;
    private static final int MAX_SUPPORTED_SCHEMA_VERSION = 1;

    private static final String TASK_INFO_PATH_NAME = "TaskInfo";
    private static final String TASK_STATUS_PATH_NAME = "TaskStatus";
    private static final String FWK_ID_PATH_NAME = "FrameworkID";
    private static final String PROPERTIES_PATH_NAME = "Properties";
    private static final String TASKS_ROOT_NAME = "Tasks";

    private final CuratorPersister curator;
    private final TaskPathMapper taskPathMapper;
    private final String fwkIdPath;
    private final String propertiesPath;

    /**
     * Creates a new {@link StateStore} which uses Curator with a default {@link RetryPolicy} and
     * connection string.
     *
     * @param frameworkName    The name of the framework
     */
    public CuratorStateStore(String frameworkName) {
        this(frameworkName, DcosConstants.MESOS_MASTER_ZK_CONNECTION_STRING);
    }

    /**
     * Creates a new {@link StateStore} which uses Curator with a default {@link RetryPolicy}.
     *
     * @param frameworkName    The name of the framework
     * @param connectionString The host/port of the ZK server, eg "master.mesos:2181"
     */
    public CuratorStateStore(String frameworkName, String connectionString) {
        this(frameworkName, connectionString, new ExponentialBackoffRetry(
                CuratorUtils.DEFAULT_CURATOR_POLL_DELAY_MS,
                CuratorUtils.DEFAULT_CURATOR_MAX_RETRIES));
    }

    /**
     * Creates a new {@link StateStore} which uses Curator with a custom {@link RetryPolicy}.
     *
     * @param frameworkName    The name of the framework
     * @param connectionString The host/port of the ZK server, eg "master.mesos:2181"
     * @param retryPolicy      The custom {@link RetryPolicy}
     */
    public CuratorStateStore(
            String frameworkName, String connectionString, RetryPolicy retryPolicy) {
        this.curator = new CuratorPersister(connectionString, retryPolicy);

        // Check version up-front:
        int currentVersion = new CuratorSchemaVersionStore(curator, frameworkName).fetch();
        if (!SchemaVersionStore.isSupported(
                currentVersion, MIN_SUPPORTED_SCHEMA_VERSION, MAX_SUPPORTED_SCHEMA_VERSION)) {
            throw new IllegalStateException(String.format(
                    "Storage schema version %d is not supported by this software " +
                            "(support: min=%d, max=%d)",
                    currentVersion, MIN_SUPPORTED_SCHEMA_VERSION, MAX_SUPPORTED_SCHEMA_VERSION));
        }

        final String rootPath = CuratorUtils.toServiceRootPath(frameworkName);
        this.taskPathMapper = new TaskPathMapper(rootPath);
        this.fwkIdPath = CuratorUtils.join(rootPath, FWK_ID_PATH_NAME);
        this.propertiesPath = CuratorUtils.join(rootPath, PROPERTIES_PATH_NAME);
    }

    // Framework ID

    @Override
    public void storeFrameworkId(Protos.FrameworkID fwkId) throws StateStoreException {
        try {
            logger.debug("Storing FrameworkID in '{}'", fwkIdPath);
            curator.store(fwkIdPath, fwkId.toByteArray());
        } catch (Exception e) {
            throw new StateStoreException(String.format(
                    "Failed to store FrameworkID in '%s'", fwkIdPath), e);
        }
    }

    @Override
    public void clearFrameworkId() throws StateStoreException {
        try {
            logger.debug("Clearing FrameworkID at '{}'", fwkIdPath);
            curator.clear(fwkIdPath);
        } catch (KeeperException.NoNodeException e) {
            // Clearing a non-existent FrameworkID should not result in an exception from us.
            logger.warn("Cleared unset FrameworkID, continuing silently", e);
            return;
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public Optional<Protos.FrameworkID> fetchFrameworkId() throws StateStoreException {
        try {
            logger.debug("Fetching FrameworkID from '{}'", fwkIdPath);
            byte[] bytes = curator.fetch(fwkIdPath);
            if (bytes.length > 0) {
                return Optional.of(Protos.FrameworkID.parseFrom(bytes));
            } else {
                throw new StateStoreException(String.format(
                        "Failed to retrieve FrameworkID in '%s'", fwkIdPath));
            }
        } catch (KeeperException.NoNodeException e) {
            logger.warn("No FrameworkId found at: " + fwkIdPath);
            return Optional.empty();
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    // Write Tasks

    @Override
    public void storeTasks(Collection<Protos.TaskInfo> tasks) throws StateStoreException {
        for (Protos.TaskInfo taskInfo : tasks) {
            String path = taskPathMapper.getTaskInfoPath(taskInfo.getName());
            logger.debug("Storing Taskinfo for {} in '{}'", taskInfo.getName(), path);
            try {
                curator.store(path, taskInfo.toByteArray());
            } catch (Exception e) {
                // exit early, without proceeding to other tasks:
                throw new StateStoreException(String.format(
                        "Failed to store TaskInfo in '%s'", path), e);
            }
        }
    }

    @Override
    public void storeStatus(Protos.TaskStatus status) throws StateStoreException {
        String taskName;
        try {
            taskName = TaskUtils.toTaskName(status.getTaskId());
        } catch (TaskException e) {
            throw new StateStoreException(String.format(
                    "Failed to parse the Task Name from TaskStatus.task_id: '%s'", status), e);
        }

        // Validate that a TaskInfo with the exact same UUID is currently present. We intentionally
        // ignore TaskStatuses whose TaskID doesn't (exactly) match the current TaskInfo: We will
        // occasionally get these for stale tasks that have since been changed (with new UUIDs).
        Optional<Protos.TaskInfo> optionalTaskInfo;
        try {
            optionalTaskInfo = fetchTask(taskName);
        } catch (Exception e) {
            throw new StateStoreException(String.format(
                    "Unable to retrieve matching TaskInfo for the provided TaskStatus name %s.", taskName), e);
        }

        if (!optionalTaskInfo.isPresent()) {
            throw new StateStoreException(
                    String.format("The following TaskInfo is not present in the StateStore: %s. " +
                            "TaskInfo must be present in order to store a TaskStatus.", taskName));
        }

        if (!optionalTaskInfo.get().getTaskId().getValue().equals(status.getTaskId().getValue())) {
            throw new StateStoreException(String.format(
                    "Task ID '%s' of updated status doesn't match Task ID '%s' of current TaskInfo."
                            + " Task IDs must exactly match before status may be updated."
                            + " NewTaskStatus[%s] CurrentTaskInfo[%s]",
                    status.getTaskId().getValue(), optionalTaskInfo.get().getTaskId().getValue(),
                    status, optionalTaskInfo));
        }

        String path = taskPathMapper.getTaskStatusPath(taskName);
        logger.debug("Storing status for '{}' in '{}'", taskName, path);

        try {
            curator.store(path, status.toByteArray());
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public void clearTask(String taskName) throws StateStoreException {
        String path = taskPathMapper.getTaskPath(taskName);
        logger.debug("Clearing Task at '{}'", path);
        try {
            curator.clear(path);
        } catch (KeeperException.NoNodeException e) {
            // Clearing a non-existent Task should not result in an exception from us.
            logger.warn("Cleared nonexistent Task, continuing silently: {}", taskName, e);
            return;
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    // Read Tasks

    @Override
    public Collection<String> fetchTaskNames() throws StateStoreException {
        String path = taskPathMapper.getTasksRootPath();
        logger.debug("Fetching task names from '{}'", path);
        try {
            Collection<String> taskNames = new ArrayList<>();
            for (String childNode : curator.getChildren(path)) {
                taskNames.add(childNode);
            }
            return taskNames;
        } catch (KeeperException.NoNodeException e) {
            // Root path doesn't exist yet. Treat as an empty list of tasks. This scenario is
            // expected to commonly occur when the Framework is being run for the first time.
            return Collections.emptyList();
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public Collection<Protos.TaskInfo> fetchTasks() throws StateStoreException {
        Collection<Protos.TaskInfo> taskInfos = new ArrayList<>();
        for (String taskName : fetchTaskNames()) {
            try {
                byte[] bytes = curator.fetch(taskPathMapper.getTaskInfoPath(taskName));
                taskInfos.add(Protos.TaskInfo.parseFrom(bytes));
            } catch (Exception e) {
                // Throw even for NoNodeException: We should always have a TaskInfo for every entry
                throw new StateStoreException(e);
            }
        }
        return taskInfos;
    }

    @Override
    public Optional<Protos.TaskInfo> fetchTask(String taskName) throws StateStoreException {
        String path = taskPathMapper.getTaskInfoPath(taskName);
        logger.debug("Fetching TaskInfo {} from '{}'", taskName, path);
        try {
            byte[] bytes = curator.fetch(path);
            if (bytes.length > 0) {
                return Optional.of(Protos.TaskInfo.parseFrom(bytes));
            } else {
                throw new StateStoreException(String.format(
                        "Failed to retrieve TaskInfo for TaskName: %s", taskName));
            }
        } catch (KeeperException.NoNodeException e) {
            logger.warn("No TaskInfo found for the requested name: " + taskName + " at: " + path);
            return Optional.empty();
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public Collection<Protos.TaskStatus> fetchStatuses() throws StateStoreException {
        Collection<Protos.TaskStatus> taskStatuses = new ArrayList<>();
        for (String taskName : fetchTaskNames()) {
            try {
                byte[] bytes = curator.fetch(taskPathMapper.getTaskStatusPath(taskName));
                taskStatuses.add(Protos.TaskStatus.parseFrom(bytes));
            } catch (KeeperException.NoNodeException e) {
                // The task node exists, but it doesn't contain a TaskStatus node. This may occur if
                // the only contents are a TaskInfo.
                continue;
            } catch (Exception e) {
                throw new StateStoreException(e);
            }
        }
        return taskStatuses;
    }

    @Override
    public Set<Protos.TaskStatus> getTaskStatuses() throws StateStoreException {
        return new HashSet<>(fetchStatuses());
    }

    @Override
    public Optional<Protos.TaskStatus> fetchStatus(String taskName) throws StateStoreException {
        String path = taskPathMapper.getTaskStatusPath(taskName);
        logger.debug("Fetching status for '{}' in '{}'", taskName, path);
        try {
            byte[] bytes = curator.fetch(path);
            if (bytes.length > 0) {
                return Optional.of(Protos.TaskStatus.parseFrom(bytes));
            } else {
                throw new StateStoreException(String.format(
                        "Failed to retrieve TaskStatus for TaskName: %s", taskName));
            }
        } catch (KeeperException.NoNodeException e) {
            logger.warn("No TaskInfo found for the requested name: " + taskName + " at: " + path);
            return Optional.empty();
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public void storeProperty(final String key, final byte[] value) throws StateStoreException {
        StateStoreUtils.validateKey(key);
        StateStoreUtils.validateValue(value);
        try {
            final String path = CuratorUtils.join(this.propertiesPath, key);
            logger.debug("Storing property key: {} into path: {}", key, path);
            curator.store(path, value);
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public byte[] fetchProperty(final String key) throws StateStoreException {
        StateStoreUtils.validateKey(key);
        try {
            final String path = CuratorUtils.join(this.propertiesPath, key);
            logger.debug("Fetching property key: {} from path: {}", key, path);
            return curator.fetch(path);
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public Collection<String> fetchPropertyKeys() throws StateStoreException {
        try {
            return curator.getChildren(this.propertiesPath);
        } catch (KeeperException.NoNodeException e) {
            // Root path doesn't exist yet. Treat as an empty list of properties. This scenario is
            // expected to commonly occur when the Framework is being run for the first time.
            return Collections.emptyList();
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    @Override
    public void clearProperty(final String key) throws StateStoreException {
        StateStoreUtils.validateKey(key);
        try {
            final String path = CuratorUtils.join(this.propertiesPath, key);
            logger.debug("Removing property key: {} from path: {}", key, path);
            curator.clear(path);
        } catch (KeeperException.NoNodeException e) {
            // Clearing a non-existent Property should not result in an exception from us.
            logger.warn("Cleared nonexistent Property, continuing silently: {}", key, e);
            return;
        } catch (Exception e) {
            throw new StateStoreException(e);
        }
    }

    void close() {
        curator.close();
    }

    // Internals

    private static class TaskPathMapper {
        private final String tasksRootPath;

        private TaskPathMapper(String rootPath) {
            this.tasksRootPath = CuratorUtils.join(rootPath, TASKS_ROOT_NAME);
        }

        private String getTaskInfoPath(String taskName) {
            return CuratorUtils.join(getTaskPath(taskName), TASK_INFO_PATH_NAME);
        }

        private String getTaskStatusPath(String taskName) {
            return CuratorUtils.join(getTaskPath(taskName), TASK_STATUS_PATH_NAME);
        }

        private String getTaskPath(String taskName) {
            return CuratorUtils.join(getTasksRootPath(), taskName);
        }

        private String getTasksRootPath() {
            return tasksRootPath;
        }
    }
}
