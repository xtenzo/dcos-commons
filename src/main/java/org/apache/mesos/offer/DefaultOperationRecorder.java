package org.apache.mesos.offer;

import com.google.protobuf.TextFormat;
import org.apache.mesos.Protos;
import org.apache.mesos.state.StateStore;
import org.apache.mesos.state.StateStoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultOperationRecorder implements OperationRecorder {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final StateStore stateStore;

    public DefaultOperationRecorder(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    @Override
    public void record(Protos.Offer.Operation operation, Protos.Offer offer) throws Exception {
        if (operation.getType() == Protos.Offer.Operation.Type.LAUNCH) {
            recordTasks(operation.getLaunch().getTaskInfosList());
        }
    }

    private void recordTasks(List<Protos.TaskInfo> taskInfos) {
        List<Protos.TaskStatus> taskStatuses = new ArrayList<>();
        logger.info(String.format("Recording %d updated TaskInfos/TaskStatuses:", taskInfos.size()));

        for (Protos.TaskInfo taskInfo : taskInfos) {
            Protos.TaskStatus taskStatus = Protos.TaskStatus.newBuilder()
                    .setTaskId(taskInfo.getTaskId())
                    .setExecutorId(taskInfo.getExecutor().getExecutorId())
                    .setState(Protos.TaskState.TASK_STAGING)
                    .build();
            logger.info(String.format("- %s => %s", taskInfo, taskStatus));
            logger.info("Marking stopped task as failed: {}", TextFormat.shortDebugString(taskInfo));
            taskStatuses.add(taskStatus);
        }

        stateStore.storeTasks(taskInfos);
        for (Protos.TaskStatus taskStatus : taskStatuses) {
            recordTaskStatus(taskStatus);
        }
    }

    private void recordTaskStatus(Protos.TaskStatus taskStatus) throws StateStoreException {
        if (!taskStatus.getState().equals(Protos.TaskState.TASK_STAGING) && !taskStatusExists(taskStatus)) {
            logger.warn("Dropping non-STAGING status update because the ZK path doesn't exist: "
                    + taskStatus);
        } else {
            stateStore.storeStatus(taskStatus);
        }
    }

    private boolean taskStatusExists(Protos.TaskStatus taskStatus) throws StateStoreException {
        String taskName;
        try {
            taskName = TaskUtils.toTaskName(taskStatus.getTaskId());
        } catch (TaskException e) {
            throw new StateStoreException(String.format(
                    "Failed to get TaskName/ExecName from TaskStatus %s", taskStatus), e);
        }
        try {
            stateStore.fetchStatus(taskName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
