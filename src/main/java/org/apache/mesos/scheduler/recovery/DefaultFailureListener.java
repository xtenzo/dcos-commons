package org.apache.mesos.scheduler.recovery;

import org.apache.mesos.Protos;
import org.apache.mesos.offer.TaskException;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.state.StateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultFailureListener implements TaskFailureListener {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final StateStore stateStore;

    public DefaultFailureListener(StateStore stateStore) {
        this.stateStore = stateStore;
    }

    @Override
    public void taskFailed(Protos.TaskID taskId) {
        try {
            Protos.TaskInfo taskInfo = stateStore.fetchTask(TaskUtils.toTaskName(taskId));
            taskInfo = FailureUtils.markFailed(taskInfo);
            stateStore.storeTasks(Arrays.asList(taskInfo));
        } catch (TaskException e) {
            logger.error("Failed to fetch Task for taskId: " + taskId + " with exception:", e);
        }
    }
}
