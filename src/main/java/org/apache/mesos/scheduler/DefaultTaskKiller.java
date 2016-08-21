package org.apache.mesos.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.scheduler.recovery.TaskFailureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultTaskKiller implements TaskKiller {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final TaskFailureListener taskFailureListener;

    private final Integer restartLock = 0;
    private List<Protos.TaskInfo> tasksToRestart = new ArrayList<>();
    private final Integer rescheduleLock = 0;
    private List<Protos.TaskInfo> tasksToReschedule = new ArrayList<>();

    public DefaultTaskKiller(TaskFailureListener taskFailureListener) {
        this.taskFailureListener = taskFailureListener;
    }

    @Override
    public void restartTask(Protos.TaskInfo taskInfo) {
        synchronized (restartLock) {
            tasksToRestart.add(taskInfo);
        }
    }

    @Override
    public void replaceTask(Protos.TaskInfo taskInfo) {
        synchronized (rescheduleLock) {
            tasksToReschedule.add(taskInfo);
        }
    }

    @Override
    public void processTaskKills(SchedulerDriver driver) {
        processTasksToRestart(driver);
        processTasksToReschedule(driver);
    }

    private void processTasksToRestart(SchedulerDriver driver) {
        synchronized (restartLock) {
            for (Protos.TaskInfo taskInfo : tasksToRestart) {
                if (taskInfo != null) {
                    logger.info("Restarting task: " + taskInfo.getTaskId().getValue());
                    driver.killTask(taskInfo.getTaskId());
                } else {
                    logger.warn("Asked to restart null task.");
                }
            }

            tasksToRestart = new ArrayList<>();
        }
    }

    private void processTasksToReschedule(SchedulerDriver driver) {
        synchronized (rescheduleLock) {
            for (Protos.TaskInfo taskInfo : tasksToReschedule) {
                if (taskInfo != null) {
                    logger.info("Rescheduling task: " + taskInfo.getTaskId().getValue());
                    taskFailureListener.taskFailed(taskInfo.getTaskId());
                    driver.killTask(taskInfo.getTaskId());
                } else {
                    logger.warn("Asked to reschedule null task.");
                }
            }

            tasksToReschedule = new ArrayList<>();
        }
    }
}