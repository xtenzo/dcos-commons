package org.apache.mesos.scheduler;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

/**
 * Created by gabriel on 8/20/16.
 */
public interface TaskKiller {
    void restartTask(Protos.TaskInfo taskInfo);
    void replaceTask(Protos.TaskInfo taskInfo);
    void processTaskKills(SchedulerDriver driver);
}
