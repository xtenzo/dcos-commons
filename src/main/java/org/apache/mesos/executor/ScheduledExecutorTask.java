package org.apache.mesos.executor;

import java.time.Duration;

/**
 * A Task which can be run periodically on an Executor.
 */
public interface ScheduledExecutorTask extends Runnable {
    Duration getDelay();
    Duration getInterval();
    void stop();
}
