package org.apache.mesos.scheduler.plan;

import java.util.Map;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultServiceSpecification {
    private final Map<TaskSpecification, Integer> taskMap;

    public DefaultServiceSpecification(Map<TaskSpecification, Integer> taskMap) {
        this.taskMap = taskMap;
    }

    public Map<TaskSpecification, Integer> getTaskSpecificationMap() {
        return taskMap;
    }
}
