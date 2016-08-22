package org.apache.mesos.scheduler.plan;

import java.util.List;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultServiceSpecification {
    private final List<TaskSpecificationTemplate> taskSpecificationTemplates;

    public DefaultServiceSpecification(List<TaskSpecificationTemplate> taskSpecificationTemplates) {
        this.taskSpecificationTemplates = taskSpecificationTemplates;
    }

    public List<TaskSpecificationTemplate> getTaskSpecificationTempaltes() {
        return taskSpecificationTemplates;
    }
}
