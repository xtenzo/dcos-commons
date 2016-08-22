package org.apache.mesos.scheduler.plan;

import org.apache.mesos.Protos;

import java.util.Collection;

/**
 * Created by gabriel on 8/20/16.
 */
public interface TaskSpecification {
    String getName();
    Collection<Protos.Resource> getResources();
    Protos.CommandInfo getCommand();
    TaskSpecificationMode getMode();

    /**
     * The modes of a TaskSpecification.  If a Task has never been launched it is NEW.  If a Configuration change
     * necessitates updating a Task it's mode is UPDATE.  If no change is needed the Task's mode is COMPLETE.
     */
    enum TaskSpecificationMode {
        NEW,
        UPDATE,
        COMPLETE
    }
}
