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
}
