package org.apache.mesos.scheduler.plan;

import org.apache.mesos.Protos;

import java.util.Collection;

/**
 * Created by gabriel on 8/21/16.
 */
public interface TaskSpecificationTemplate {
    int getCount();
    String getName();
    Collection<Protos.Resource> getResources();
    Protos.CommandInfo getCommand();
}
