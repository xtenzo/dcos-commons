package org.apache.mesos.scheduler.plan;

import org.apache.mesos.Protos;

import java.util.Collection;

/**
 * Created by gabriel on 8/20/16.
 */
public class DefaultTaskSpecification implements TaskSpecification {
    private final String name;
    private final Collection<Protos.Resource> resources;
    private final Protos.CommandInfo commandInfo;

    public DefaultTaskSpecification(
            String name,
            Collection<Protos.Resource> resources,
            Protos.CommandInfo commandInfo) {
        this.name = name;
        this.resources = resources;
        this.commandInfo = commandInfo;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Collection<Protos.Resource> getResources() {
        return resources;
    }

    @Override
    public Protos.CommandInfo getCommand() {
        return commandInfo;
    }
}
