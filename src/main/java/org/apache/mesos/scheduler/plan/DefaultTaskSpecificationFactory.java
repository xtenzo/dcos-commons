package org.apache.mesos.scheduler.plan;

import org.apache.mesos.Protos;
import org.apache.mesos.config.*;
import org.apache.mesos.offer.TaskException;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.state.StateStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Created by gabriel on 8/21/16.
 */
public class DefaultTaskSpecificationFactory implements TaskSpecificationFactory<EnvironmentConfiguration> {
    private StateStore stateStore;
    private ConfigStore configStore;
    private ConfigurationUpdateDetector configurationUpdateDetector;
    private TaskSpecificationTemplate taskSpecificationTemplate;
    private ConfigurationFactory<EnvironmentConfiguration> configurationFactory;

    @Override
    public List<TaskSpecification> create(
            TaskSpecificationTemplate taskSpecificationTemplate,
            ConfigurationUpdateDetector configurationUpdateDetector,
            ConfigurationFactory<EnvironmentConfiguration> configurationFactory,
            ConfigStore configStore,
            StateStore stateStore) throws TaskException, ConfigStoreException {
        this.taskSpecificationTemplate = taskSpecificationTemplate;
        this.configurationUpdateDetector = configurationUpdateDetector;
        this.configurationFactory = configurationFactory;
        this.configStore = configStore;
        this.stateStore = stateStore;

        return getTaskSpecifications();
    }

    private List<TaskSpecification> getTaskSpecifications() throws TaskException, ConfigStoreException {
        List<TaskSpecification> taskSpecifications = new ArrayList<>();

        for (int i = 0; i < taskSpecificationTemplate.getCount(); i++) {
            taskSpecifications.add(getTaskSpecification(taskSpecificationTemplate, i));
        }

        return taskSpecifications;
    }

    private TaskSpecification getTaskSpecification(TaskSpecificationTemplate taskSpecificationTemplate, int id)
            throws TaskException, ConfigStoreException {
        String name = getName(taskSpecificationTemplate, id);

        TaskSpecification.TaskSpecificationMode mode = getMode(name);

        return new DefaultTaskSpecification(
                name,
                taskSpecificationTemplate.getResources(),
                taskSpecificationTemplate.getCommand(),
                mode);
    }

    private String getName(TaskSpecificationTemplate taskSpecificationTemplate, int id) {
        return taskSpecificationTemplate.getName() + "-" + id;
    }

    private TaskSpecification.TaskSpecificationMode getMode(String name) throws TaskException, ConfigStoreException {
        Optional<Protos.TaskInfo> optionalTaskInfo = stateStore.fetchTask(name);
        if (!optionalTaskInfo.isPresent()) {
            return TaskSpecification.TaskSpecificationMode.NEW;
        }

        Protos.TaskInfo taskInfo = optionalTaskInfo.get();
        UUID oldConfigurationId = TaskUtils.getTargetConfiguration(taskInfo);
        EnvironmentConfiguration oldConfiguration =
                (EnvironmentConfiguration) configStore.fetch(oldConfigurationId, configurationFactory);

        if (configurationUpdateDetector.isRelevantUpdate(
                taskSpecificationTemplate,
                oldConfiguration,
                (EnvironmentConfiguration) configStore.getTargetConfig(configurationFactory).get())) {
            return TaskSpecification.TaskSpecificationMode.UPDATE;
        } else {
            return TaskSpecification.TaskSpecificationMode.COMPLETE;
        }
    }
}
