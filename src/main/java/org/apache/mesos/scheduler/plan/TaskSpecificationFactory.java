package org.apache.mesos.scheduler.plan;

import org.apache.mesos.config.ConfigStore;
import org.apache.mesos.config.ConfigStoreException;
import org.apache.mesos.config.Configuration;
import org.apache.mesos.config.ConfigurationFactory;
import org.apache.mesos.offer.TaskException;
import org.apache.mesos.state.StateStore;

import java.util.List;

/**
 * Created by gabriel on 8/21/16.
 * @param <T> The type of Configuration object consumed when generating TaskSpecifications objects.
 */
public interface TaskSpecificationFactory<T extends Configuration> {
    List<TaskSpecification> create(
            TaskSpecificationTemplate taskSpecificationTemplate,
            ConfigurationUpdateDetector configurationUpdateDetector,
            ConfigurationFactory<T> configurationFactory,
            ConfigStore configStore,
            StateStore stateStore) throws TaskException, ConfigStoreException;
}
