package org.apache.mesos.scheduler.plan;

import org.apache.mesos.config.Configuration;

/**
 * Created by gabriel on 8/21/16.
 */
public class DefaultConfigurationUpdateDetector implements ConfigurationUpdateDetector {
    @Override
    public boolean isRelevantUpdate(
            TaskSpecificationTemplate taskSpecificationTemplate,
            Configuration oldConfiguration,
            Configuration newConfiguration) {
        return true;
    }
}
