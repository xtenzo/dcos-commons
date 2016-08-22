package org.apache.mesos.scheduler.plan;

import org.apache.mesos.config.Configuration;

/**
 * Created by gabriel on 8/21/16.
 */
public interface ConfigurationUpdateDetector {
    boolean isRelevantUpdate(
            TaskSpecificationTemplate taskSpecificationTemplate,
            Configuration oldConfiguration,
            Configuration newConfiguration);
}
