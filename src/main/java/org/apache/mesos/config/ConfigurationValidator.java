package org.apache.mesos.config;

import java.util.Collection;
import java.util.Optional;

/**
 * Created by gabriel on 8/20/16.
 */
public interface ConfigurationValidator {
    Collection<ConfigurationValidationError> validate(Optional<Configuration> oldConfig, Configuration newConfig);
}
