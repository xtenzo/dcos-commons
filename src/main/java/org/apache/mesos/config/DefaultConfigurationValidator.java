package org.apache.mesos.config;

import java.util.*;

/**
 * Default Configuration validator, which validates a given new {@code Configuration} w.r.t.
 * an old {@code Configuration}.
 */
public class DefaultConfigurationValidator implements ConfigurationValidator {
    private final Collection<ConfigurationValidation> validations = new ArrayList<>();

    public DefaultConfigurationValidator(final ConfigurationValidation... validations) {
        this(Arrays.asList(validations));
    }

    public DefaultConfigurationValidator(final Collection<ConfigurationValidation> validations) {
        if (validations != null && !validations.isEmpty()) {
            this.validations.addAll(validations);
        }
    }

    @Override
    public Collection<ConfigurationValidationError> validate(Optional<Configuration> oldConfig, Configuration newConfig) {
        final List<ConfigurationValidationError> errors = new ArrayList<>();
        for (ConfigurationValidation validation : validations) {
            errors.addAll(validation.validate(oldConfig, newConfig));
        }
        return errors;
    }
}
