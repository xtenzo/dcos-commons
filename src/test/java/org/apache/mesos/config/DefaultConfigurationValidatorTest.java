package org.apache.mesos.config;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class DefaultConfigurationValidatorTest {
    public static class TestConfig implements Configuration {
        private int a;

        public TestConfig(int a) {
            this.a = a;
        }

        public int getA() {
            return a;
        }

        @Override
        public byte[] getBytes() throws ConfigStoreException {
            return new byte[0];
        }

        @Override
        public String toJsonString() throws Exception {
            return null;
        }
    }

    ConfigurationValidation test = (oldConfig, newConfig) -> {
        final Optional<TestConfig> oConfig = oldConfig.map(c -> (TestConfig) c);
        final TestConfig nConfig = (TestConfig) newConfig;

        if (oConfig.get().getA() != nConfig.getA()) {
            return Arrays.asList(new ConfigurationValidationError("a", "" + nConfig.getA(), "not equal"));
        }

        return Collections.emptyList();
    };

    @Test
    public void testZeroValidations() {
        final TestConfig oldConfig = new TestConfig(1);
        final TestConfig newConfig = new TestConfig(2);

        final DefaultConfigurationValidator configurationValidator = new DefaultConfigurationValidator();
        final Collection<ConfigurationValidationError> validate =
                configurationValidator.validate(Optional.of(oldConfig), newConfig);

        Assert.assertNotNull(validate);
        Assert.assertTrue(validate.size() == 0);
    }

    @Test
    public void testNonZeroValidationsNoError() {
        final TestConfig oldConfig = new TestConfig(1);
        final TestConfig newConfig = new TestConfig(1);

        final DefaultConfigurationValidator configurationValidator
                = new DefaultConfigurationValidator(Arrays.asList(test));
        final Collection<ConfigurationValidationError> validate =
                configurationValidator.validate(Optional.of(oldConfig), newConfig);

        Assert.assertNotNull(validate);
        Assert.assertTrue(validate.size() == 0);
    }

    @Test
    public void testNonZeroValidationsSingleError() {
        final TestConfig oldConfig = new TestConfig(1);
        final TestConfig newConfig = new TestConfig(2);

        final DefaultConfigurationValidator configurationValidator
                = new DefaultConfigurationValidator(Arrays.asList(test));
        final Collection<ConfigurationValidationError> validate =
                configurationValidator.validate(Optional.of(oldConfig), newConfig);

        Assert.assertNotNull(validate);
        Assert.assertTrue(validate.size() == 1);
    }
}
