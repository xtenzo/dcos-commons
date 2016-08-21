package org.apache.mesos.config;

import org.apache.commons.collections.IteratorUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by gabriel on 8/20/16.
 */
public class EnvironmentConfiguration implements Configuration {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final Map<String, String> environment;

    public EnvironmentConfiguration() {
        this(System.getenv().keySet());
    }

    public EnvironmentConfiguration(Set<String> keys) {
        this.environment = new HashMap<>();

        for (String key : keys) {
            String value = System.getenv(key);
            if (value != null) {
                environment.put(key, value);
            }
        }
    }

    @Override
    public byte[] getBytes() throws ConfigStoreException {
        try {
            return toJsonString().getBytes();
        } catch (Exception e) {
            logger.error("Failed to get bytes with exception: ", e);
            throw new ConfigStoreException(e);
        }
    }

    @Override
    public String toJsonString() throws Exception {
        return new JSONObject(environment).toString();
    }

    /**
     * Factory which performs the inverse of {@link EnvironmentConfiguration#getBytes()}.
     *
     */
    public static class Factory implements ConfigurationFactory<EnvironmentConfiguration> {
        @Override
        public EnvironmentConfiguration parse(byte[] bytes) throws ConfigStoreException {
            List<String> keys = IteratorUtils.toList(new JSONObject(bytes).keys());
            return new EnvironmentConfiguration(new HashSet<>(keys));
        }
    }
}
