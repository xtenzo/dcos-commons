package org.apache.mesos.scheduler;

/**
 * This class provides utilities common to the construction and operation of Mesos Schedulers.
 */
public class SchedulerUtils {
    private static final String ROLE_SUFFIX = "-role";
    private static final String PRINCIPAL_SUFFIX = "-principal";

    public static String nameToRole(String frameworkName) {
        return frameworkName + ROLE_SUFFIX;
    }

    public static String nameToPrincipal(String frameworkName) {
        return frameworkName + PRINCIPAL_SUFFIX;
    }
}
