package org.apache.mesos.offer.constrain;

/**
 * An exception for the case where a {@link PlacementRuleGenerator} is unable to find any valid
 * location to launch a task. In this situation, deployment is effectively stuck. The caller may
 * retry the request after launched tasks have changed.
 */
public class StuckDeploymentException extends Exception {

    public StuckDeploymentException() {
        super();
    }

    public StuckDeploymentException(String string) {
        super(string);
    }

    public StuckDeploymentException(String string, Throwable throwable) {
        super(string, throwable);
    }

    public StuckDeploymentException(Throwable throwable) {
        super(throwable);
    }
}
