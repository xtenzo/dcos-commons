package org.apache.mesos.api;

import org.eclipse.jetty.server.Server;
import org.glassfish.jersey.jetty.JettyHttpContainerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;

/**
 * Created by gabriel on 8/29/16.
 */
public class ApiServer {
    URI baseUri = UriBuilder.fromUri("http://localhost/").port(9998).build();
    ResourceConfig config = new ResourceConfig(ApiConfig.class);
    Server server = JettyHttpContainerFactory.createServer(baseUri, config);

    private static class ApiConfig extends ResourceConfig {
        public ApiConfig() {
            packages("org.apache.mesos.config.api;org.apache.mesos.state.api;org.apache.mesos.scheduler.plan.api");
        }
    }
}