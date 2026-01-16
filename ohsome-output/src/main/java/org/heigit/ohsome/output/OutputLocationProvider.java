package org.heigit.ohsome.output;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ServiceLoader;

public interface OutputLocationProvider {
    Logger logger = LoggerFactory.getLogger(OutputLocationProvider.class);

    static OutputLocation load(String path) throws Exception {
        logger.debug("Loading output location at {}", path);
        var loader = ServiceLoader.load(OutputLocationProvider.class);
        for (OutputLocationProvider provider : loader) {
            if (provider.accepts(path)) {
                logger.debug("Found output location provider {} for {}", provider.getClass(), path);
                return provider.open(path);
            }
        }
        logger.debug("No output location provider found for {} using default {}", path, LocalOutputLocation.class.getName());
        return new LocalOutputLocation(Path.of(path));
    }

    boolean accepts(String path);

    OutputLocation open(String path) throws Exception;

}
