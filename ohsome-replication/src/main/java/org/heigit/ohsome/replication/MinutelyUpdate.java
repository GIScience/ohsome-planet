package org.heigit.ohsome.replication;

import org.heigit.ohsome.replication.state.ChangesetStateManager;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "minutely",
        mixinStandardHelpOptions = true,
        version = "ohsome-planet minutely update 1.0.1",
        description = "generates minutely parquet files starting from the last planet pbf")
public class MinutelyUpdate implements Callable<Integer> {


    @Override
    public Integer call() {
        var changesetState = new ChangesetStateManager();
        return 0;
    }

}
