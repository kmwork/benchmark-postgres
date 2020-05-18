package ru.datana.cassandra;

import ru.datana.cassandra.filler.MultiSensorToRowFiller;
import ru.datana.cassandra.filler.SingleSensorToRowFiller;

public class PostgreSqlTools {
    public static void main(String... args) {
        ToolsParameters parameters = ToolsParameters.parseArgs(args);
        switch (parameters.getMode()) {
            case SINGLE:
                new SingleSensorToRowFiller().fillDatabase(parameters);
                break;
            case MULTI:
                new MultiSensorToRowFiller().fillDatabase(parameters);
                break;
        }
    }
}
