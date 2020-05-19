package ru.datana.benchmark.postgres;

import lombok.extern.slf4j.Slf4j;
import ru.datana.benchmark.postgres.filler.MultiSensorToRowFiller;
import ru.datana.benchmark.postgres.filler.SingleSensorToRowFiller;

@Slf4j
public class PostgreSqlTools {
    public static void main(String... args) {
        try {
            ToolsParameters parameters = ToolsParameters.parseArgs(args);

            switch (parameters.getMode()) {
                case SINGLE:
                    SingleSensorToRowFiller single = new SingleSensorToRowFiller(parameters);
                    single.fillDatabase();
                    break;
                case MULTI:
                    MultiSensorToRowFiller multi = new MultiSensorToRowFiller(parameters);
                    multi.fillDatabase();
                    break;
            }
        }catch (Exception e){
            log.error("Ошибка в программе", e);
        }
    }
}
