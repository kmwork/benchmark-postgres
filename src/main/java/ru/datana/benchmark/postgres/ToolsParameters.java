package ru.datana.benchmark.postgres;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class ToolsParameters {
    public static final int ONE_BY_ONE_FILLING = 1;
    public static final int UNLIMITED_PACKAGES = 0;
    public static final boolean DISABLE_RECREATING = false;

    private String host;
    private Integer port;
    private String login;
    private String password;
    private String schema;
    private int packageSize;
    private int numberOfPackages;
    private int numberOfSensors;
    private boolean forceRecreate;

    public enum ColumnMode {
        SINGLE, MULTI
    }

    @SuppressWarnings("WeakerAccess")
    public static ToolsParameters parseArgs(String... args) {
        ToolsParameters parameters = new ToolsParameters();
        //default parameters
        parameters.setPackageSize(ONE_BY_ONE_FILLING);
        parameters.setNumberOfPackages(UNLIMITED_PACKAGES);
        parameters.setForceRecreate(DISABLE_RECREATING);
        Arrays.stream(args).forEach(arg -> {
            String[] split = arg.split("=");
            switch (split[0]) {
                case "-h":
                    parameters.setHost(split[1]);
                    break;
                case "-p":
                    parameters.setPort(Integer.valueOf(split[1]));
                    break;
                case "-l":
                    parameters.setLogin(split[1]);
                    break;
                case "-w":
                    parameters.setPassword(split[1]);
                    break;
                case "-k":
                    parameters.setSchema(split[1]);
                    break;
                case "-s":
                    parameters.setPackageSize(Integer.valueOf(split[1]));
                    break;
                case "-n":
                    parameters.setNumberOfPackages(Integer.valueOf(split[1]));
                    break;
                case "-c":
                    parameters.setNumberOfSensors(Integer.valueOf(split[1]));
                    break;
                case "-r":
                    parameters.setForceRecreate(Boolean.valueOf(split[1]));
                    break;
            }
        });
        return parameters;
    }
}
