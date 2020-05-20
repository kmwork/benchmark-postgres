package ru.datana.benchmark.postgres.model;

import lombok.Builder;
import lombok.Getter;

import java.sql.Timestamp;

@Builder
@Getter
public class TechnicalData {
    private long requestId;
    private long controllerId;
    private long taskId;
    private Timestamp requestDatetime;
    private Timestamp requestDatetimeProxy;
    private Timestamp responseDatetime;
}
