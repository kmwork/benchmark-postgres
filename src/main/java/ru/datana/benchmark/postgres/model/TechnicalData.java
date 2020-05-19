package ru.datana.benchmark.postgres.model;

import lombok.Builder;
import lombok.Getter;

import java.sql.Timestamp;
import java.util.UUID;

@Builder
@Getter
public class TechnicalData {
    private UUID requestId;
    private UUID controllerId;
    private UUID taskId;
    private Timestamp requestDatetime;
    private Timestamp requestDatetimeProxy;
    private Timestamp responseDatetime;
}
