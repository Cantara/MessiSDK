package no.cantara.messi.api;

public enum MessiCursorStartingPointType {
    OLDEST_RETAINED,
    NOW,
    AT_ULID,
    AT_EXTERNAL_ID,
    AT_PROVIDER_TIME,
    AT_PROVIDER_SEQUENCE
}
