package no.cantara.messi.api;

public class MessiNotCompatibleCursorException extends RuntimeException {

    public MessiNotCompatibleCursorException() {
    }

    public MessiNotCompatibleCursorException(String message) {
        super(message);
    }

    public MessiNotCompatibleCursorException(String message, Throwable cause) {
        super(message, cause);
    }

    public MessiNotCompatibleCursorException(Throwable cause) {
        super(cause);
    }

    public MessiNotCompatibleCursorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
