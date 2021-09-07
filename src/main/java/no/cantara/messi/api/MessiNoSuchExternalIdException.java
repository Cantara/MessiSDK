package no.cantara.messi.api;

public class MessiNoSuchExternalIdException extends RuntimeException {
    public MessiNoSuchExternalIdException() {
    }

    public MessiNoSuchExternalIdException(String message) {
        super(message);
    }

    public MessiNoSuchExternalIdException(String message, Throwable cause) {
        super(message, cause);
    }

    public MessiNoSuchExternalIdException(Throwable cause) {
        super(cause);
    }

    public MessiNoSuchExternalIdException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
