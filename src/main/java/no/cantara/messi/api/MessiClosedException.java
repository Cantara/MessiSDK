package no.cantara.messi.api;

public class MessiClosedException extends RuntimeException {
    public MessiClosedException() {
    }

    public MessiClosedException(String message) {
        super(message);
    }

    public MessiClosedException(String message, Throwable cause) {
        super(message, cause);
    }

    public MessiClosedException(Throwable cause) {
        super(cause);
    }

    public MessiClosedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
