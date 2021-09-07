package no.cantara.messi.api;

import java.util.Set;

public interface MessiMetadataClient {

    /**
     * @return the name of the topic from which this metadata-client will read from and write to.
     */
    String topic();

    /**
     * @return an iterable with the keys of all the metadata entries stored for this topic.
     */
    Set<String> keys();

    /**
     * Read value for a given key.
     *
     * @param key the key of the metadata entry.
     * @return the value of the metadata entry.
     */
    byte[] get(String key);

    /**
     * Write metadata value for a given key.
     *
     * @param key   the key of the metadata entry
     * @param value the value of the metadata entry
     * @return this metadata-client.
     */
    MessiMetadataClient put(String key, byte[] value);

    /**
     * Remove the metadata entry associated with the given key.
     *
     * @param key the key of the metadata entry to remove
     * @return this metadata-client.
     */
    MessiMetadataClient remove(String key);
}
