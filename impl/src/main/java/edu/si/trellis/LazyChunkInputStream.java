package edu.si.trellis;

import static java.util.Objects.requireNonNull;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import java.io.InputStream;

/**
 * A {@link LazyFilterInputStream} backed by a Cassandra query to retrieve one binary chunk.
 * <p>
 * Not thread-safe!
 * </p>
 * 
 * @see InputStreamCodec
 */
public class LazyChunkInputStream extends LazyFilterInputStream {

    private final Session session;

    private final Statement query;

    public LazyChunkInputStream(Session session, Statement query) {
        this.session = session;
        this.query = query;
    }

    @Override
    protected InputStream initialize() {
        Row row = requireNonNull(session.execute(query).one(), "Missing binary chunk!");
        return row.get("chunk", InputStream.class);
    }
}
