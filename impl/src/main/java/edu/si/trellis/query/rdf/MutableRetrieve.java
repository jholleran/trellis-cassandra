package edu.si.trellis.query.rdf;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;

import edu.si.trellis.MutableReadConsistency;

import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

/**
 * A query to retrieve mutable data about a resource from Cassandra.
 */
public class MutableRetrieve extends ResourceQuery {

    @Inject
    public MutableRetrieve(CqlSession session, @MutableReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT quads FROM " + MUTABLE_TABLENAME + " WHERE identifier = :identifier;", consistency);
    }

    /**
     * @param id the {@link IRI} of the resource, the mutable data of which is to be retrieved
     * @return the RDF retrieved
     */
    public Stream<Quad> execute(IRI id) {
        return quads(preparedStatement().bind().set("identifier", id, IRI.class));
    }
}
