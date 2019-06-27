package edu.si.trellis;

import static java.util.stream.Stream.concat;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.BinaryMetadata.builder;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.NonRDFSource;
import static org.trellisldp.vocabulary.LDP.PreferContainment;
import static org.trellisldp.vocabulary.LDP.contains;
import static org.trellisldp.vocabulary.LDP.getSuperclassOf;

import com.datastax.oss.driver.api.core.cql.Row;

import edu.si.trellis.query.rdf.BasicContainment;
import edu.si.trellis.query.rdf.ImmutableRetrieve;
import edu.si.trellis.query.rdf.MutableRetrieve;

import java.time.Instant;
import java.util.Optional;
import java.util.Spliterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDF;
import org.slf4j.Logger;
import org.trellisldp.api.BinaryMetadata;
import org.trellisldp.api.Resource;
import org.trellisldp.api.TrellisUtils;

class CassandraResource implements Resource {

    private static final Logger log = getLogger(CassandraResource.class);

    private final IRI identifier, container, interactionModel;

    private final boolean hasAcl, isContainer;

    private final Instant modified;

    private final BinaryMetadata binary;

    private final ImmutableRetrieve immutable;

    private final MutableRetrieve mutable;

    private final BasicContainment bcontainment;

    private static final RDF rdfFactory = TrellisUtils.getInstance();

    public CassandraResource(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryIdentifier, String mimeType, IRI container,
                    Instant modified, ImmutableRetrieve immutable, MutableRetrieve mutable,
                    BasicContainment bcontainment) {
        this.identifier = id;
        this.interactionModel = ixnModel;
        this.isContainer = Container.equals(getInteractionModel())
                        || Container.equals(getSuperclassOf(getInteractionModel()));
        this.hasAcl = hasAcl;
        this.container = container;
        log.trace("Resource is {}a container.", !isContainer ? "not " : "");
        this.modified = modified;
        boolean isBinary = NonRDFSource.equals(getInteractionModel());
        this.binary = isBinary ? builder(binaryIdentifier).mimeType(mimeType).build() : null;
        log.trace("Resource is {}a NonRDFSource.", !isBinary ? "not " : "");
        this.mutable = mutable;
        this.immutable = immutable;
        this.bcontainment = bcontainment;
    }

    @Override
    public IRI getIdentifier() {
        return identifier;
    }

    /**
     * @return a container for this resource
     */
    @Override
    public Optional<IRI> getContainer() {
        return Optional.ofNullable(container);
    }

    @Override
    public IRI getInteractionModel() {
        return interactionModel;
    }

    @Override
    public Instant getModified() {
        return modified;
    }

    @Override
    public boolean hasAcl() {
        return hasAcl;
    }

    @Override
    public Optional<BinaryMetadata> getBinaryMetadata() {
        return Optional.ofNullable(binary);
    }

    @Override
    public Stream<Quad> stream() {
        log.trace("Retrieving quad stream for resource {}", getIdentifier());
        Stream<Quad> quads = concat(mutableQuads(), immutableQuads());
        if (isContainer) {
            Stream<Quad> containmentQuads = basicContainmentQuads();
            quads = concat(quads, containmentQuads);
        }
        return quads;
    }

    /**
     * @return the quads stored via mutable data paths, either current or Memento
     */
    protected Stream<Quad> mutableQuads() {
        return mutable.execute(getIdentifier());
    }

    private Stream<Quad> immutableQuads() {
        return immutable.execute(getIdentifier());
    }

    protected Stream<Quad> basicContainmentQuads() {
        Spliterator<Row> rows = bcontainment.execute(getIdentifier()).spliterator();
        Stream<IRI> contained = StreamSupport.stream(rows, false).map(r -> r.get("contained", IRI.class));
        return contained.distinct().map(c -> rdfFactory.createQuad(PreferContainment, getIdentifier(), contains, c))
                        .peek(t -> log.trace("Built containment quad: {}", t));
    }
    
    /**
     * If there are mutable triples stored in the PreferContainment named graph, they will <i>not</i> be returned here.
     * Our assumption is that no user will intentionally use that URI as a name for a graph.
     *
     * @see org.trellisldp.api.Resource#stream(org.apache.commons.rdf.api.IRI)
     */
    @Override
    public Stream<Quad> stream(IRI graphName) {
        if (graphName.equals(PreferContainment)) return basicContainmentQuads();
        return Resource.super.stream(graphName);
    }
}
