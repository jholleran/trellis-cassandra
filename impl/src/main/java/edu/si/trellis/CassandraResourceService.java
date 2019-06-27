package edu.si.trellis;

import static java.time.Instant.now;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;
import static java.util.UUID.randomUUID;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.Metadata.builder;
import static org.trellisldp.api.Resource.SpecialResources.MISSING_RESOURCE;
import static org.trellisldp.api.TrellisUtils.TRELLIS_DATA_PREFIX;
import static org.trellisldp.vocabulary.LDP.BasicContainer;
import static org.trellisldp.vocabulary.LDP.Container;
import static org.trellisldp.vocabulary.LDP.NonRDFSource;
import static org.trellisldp.vocabulary.LDP.RDFSource;

import com.datastax.oss.driver.api.core.uuid.Uuids;

import edu.si.trellis.query.rdf.BasicContainment;
import edu.si.trellis.query.rdf.Delete;
import edu.si.trellis.query.rdf.Get;
import edu.si.trellis.query.rdf.ImmutableInsert;
import edu.si.trellis.query.rdf.ImmutableRetrieve;
import edu.si.trellis.query.rdf.MutableInsert;
import edu.si.trellis.query.rdf.MutableRetrieve;
import edu.si.trellis.query.rdf.Touch;

import java.time.Instant;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.rdf.api.Dataset;
import org.apache.commons.rdf.api.IRI;
import org.slf4j.Logger;
import org.trellisldp.api.BinaryMetadata;
import org.trellisldp.api.Metadata;
import org.trellisldp.api.Resource;
import org.trellisldp.api.ResourceService;
import org.trellisldp.api.RuntimeTrellisException;
import org.trellisldp.api.TrellisUtils;
import org.trellisldp.vocabulary.LDP;

/**
 * Implements persistence into a simple Apache Cassandra schema.
 *
 * @author ajs6f
 *
 */
public class CassandraResourceService extends CassandraBuildingService implements ResourceService {

    private static final Set<IRI> SUPPORTED_INTERACTION_MODELS;

    static {
        Set<IRI> models = new HashSet<>();
        models.addAll(asList(LDP.Resource, RDFSource, NonRDFSource, Container, BasicContainer));
        SUPPORTED_INTERACTION_MODELS = unmodifiableSet(models);
    }

    static final Logger log = getLogger(CassandraResourceService.class);

    private final Delete delete;

    private final Get get;

    private final ImmutableInsert immutableInsert;

    private final MutableInsert mutableInsert;

    private final Touch touch;

    private final BasicContainment bcontainment;

    private final MutableRetrieve mutableRetrieve;

    private final ImmutableRetrieve immutableRetrieve;

    /**
     * Constructor.
     * 
     * @param delete {@link Delete} query to use to delete resources
     * @param get {@link Get} query to support retrieving content
     * @param immutableInsert {@link ImmutableInsert} query to support storing immutable data
     * @param mutableInsert {@link MutableInsert} query to support storing mutable data
     * @param touch {@link Touch} query to support updating the value of {@link Resource#getModified()}
     * @param mutableRetrieve {@link MutableRetrieve} to support retrieving content
     * @param immutableRetrieve {@link ImmutableRetrieve} to support retrieving content
     * @param bcontainment {@link BasicContainment} to support retrieving content
     */
    @Inject
    public CassandraResourceService(Delete delete, Get get, ImmutableInsert immutableInsert,
                    MutableInsert mutableInsert, Touch touch, MutableRetrieve mutableRetrieve,
                    ImmutableRetrieve immutableRetrieve, BasicContainment bcontainment) {
        this.delete = delete;
        this.get = get;
        this.immutableInsert = immutableInsert;
        this.mutableInsert = mutableInsert;
        this.touch = touch;
        this.mutableRetrieve = mutableRetrieve;
        this.immutableRetrieve = immutableRetrieve;
        this.bcontainment = bcontainment;

    }

    /**
     * Build a root container.
     */
    @PostConstruct
    void initializeRoot() {

        IRI rootIri = TrellisUtils.getInstance().createIRI(TRELLIS_DATA_PREFIX);
        try {
            if (get(rootIri).toCompletableFuture().get().equals(MISSING_RESOURCE)) {
                Metadata rootResource = builder(rootIri).interactionModel(BasicContainer).build();
                create(rootResource, null).toCompletableFuture().get();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InterruptedStartupException("Interrupted while building repository root!", e);
        } catch (ExecutionException e) {
            throw new RuntimeTrellisException(e);
        }
    }

    @Override
    public CompletionStage<? extends Resource> get(final IRI id) {
        return get.execute(id).thenApply(rows -> parse(rows, log, id));
    }

    @Override
    public String generateIdentifier() {
        return randomUUID().toString();
    }

    @Override
    public CompletionStage<Void> add(final IRI id, final Dataset dataset) {
        log.debug("Adding immutable data to {}", id);
        return immutableInsert.execute(id, dataset, now());
    }

    @Override
    public CompletionStage<Void> create(Metadata meta, Dataset data) {
        log.debug("Creating {} with interaction model {}", meta.getIdentifier(), meta.getInteractionModel());
        return write(meta, data);
    }

    @Override
    public CompletionStage<Void> replace(Metadata meta, Dataset data) {
        log.debug("Replacing {} with interaction model {}", meta.getIdentifier(), meta.getInteractionModel());
        return write(meta, data);
    }

    @Override
    public CompletionStage<Void> delete(Metadata meta) {
        log.debug("Deleting {}", meta.getIdentifier());
        return delete.execute(meta.getIdentifier());
    }

    /*
     * (non-Javadoc) TODO avoid read-modify-write?
     */
    @Override
    public CompletionStage<Void> touch(IRI id) {
        return touch.execute(now(), id);
    }

    @Override
    public Set<IRI> supportedInteractionModels() {
        return SUPPORTED_INTERACTION_MODELS;
    }

    private CompletionStage<Void> write(Metadata meta, Dataset data) {
        IRI id = meta.getIdentifier();
        IRI ixnModel = meta.getInteractionModel();
        IRI container = meta.getContainer().orElse(null);

        Optional<BinaryMetadata> binary = meta.getBinary();
        IRI binaryIdentifier = binary.map(BinaryMetadata::getIdentifier).orElse(null);
        String mimeType = binary.flatMap(BinaryMetadata::getMimeType).orElse(null);
        Instant now = now();

        return mutableInsert.execute(ixnModel, mimeType, container, data, now, binaryIdentifier, Uuids.timeBased(), id);
    }

    @Override
    Resource construct(IRI id, IRI ixnModel, boolean hasAcl, IRI binaryId, String mimeType, IRI container,
                    Instant modified) {
        return new CassandraResource(id, ixnModel, hasAcl, binaryId, mimeType, container, modified, immutableRetrieve,
                        mutableRetrieve, bcontainment);
    }
}
