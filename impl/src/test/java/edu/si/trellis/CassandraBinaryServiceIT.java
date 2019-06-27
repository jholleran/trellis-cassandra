package edu.si.trellis;

import static edu.si.trellis.CassandraBinaryService.CASSANDRA_CHUNK_HEADER_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.UUID.randomUUID;
import static org.apache.commons.io.IOUtils.contentEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.slf4j.LoggerFactory.getLogger;
import static org.trellisldp.api.BinaryMetadata.builder;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.trellisldp.api.Binary;
import org.trellisldp.api.RuntimeTrellisException;

class CassandraBinaryServiceIT extends CassandraServiceIT {

    private static final Logger log = getLogger(CassandraBinaryServiceIT.class);

    @Test
    void setAndGetSmallContent() throws IOException {
        IRI id = createIRI();
        log.debug("Using identifier: {} for testSetAndGetSmallContent", id);
        String content = "This is only a short test, but it has meaning";
        try (InputStream testInput = IOUtils.toInputStream(content, UTF_8)) {
            connection.binaryService.setContent(builder(id).build(), testInput).toCompletableFuture().join();
        }
        try (InputStream got = connection.binaryService.get(id).toCompletableFuture().join().getContent()
                        .toCompletableFuture().join()) {
            String reply = IOUtils.toString(got, UTF_8);
            assertEquals(content, reply);
        }

        try (InputStream got = connection.binaryService.get(id).toCompletableFuture().join().getContent(5, 11)
                        .toCompletableFuture().join()) {
            String reply = IOUtils.toString(got, UTF_8);
            assertEquals(content.subSequence(5, 12), reply);
        }
        connection.binaryService.purgeContent(id).toCompletableFuture().join();

        try {
            connection.binaryService.get(id).toCompletableFuture().get();
            fail();
        } catch (Exception e) {
            Throwable cause = e.getCause();
            assertTrue(cause instanceof NullPointerException);
            assertTrue(cause.getMessage().contains(id.getIRIString()));
        }
    }

    @Test
    void setAndGetMultiChunkContent() throws IOException {
        IRI id = createIRI();
        final String md5sum = "89c4b71c69f59cde963ce8aa9dbe1617";
        try (FileInputStream testData = new FileInputStream("src/test/resources/test.jpg")) {
            connection.binaryService.setContent(builder(id).build(), testData).toCompletableFuture().join();
        }

        CompletableFuture<Binary> got = connection.binaryService.get(id).toCompletableFuture();
        Binary binary = got.join();
        assertTrue(got.isDone());

        try (InputStream testData = new FileInputStream("src/test/resources/test.jpg");
             InputStream content = binary.getContent().toCompletableFuture().join()) {
            assertTrue(contentEquals(testData, content), "Didn't retrieve correct content!");
        }

        try (InputStream content = binary.getContent().toCompletableFuture().join()) {
            String digest = DigestUtils.md5Hex(content);
            assertEquals(md5sum, digest);
        }
    }

    @Test
    void varyChunkSizeFromDefault() throws IOException {
        IRI id = createIRI();
        final String chunkSize = "10000000";
        final String md5sum = "89c4b71c69f59cde963ce8aa9dbe1617";
        try (FileInputStream testData = new FileInputStream("src/test/resources/test.jpg")) {
            Map<String, List<String>> hints = singletonMap(CASSANDRA_CHUNK_HEADER_NAME, singletonList(chunkSize));
            connection.binaryService.setContent(builder(id).hints(hints).build(), testData).toCompletableFuture().join();
        }

        CompletableFuture<Binary> got = connection.binaryService.get(id).toCompletableFuture();
        Binary binary = got.join();
        assertTrue(got.isDone());

        try (InputStream testData = new FileInputStream("src/test/resources/test.jpg");
             InputStream content = binary.getContent().toCompletableFuture().join()) {
            assertTrue(contentEquals(testData, content), "Didn't retrieve correct content!");
        }

        try (InputStream content = binary.getContent().toCompletableFuture().join()) {
            String digest = DigestUtils.md5Hex(content);
            assertEquals(md5sum, digest);
        }

        try (FileInputStream testData = new FileInputStream("src/test/resources/test.jpg")) {
            Map<String, List<String>> hints = singletonMap(CASSANDRA_CHUNK_HEADER_NAME,
                            asList(chunkSize, chunkSize + 1000));
            try {
                connection.binaryService.setContent(builder(id).hints(hints).build(), testData).toCompletableFuture()
                                .join();
                fail();
            } catch (Exception e) {
                assertTrue(e instanceof RuntimeTrellisException);
                assertTrue(e.getMessage().contains(CASSANDRA_CHUNK_HEADER_NAME));
            }
        }
    }

    private IRI createIRI() {
        return rdfFactory.createIRI("http://example.com/" + randomUUID());
    }
}
