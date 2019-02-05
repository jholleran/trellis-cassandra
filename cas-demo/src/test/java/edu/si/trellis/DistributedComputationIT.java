package edu.si.trellis;

import static org.apache.http.HttpStatus.SC_CREATED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.slf4j.LoggerFactory.getLogger;

import com.github.jsonldjava.shaded.com.google.common.collect.ImmutableList;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.simple.SimpleRDF;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;

/**
 * Demonstrates a simple CAS workflow.
 *
 */
public class DistributedComputationIT {

    private static final RDF factory = new SimpleRDF();

    private static final Logger log = getLogger(DistributedComputationIT.class);

    private static final CloseableHttpClient client = HttpClients.createMinimal();

    private static final Integer port = Integer.getInteger("trellis.port");

    private static final String trellisUri = "http://localhost:" + port + "/";

    private static final List<String> images = ImmutableList.of();

    private static final String FUNCTION_DEFINITION = "CREATE OR REPLACE FUNCTION trellis.extractQR (image blob) "
                    + "RETURNS NULL ON NULL INPUT " + "RETURNS text " + "LANGUAGE java AS "
                    + "'clone() ; return edu.si.trellis.QrCodeExtractor.process(image);';";

    @RegisterExtension
    protected static CassandraConnection connection = new CassandraConnection();

    @BeforeAll
    static void load() {
        images.forEach(DistributedComputationIT::loadOne);
        connection.session.execute(FUNCTION_DEFINITION);
    }

    private static String loadOne(String slug) {
        log.debug("Using Slug {} to add data.", slug);
        HttpPost req = new HttpPost(trellisUri);
        req.setHeader("Slug", slug);
        req.setHeader("Content-Type", "image/jpeg");
        try (InputStream image = new FileInputStream("src/test/resources/" + slug + ".jpg")) {
            req.setEntity(new InputStreamEntity(image));
            try (CloseableHttpResponse res = client.execute(req); InputStream url = res.getEntity().getContent()) {
                assertEquals(SC_CREATED, res.getStatusLine().getStatusCode());
                return res.getFirstHeader("Location").getValue();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    void tryOne() {
        IRI binaryId = idForBinary(factory.createIRI("trellis://cat.jpg"));
        String qr = connection.session
                        .execute("SELECT extractQR(chunk) AS qr FROM binarydata WHERE id=" + binaryId + ";").one()
                        .getString("qr");
        log.info("Found QR code: {}", qr);
    }

    private IRI idForBinary(IRI resource) {
        return connection.session.execute(
                        "SELECT binaryIdentifier FROM mutabledata WHERE identifier=" + resource.getIRIString() + ";")
                        .one().get("binary", IRI.class);
    }

}
