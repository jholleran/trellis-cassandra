package edu.si.trellis;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import edu.si.trellis.ByteBufferInputStream;
import javax.imageio.ImageIO;

import boofcv.abst.fiducial.QrCodeDetector;
import boofcv.alg.fiducial.qrcode.QrCode;
import boofcv.factory.fiducial.FactoryFiducial;
import boofcv.io.image.ConvertBufferedImage;
import boofcv.struct.image.GrayU8;

/**
 * Defines a simple {@code static} function for use as a Cassandra UDF applied to single-chunk binary image data.
 * 
 * @see QrCodeDetector
 */
public class QrCodeExtractor {

    /**
     * Used only to select {@link ConvertBufferedImage#convertFrom(BufferedImage, GrayU8)} from all
     * {@link ConvertBufferedImage#convertFrom} signatures.
     */
    private static final GrayU8 GRAYU8 = null;

    /**
     * @param image a {@link ByteBuffer} containing an image
     * @return the first {@link QrCode} detected or {@code null} if none is detected
     */
    public static QrCode process(ByteBuffer image) {

        QrCodeDetector<GrayU8> detector = FactoryFiducial.qrcode(null, GrayU8.class);

        try (ByteBufferInputStream imageStream = new ByteBufferInputStream(image)) {
            BufferedImage img = ImageIO.read(imageStream);
            GrayU8 gray = ConvertBufferedImage.convertFrom(img, GRAYU8);
            detector.process(gray);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        List<QrCode> detections = detector.getDetections();
        return detections.isEmpty() ? null : detections.get(0);
    }
}
