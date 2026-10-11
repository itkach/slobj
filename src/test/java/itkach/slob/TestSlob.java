package itkach.slob;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;
import java.text.Collator;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

public class TestSlob {

    @Test
    public void signedToUnsignedByteConversion() {
        assertEquals(0, Slob.toUnsignedByte((byte)0));
        assertEquals(127, Slob.toUnsignedByte((byte)127));
        assertEquals(255, Slob.toUnsignedByte((byte)-1));
        assertEquals(128, Slob.toUnsignedByte((byte)-128));
    }

    @Test
    public void signedToUnsignedShortConversion() {
        assertEquals(0, Slob.toUnsignedShort(new byte[]{0, 0}));
        assertEquals(Short.MAX_VALUE, Slob.toUnsignedShort(new byte[]{127, -1}));
        assertEquals((int)Short.MAX_VALUE + 1, Slob.toUnsignedShort(new byte[]{-128, 0}));
        assertEquals(2*Short.MAX_VALUE + 1, Slob.toUnsignedShort(new byte[]{-1, -1}));
    }

    @Test
    public void signedToUnsignedIntConversion() {
        assertEquals(0, Slob.toUnsignedInt(new byte[]{0, 0, 0, 0}, 0));
        assertEquals(0, Slob.toUnsignedInt(new byte[]{1, 0, 0, 0, 0}, 1));
        assertEquals(Integer.MAX_VALUE, Slob.toUnsignedInt(new byte[]{127, -1, -1, -1}, 0));
        assertEquals((long)Integer.MAX_VALUE + 1L, Slob.toUnsignedInt(new byte[]{-128, 0, 0, 0}, 0));
        assertEquals(2L*Integer.MAX_VALUE + 1L, Slob.toUnsignedInt(new byte[]{-1, -1, -1, -1}, 0));
    }

    @Test
    public void uuidConversion() {
        byte[] bytes = {-122, -72, -118, -93, 13, 121, 68, 3, -81, 97, -14, 17, 123, 65, 82, 12};
        UUID uuid = UUID.fromString("86b88aa3-0d79-4403-af61-f2117b41520c");
        assertEquals(uuid, Slob.uuid(bytes));
    }

    @Test
    public void binarySearch() {
        List<String> list = Arrays.asList(new String[]{"a", "b", "c", "x", "y"});
        Collator usCollator = Collator.getInstance(Locale.US);
        assertEquals(0, Slob.binarySearch(list, "a", usCollator));
        assertEquals(0, Slob.binarySearch(list, "9", usCollator));
        assertEquals(5, Slob.binarySearch(list, "z", usCollator));
        assertEquals(4, Slob.binarySearch(list, "y", usCollator));
        assertEquals(2, Slob.binarySearch(list, "c", usCollator));
    }

    private static Slob openTestSlob(String name) throws IOException {
        URL resource = TestSlob.class.getClassLoader().getResource(name);
        RandomAccessFile f = new RandomAccessFile(resource.getFile(), "r");
        return new Slob(f.getChannel(), name);
    }

    private static void assertTestSlobContent(Slob s) throws IOException {
        assertEquals(2, s.getBlobCount());
        assertEquals(4, s.size());

        Slob.Blob earthBlob = Slob.find("earth", s).next();
        assertEquals("text/plain; charset=utf-8", earthBlob.getContentType());

        Slob.Content content = earthBlob.getContent();
        assertEquals(earthBlob.getContentType(), content.type);

        byte[] contentBytes = new byte[content.data.remaining()];
        content.data.get(contentBytes);
        String contentAsText = new String(contentBytes, s.header.encoding);
        assertEquals("Hello, Earth!", contentAsText);

        Map<String, String> tags = s.getTags();
        assertEquals("xyz", tags.get("sometag"));
        assertEquals("abc", tags.get("some.other.tag"));
    }

    @Test
    public void readSlob() throws IOException {
        assertTestSlobContent(openTestSlob("test.slob"));
    }

    @Test
    public void readUncompressedSlob() throws IOException {
        assertTestSlobContent(openTestSlob("test-uncompressed.slob"));
    }

    @Test(expectedExceptions = Slob.UnsupportedCompressionException.class)
    public void rejectUnsupportedCompression() throws IOException {
        openTestSlob("test-bz2.slob");
    }
}