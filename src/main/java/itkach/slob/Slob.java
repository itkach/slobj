package itkach.slob;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import java.math.BigInteger;

import org.tukaani.xz.LZMA2Options;

import com.ibm.icu.text.CollationKey;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

import java.util.logging.Logger;

public class Slob extends AbstractList<Slob.Blob> {

    final static Logger L = Logger.getLogger(Slob.class.getName());

    private boolean closed;

    static public interface Compressor {
        byte[] decompress(byte[] input) throws IOException;
    }

    static Map<String, Compressor> COMPRESSORS = new HashMap<String, Compressor>();

    static public void register(String name, Compressor compressor) {
        COMPRESSORS.put(name, compressor);
    }

    static {
        register("lzma2", new Compressor(){

            LZMA2Options lzma2 = new LZMA2Options();

            @Override
            public byte[] decompress(byte[] input) throws IOException {
                ByteArrayInputStream is = new ByteArrayInputStream(input);
                InputStream lis = lzma2.getInputStream(is);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                while (true) {
                    byte[] buf = new byte[input.length*4];
                    int count = lis.read(buf);
                    if (count < 0) {
                        break;
                    }
                    out.write(buf, 0, count);
                }
                byte[] result = out.toByteArray();
                is.close();
                lis.close();
                out.close();
                return result;
            }
        });

        register("zlib", new Compressor() {

            @Override
            public byte[] decompress(byte[] input) throws IOException {
                Inflater decompressor = new Inflater();
                decompressor.setInput(input);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try {
                    byte[] buf = new byte[input.length*2];
                    while (!decompressor.finished()) {
                        int count;
                        try {
                            count = decompressor.inflate(buf);
                        } catch (DataFormatException e) {
                            throw new IOException(e);
                        }
                        out.write(buf, 0, count);
                    }
                }
                finally {
                    out.close();
                }
                return out.toByteArray();
            }
        });
    }

    static int toUnsignedByte(byte[] bytes, int offset) throws EOFException {
        int count = 1;
        assert offset + count <= bytes.length;
        int unsignedByte  = 0x000000FF & ((int)bytes[offset]);
        return unsignedByte;
    }

    static long toUnsignedInt(byte[] bytes, int offset) throws EOFException {
        int count = 4;
        assert offset + count <= bytes.length;
        int[] unsignedBytes = new int[count];
        for (int i = 0; i < count; i++) {
            unsignedBytes[i] = 0x000000FF & ((int)bytes[offset + i]);
        }
        return toUnsignedInt(unsignedBytes);
    }

    static long toUnsignedInt(byte[] bytes) throws EOFException {
        return toUnsignedInt(bytes, 0);
    }

    static long toUnsignedInt(int[] bytes) throws EOFException {
        assert bytes.length == 4;
        int ch1 = bytes[0],
        ch2 = bytes[1],
        ch3 = bytes[2],
        ch4 = bytes[3];
        if ((ch1 | ch2 | ch3 | ch4) < 0)
            throw new EOFException();
        return ((long) (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0)) & 0xFFFFFFFFL;
    }

    static long toUnsignedLong(byte[] bytes) throws EOFException {
        BigInteger bi = new BigInteger(bytes);
        long result = bi.longValue();
        return result;
    }

    static <T> int binarySearch(List<? extends T> l, T key, Comparator<? super T> c) {
        int lo = 0;
        int hi = l.size();
        while (lo < hi) {
            int mid = (lo + hi) / 2;
            T midVal = l.get(mid);
            int cmp = c.compare(midVal, key);
            if (cmp < 0) {
                lo = mid + 1;
            }
            else {
                hi = mid;
            }
        }
        return lo;
    }

    enum SizeType {
            UINT(4){

                @Override
                long read(RandomAccessFile f) throws IOException {
                    return f.readUnsignedInt();
                }

            } ,
            ULONG(8) {
                @Override
                long read(RandomAccessFile f) throws IOException {
                    return f.readUnsignedLong();
                }
            };

            final int byteSize;

            private SizeType(int byteSize) {
                this.byteSize = byteSize;
            }

            abstract long read(RandomAccessFile f) throws IOException;
    }

    public static enum Strength {
        IDENTICAL(false, Collator.IDENTICAL),
        QUATERNARY(false, Collator.QUATERNARY),
        TERTIARY(false, Collator.TERTIARY),
        SECONDARY(false, Collator.SECONDARY),
        PRIMARY(false, Collator.PRIMARY),
        IDENTICAL_PREFIX(true, Collator.IDENTICAL),
        QUATERNARY_PREFIX(true, Collator.QUATERNARY),
        TERTIARY_PREFIX(true, Collator.TERTIARY),
        SECONDARY_PREFIX(true, Collator.SECONDARY),
        PRIMARY_PREFIX(true, Collator.PRIMARY);

        public final boolean prefix;
        public final int level;

        private Strength(boolean prefix, int level) {
            this.prefix = prefix;
            this.level = level;
        }
    }

    public static class UnknownFileFormatException extends IOException {

        UnknownFileFormatException(){
            super("Unknown file format");
        }
    }

    public static class TruncatedFileException extends IOException {

        TruncatedFileException(){
            super("Truncated file");
        }
    }

    final static class RandomAccessFile extends java.io.RandomAccessFile {

        RandomAccessFile(File file, String mode) throws FileNotFoundException {
            super(file, mode);
        }

        RandomAccessFile(String fileName, String mode) throws FileNotFoundException {
            super(fileName, mode);
        }

        long readUnsignedInt() throws IOException {
            int[] data = new int[4];
            for (int i = 0; i < data.length; i++) {
                data[i] = this.read();
            }
            return toUnsignedInt(data);
        }

        long readUnsignedLong() throws IOException {
            byte[] data = new byte[8];
            this.read(data);
            return toUnsignedLong(data);
        }

        UUID uuid(byte[] data) {
            long msb = 0;
            long lsb = 0;
            assert data.length == 16;
            for (int i = 0; i < 8; i++)
                msb = (msb << 8) | (data[i] & 0xff);
            for (int i = 8; i < 16; i++)
                lsb = (lsb << 8) | (data[i] & 0xff);
            return new UUID(msb, lsb);
        }

        String mkString(byte[] data, String encoding) {
            try {
                return new String(data, encoding);
            }
            catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        String readTinyText(String encoding) throws IOException {
            int length = this.readUnsignedByte();
            byte[] data = new byte[length];
            this.read(data);
            if (length == 255) {
                for (int i = 0; i < length; i++) {
                    int b = data[i] & 0xFF;
                    if (b == 0) {
                        byte[] noNullsData = new byte[i];
                        System.arraycopy(data, 0, noNullsData, 0, i);
                        data = noNullsData;
                        break;
                    }
                }
            }
            return this.mkString(data, encoding);
        }

        String readText(String encoding) throws IOException {
            int length = this.readShort();
            byte[] data = new byte[length];
            this.read(data);
            return this.mkString(data, encoding);
        }

        UUID readUUID() throws IOException {
            byte[] s = new byte[16];
            this.read(s);
            return uuid(s);
        }
    }

    final static class Ref {
        final String key;
        final long binIndex;
        final int itemIndex;
        final String fragment;

        Ref(String key, long binIndex, int itemIndex, String fragment) {
            this.key = key;
            this.binIndex = binIndex;
            this.itemIndex = itemIndex;
            this.fragment = fragment;
        }
    }

    static public interface ContentReader {
        ByteBuffer getContent() throws IOException;
        String getContentType() throws IOException;
    }

    static final public class Blob extends Keyed implements ContentReader {

        public final Slob owner;
        public final String id;
        public final String fragment;
        public final ContentReader reader;

        public Blob(Slob owner, String blobId, String key, String fragment, ContentReader reader) {
            super(key);
            this.id = blobId;
            this.fragment = fragment;
            this.reader = reader;
            this.owner = owner;
        }

        public ByteBuffer getContent() throws IOException {
            return reader.getContent();
        }

        public String getContentType() throws IOException {
            return reader.getContentType();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result
                    + ((fragment == null) ? 0 : fragment.hashCode());
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + ((owner == null) ? 0 : owner.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!super.equals(obj))
                return false;
            if (getClass() != obj.getClass())
                return false;
            Blob other = (Blob) obj;
            if (fragment == null) {
                if (other.fragment != null)
                    return false;
            } else if (!fragment.equals(other.fragment))
                return false;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (owner == null) {
                if (other.owner != null)
                    return false;
            } else if (!owner.equals(other.owner))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return String.format("%s<%s> (%s#%s)", getClass().getName(), this.id, this.key, this.fragment);
        }
    }

    static class LruCache<A, B> extends LinkedHashMap<A, B> {
        private final int maxEntries;

        public LruCache(final int maxEntries) {
            super(maxEntries + 1, 1.0f, true);
            this.maxEntries = maxEntries;
        }

        @Override
        protected boolean removeEldestEntry(final Map.Entry<A, B> eldest) {
            return super.size() > maxEntries;
        }
    }


    static abstract class ItemList<T> extends AbstractList<T> {

        public final long count;

        protected final File file;
        protected final long posOffset;
        protected final long dataOffset;
        protected final SizeType posSize;
        private final Map<Integer, T> cache;
        private int hits;
        private int misses;

        public ItemList(File file, long offset, SizeType countSize, SizeType offsetSize, int cacheSize) throws IOException {
            this.file = file;
            RandomAccessFile f = new RandomAccessFile(file, "r");
            try {
                f.seek(offset);
                this.count = countSize.read(f);
                this.posOffset = f.getFilePointer();
            }
            finally {
                f.close();
            }
            this.posSize = offsetSize;
            this.dataOffset = this.posOffset + this.posSize.byteSize*this.count;
            this.cache = Collections.synchronizedMap(new LruCache<Integer, T>(cacheSize));
            this.hits = 0;
            this.misses = 0;
        }

        private long readPointer(RandomAccessFile f, long i) throws IOException {
            long pos = this.posOffset + this.posSize.byteSize*i;
            f.seek(pos);
            return this.posSize.read(f);
        }

        protected abstract T readItem(RandomAccessFile f) throws IOException;

        private T read(RandomAccessFile f, long pointer) throws IOException {
            f.seek(this.dataOffset + pointer);
            return this.readItem(f);
        }

        public int size() {
            return (int)this.count;
        }

        public T get(int i) {
            T item = cache.get(i);
            if (L.isLoggable(Level.FINEST)) {
                L.finest(String.format("Cache %s: size %d h/m : %d/%d (%f)",
                        getClass().getName(), cache.size(), this.hits, this.misses, this.hits * 1.0 / this.misses));
            }
            if (item != null) {
                this.hits++;
                return item;
            }
            RandomAccessFile f = null;
            try {
                f = new RandomAccessFile(file, "r");
                long pointer = this.readPointer(f, i);
                item = this.read(f, pointer);
                cache.put(i, item);
                this.misses++;
                return item;
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            finally {
                if (f != null) {
                    try {
                        f.close();
                    }
                    catch (IOException ex) {
                        L.warning("Failed to close " + file.getAbsolutePath());
                    }
                }
            }
        }
    }

    static class RefList extends ItemList<Ref> {

        final String encoding;

        public RefList(File file, String encoding, long offset) throws IOException {
            super(file, offset, SizeType.UINT, SizeType.ULONG, 1024);
            this.encoding = encoding;
        }

        @Override
        protected Ref readItem(RandomAccessFile f) throws IOException {
            String key = f.readText(encoding);
            long binIndex = f.readUnsignedInt();
            int itemIndex = f.readUnsignedShort();
            String fragment = f.readTinyText(encoding);
            return new Ref(key, binIndex, itemIndex, fragment);
        }
    }

    static class KeyList extends ItemList<Keyed> {

        final String encoding;

        public KeyList(File file, String encoding, long offset) throws IOException {
            super(file, offset, SizeType.UINT, SizeType.ULONG, 1024);
            this.encoding = encoding;
        }

        @Override
        protected Keyed readItem(RandomAccessFile f) throws IOException {
            String key = f.readText(encoding);
            return new Keyed(key);
        }
    }


    static class Bin extends AbstractList<ByteBuffer> {

        private final byte[] binBytes;

        private final int count;
        private final int posOffset;
        private final int dataOffset;

        public Bin(byte[] binBytes, int count) throws IOException {
            this.binBytes = binBytes;
            this.count = count;
            this.posOffset = 0;
            this.dataOffset = this.posOffset + this.count * 4;
        }

        protected ByteBuffer readItem(int offset) throws IOException {
            int contentLength = (int)toUnsignedInt(this.binBytes, offset);
            return ByteBuffer
                    .wrap(this.binBytes, offset + 4, contentLength)
                    .asReadOnlyBuffer();
        }

        protected int readPointer(int i) throws IOException {
            return (int)toUnsignedInt(this.binBytes, this.posOffset + 4*i);
        }

        ByteBuffer read(int pointer) throws IOException {
            return readItem(this.dataOffset + pointer);
        }

        public int size() {
            return this.count;
        }

        public ByteBuffer get(int i) {
            try {
                return this.read(this.readPointer(i));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    static class StoreItem {

        final int[] contentTypeIds;
        final byte[] compressedContent;

        StoreItem(int[] contentTypeIds, byte[] compressedContent) {
            this.contentTypeIds = contentTypeIds;
            this.compressedContent = compressedContent;
        }
    }

    static class Store extends ItemList<StoreItem> {

        final Compressor compressor;
        final List<String> contentTypes;

        public Store(File file, long offset, Compressor compressor,
                List<String> contentTypes) throws IOException {
            super(file, offset, SizeType.UINT, SizeType.ULONG, 4);
            this.compressor = compressor;
            this.contentTypes = contentTypes;
        }

        @Override
        protected StoreItem readItem(RandomAccessFile f) throws IOException {
            long t0 = System.currentTimeMillis();
            long binItemCount = f.readUnsignedInt();
            int[] contentTypeIds = new int[(int)binItemCount];
            for (int i = 0; i < binItemCount; i++) {
                contentTypeIds[i] = f.readUnsignedByte();
            }
            long compressedLength = f.readUnsignedInt();
            L.fine("Compressed length: " + compressedLength);
            byte[] compressed = new byte[(int)compressedLength];
            f.readFully(compressed);
            L.fine("read compressed content bytes in " + (System.currentTimeMillis() - t0));
            return new StoreItem(contentTypeIds, compressed);
        }

        ContentReader get(final long binIndex, final int itemIndex) {
            ContentReader reader = new ContentReader() {

                Bin bin;

                private ByteBuffer getBinItem() throws IOException {
                    if (bin == null) {
                        long t0 = System.currentTimeMillis();
                        StoreItem storeItem = get((int)binIndex);
                        byte[] decompressed = compressor.decompress(
                                storeItem.compressedContent);
                        L.fine("decompressed content in " + (System.currentTimeMillis() - t0));
                        L.fine("decompressed length: " + decompressed.length);
                        bin = new Bin(decompressed, storeItem.contentTypeIds.length);
                    }
                    return bin.get(itemIndex);
                }

                @Override
                public ByteBuffer getContent() throws IOException {
                    return getBinItem().slice();
                }

                @Override
                public String getContentType() throws IOException {
                    StoreItem storeItem = get((int)binIndex);
                    return contentTypes.get(storeItem.contentTypeIds[itemIndex]);
                }
            };
            return reader;
        }
    }

    public final byte[] MAGIC = new byte[]{0x21, 0x2d, 0x31, 0x53, 0x4c, 0x4f, 0x42, 0x1f};

    public final static class Header {
        public final byte[] magic;
        public final UUID uuid;
        public final String encoding;
        public final String compression;
        public final Map<String, String> tags;
        public final List<String> contentTypes;
        public final long blobCount;
        public final long storeOffset;
        public final long refsOffset;
        public final long size;

        Header(byte[] magic,
               UUID uuid,
               String encoding,
               String compression,
               Map<String, String> tags,
               List<String> contentTypes,
               long blobCount,
               long storeOffset,
               long refsOffset,
               long size) {
            this.magic = magic;
            this.uuid = uuid;
            this.encoding = encoding;
            this.compression = compression;
            this.tags = tags;
            this.contentTypes = contentTypes;
            this.blobCount = blobCount;
            this.storeOffset = storeOffset;
            this.refsOffset = refsOffset;
            this.size = size;
        }
    }

    private RandomAccessFile f;
    public final Header header;
    RefList refList;
    KeyList keyList;
    Store store;

    public final File file;

    public Slob(File file) throws IOException {
        this.file = file;
        //We don't really need it, just hold on to it
        //to prevent deleting file while slob is open
        this.f = new RandomAccessFile(file, "r");
        try {
            this.header = this.readHeader();
            if (this.header.size != this.f.length()) {
                throw new TruncatedFileException();
            }
        }
        catch(IOException ex) {
            this.close();
            throw ex;
        }
        this.refList = new RefList(file, this.header.encoding, this.header.refsOffset);
        this.keyList = new KeyList(file, this.header.encoding, this.header.refsOffset);
        this.store = new Store(file, this.header.storeOffset,
                               COMPRESSORS.get(this.header.compression),
                               this.header.contentTypes);
    }

    private Header readHeader() throws IOException {
        byte[] magic = new byte[8];
        this.f.read(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new UnknownFileFormatException();
        }
        UUID uuid = f.readUUID();
        String encoding = f.readTinyText("UTF-8");
        String compression = f.readTinyText(encoding);
        Map<String, String> tags = readTags(encoding);
        List<String> contentTypes = readContentTypes(encoding);
        long blobCount = this.f.readUnsignedInt();
        long storeOffset = this.f.readLong();
        long size = this.f.readLong();
        long refsOffset = this.f.getFilePointer();
        return new Header(
                magic, uuid, encoding, compression, tags,
                contentTypes, blobCount, storeOffset, refsOffset, size);
    }

    private Map<String, String> readTags(String encoding) throws IOException {
        HashMap<String, String> tags = new HashMap<String, String>();
        int length = this.f.readUnsignedByte();
        for (int i = 0; i < length; i++) {
            String key = this.f.readTinyText(encoding);
            String value = this.f.readTinyText(encoding);
            tags.put(key, value);
        }
        return Collections.unmodifiableMap(tags);
    }

    private List<String> readContentTypes(String encoding) throws IOException {
        ArrayList<String> contentTypes = new ArrayList<String>();
        int length = this.f.readUnsignedByte();
        for (int i = 0; i < length; i++) {
            contentTypes.add(this.f.readText(encoding));
        }
        return Collections.unmodifiableList(contentTypes);
    }

    public UUID getId() {
        return header.uuid;
    }

    public Map<String, String> getTags() {
        return header.tags;
    }

    public long getBlobCount() {
        return header.blobCount;
    }

    public String getURI() {
        Map<String, String> tags = getTags();
        String uri = tags.get("uri");
        if (uri == null) {
            uri = "slob:" + getId();
        }
        return uri;
    }

    public int size() {
        return this.refList.size();
    }

    public Blob get(int i) {
        Ref ref = this.refList.get(i);
        ContentReader contentReader = this.store.get(ref.binIndex, ref.itemIndex);
        return new Blob(this,
                String.format("%s-%s", ref.binIndex, ref.itemIndex),
                ref.key, ref.fragment, contentReader);
    }

    public ContentReader get(String blobId) throws IOException {
        String[] parts = blobId.split("-", 2);
        long binIndex = Long.parseLong(parts[0]);
        int itemIndex = Integer.parseInt(parts[1]);
        return this.store.get(binIndex, itemIndex);
    }

    @Override
    public String toString() {
        return String.format("%s <%s>", this.getClass().getName(), this.getId());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Slob other = (Slob) obj;
        if (!other.getId().equals(this.getId()))
            return false;
        return true;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    public static class Keyed {

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((key == null) ? 0 : key.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Keyed other = (Keyed) obj;
            if (key == null) {
                if (other.key != null)
                    return false;
            } else if (!key.equals(other.key))
                return false;
            return true;
        }

        final public String key;

        public Keyed(String key) {
            this.key = key;
        }
    }

    public Iterator<Blob> find(final String key, Strength strength) {
        long t0 = System.currentTimeMillis();
        final Comparator<Keyed> comparator = COMPARATORS.get(strength);
        final Keyed lookupEntry = new Keyed(key);
        final int initialIndex = binarySearch(this.keyList, lookupEntry, comparator);
        if (L.isLoggable(Level.FINE)) {
            L.fine(String.format("%s: done binary search for %s (strength %s) in %s",
                    getTags().get("label"), key, strength, System.currentTimeMillis() - t0));
        }
        Iterator<Blob> iterator = new Iterator<Blob>() {

            int   index = initialIndex;
            Blob nextEntry;

            {
                prepareNext();
            }

            private void prepareNext() {
                if (index < size()) {
                    Blob matchedEntry = get(index);
                    nextEntry = (0 == comparator.compare(matchedEntry, lookupEntry)) ? matchedEntry : null;
                    index++;
                }
                else {
                    nextEntry = null;
                }
            }

            public boolean hasNext() {
                return nextEntry != null;
            }

            public Blob next() {
                Blob current = nextEntry;
                prepareNext();
                return current;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        return iterator;
    }


    public void close() throws IOException {
        this.f.close();
        this.closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    public static class KeyComparator implements Comparator<Keyed>{

        private Map<String, CollationKey> cache;
        private RuleBasedCollator collator;

        public KeyComparator(int strength, Map<String, CollationKey> cache) {
            try {
                collator = (RuleBasedCollator)Collator.getInstance(Locale.ROOT).clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
            collator.setStrength(strength);
            collator.setAlternateHandlingShifted(true);
            this.cache = cache;
        }

        @Override
        public int compare(Keyed o1, Keyed o2) {
            return this.compare(o1.key, o2.key);
        }

        public int compare(String k1, String k2) {
            CollationKey ck1 = getCollationKey(k1);
            CollationKey ck2 = getCollationKey(k2);
            return ck1.compareTo(ck2);
        }

        public CollationKey getCollationKey(String key) {
            CollationKey ck = cache.get(key);
            if (ck == null) {
                synchronized (collator) {
                    ck = collator.getCollationKey(key);
                }
                cache.put(key, ck);
            }
            return ck;
        }
    }

    static String format(byte[] bytes) {
        StringBuilder s = new StringBuilder();
        for (byte b : bytes) {
            s.append(Integer.toHexString(b & 0xFF));
            s.append("-");
        }
        s.deleteCharAt(s.length() - 1);
        return s.toString();
    }

    static class StartsWithKeyComparator extends KeyComparator {

        public StartsWithKeyComparator(int strength, Map<String, CollationKey> cache) {
            super(strength, cache);
        }

        @Override
        public int compare(Keyed o1, Keyed o2) {
            CollationKey ck1 = getCollationKey(o1.key);
            CollationKey ck2 = getCollationKey(o2.key);

            byte[] key1 = ck1.toByteArray();
            byte[] key2 = ck2.toByteArray();

            int key, targetKey, result = 0, i = 0;
            while (true) {
                key = key1[i] & 0xFF;
                targetKey = key2[i] & 0xFF;
                if (targetKey == 0) {
                    break;
                }
                if (key == 0) {
                    result = -1;
                    break;
                }
                if (key < targetKey) {
                    result = -1;
                    break;
                }
                if (key > targetKey) {
                    result = 1;
                    break;
                }
                i++;
            }
            return result;
        }
    }

    private static Map<String, CollationKey> newCollationKeyCache() {
        return Collections.synchronizedMap(new LruCache<String, CollationKey>(4096));
    }

    public static Map<Strength, KeyComparator> COMPARATORS = new EnumMap<Strength, KeyComparator>(Strength.class) {
        {
            Map<String, CollationKey> quaternaryCache = newCollationKeyCache();
            Map<String, CollationKey> tertiaryCache = newCollationKeyCache();
            Map<String, CollationKey> secondaryCache = newCollationKeyCache();
            Map<String, CollationKey> primaryCache = newCollationKeyCache();

            put(Strength.QUATERNARY, new KeyComparator(Collator.QUATERNARY, quaternaryCache));
            put(Strength.TERTIARY, new KeyComparator(Collator.TERTIARY, tertiaryCache));
            put(Strength.SECONDARY, new KeyComparator(Collator.SECONDARY, secondaryCache));
            put(Strength.PRIMARY, new KeyComparator(Collator.PRIMARY, primaryCache));
            put(Strength.QUATERNARY_PREFIX, new StartsWithKeyComparator(Collator.QUATERNARY, quaternaryCache));
            put(Strength.TERTIARY_PREFIX, new StartsWithKeyComparator(Collator.TERTIARY, tertiaryCache));
            put(Strength.SECONDARY_PREFIX, new StartsWithKeyComparator(Collator.SECONDARY, secondaryCache));
            put(Strength.PRIMARY_PREFIX, new StartsWithKeyComparator(Collator.PRIMARY, primaryCache));
        }
    };

    static final class MatchIterator implements Iterator<Blob> {

        private Blob                        next;
        private int                         currentCount = 0;
        private Set<String>                 seen         = new HashSet<String>();
        private Iterator<Iterator<Blob>>    iterators;
        private Iterator<Blob>              current;
        private int                         maxFromOne;

        MatchIterator(Iterator<Iterator<Blob>> iterators, int maxFromOne) {
            this.iterators = iterators;
            this.maxFromOne = maxFromOne;
            if (this.iterators.hasNext()) {
                this.current = this.iterators.next();
            }
        }

        private String mkDedupKey(Blob b) {
            return String.format("%s:%s#%s", b.owner.getId(), b.id, b.fragment);
        }

        public Blob next() {
            if (next != null) {
                Blob toReturn = next;
                next = null;
                return toReturn;
            }
            if (current == null) {
                return null;
            }
            while (true) {
                while (current.hasNext() && currentCount <= maxFromOne) {
                    Blob maybeNext = current.next();
                    String dedupKey = mkDedupKey(maybeNext);
                    if (seen.contains(dedupKey)) {
                        continue;
                    }
                    else {
                        seen.add(dedupKey);
                        currentCount++;
                        next = maybeNext;
                        return next;
                    }
                }
                if (this.iterators.hasNext()) {
                    current = this.iterators.next();
                    currentCount = 0;
                }
                else {
                    current = null;
                    break;
                }
            }
            return null;
        }

        public boolean hasNext() {
            if (next == null) {
                next();
            }
            return next != null;
        }


        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static Iterator<Blob> find(String key, Slob ... slobs) {
        return find(key, 100, slobs);
    }


    private static int findWeight(Map.Entry<Slob, Strength> e, Slob preferred) {
        Slob slob = e.getKey();
        Strength strength = e.getValue();
        int w = 0;
        if (preferred != null) {
            //We want to see all whole word matches from preferred slob first,
            //case, diacritic and punctuation insensitive
            //Only slight preference to be given for partial (prefix) matches
            //Whole word matches from other dictionaries should appear before any partial matches
            if (slob.equals(preferred)) {
                w = strength.prefix ? 50 : 1000;
            }
            //Slobs with the same URI are almost as good as preferred slob itself
            //This is useful when content was split into multiple slobs sharing same URI tag
            else if (slob.getURI().equals(preferred.getURI())) {
                w = strength.prefix ? 50 : 500;
            }
        }
        /*
        PRIMARY 0
        IDENTITY 15
         */
        w += strength.level + (strength.prefix ? 0 : 100);
        return w;
    }

    public static Iterator<Blob> find(final String key, int maxFromOne, final Slob preferred, Slob[] slobs, int maxStrengthLevel) {
        long t0 = System.currentTimeMillis();

        List<Map.Entry<Slob, Strength>> variants = new ArrayList<Map.Entry<Slob, Strength>>();

        for (Strength strength : Strength.values()) {
            if (strength.level > maxStrengthLevel) {
                continue;
            }
            for (Slob s : slobs) {
                variants.add(new AbstractMap.SimpleImmutableEntry<Slob, Strength>(s, strength));
            }
        }

        Collections.sort(variants, new Comparator<Map.Entry<Slob, Strength>>() {
            @Override
            public int compare(Map.Entry<Slob, Strength> e1, Map.Entry<Slob, Strength> e2) {
                int w1 = findWeight(e1, preferred);
                int w2 = findWeight(e2, preferred);
                return w2 - w1;
            }
        });

        if (L.isLoggable(Level.FINER)) {
            if (preferred != null) {
                L.finer("Preferred: " + preferred.getId() + " " + preferred.getURI());
            }
            for (Map.Entry<Slob, Strength> variant : variants) {
                L.finer(String.format("%d %s %s %s", findWeight(variant, preferred),
                        variant.getKey().getId(), variant.getKey().getURI(), variant.getValue()));
            }
        }

        final Iterator<Map.Entry<Slob, Strength>>variantsIterator = variants.iterator();


        Iterator<Iterator<Blob>> iterators = new Iterator<Iterator<Blob>>() {

            public boolean hasNext() {
                return variantsIterator.hasNext();
            }

            public Iterator<Blob> next() {
                Map.Entry<Slob, Strength> variant = variantsIterator.next();
                Iterator<Blob> result = variant.getKey().find(key, variant.getValue());
                return result;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
        MatchIterator result = new MatchIterator(iterators, maxFromOne);
        L.fine("find returned in " + (System.currentTimeMillis() - t0));
        return result;
    }

    public static Iterator<Blob> find(String key, int maxFromOne, Slob ... slobs) {
        return find(key, maxFromOne, null, slobs, Collator.QUATERNARY);
    }
}
