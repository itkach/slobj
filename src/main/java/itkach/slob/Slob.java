package itkach.slob;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
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
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import java.math.BigInteger;

import org.tukaani.xz.LZMA2Options;

import com.ibm.icu.text.CollationKey;
import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RuleBasedCollator;

public class Slob extends AbstractList<Slob.Blob> {

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
                    int b = lis.read();
                    if (b < 0) {
                        break;
                    }
                    out.write(b);
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
                    byte[] buf = new byte[1024];
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
        IDENTICAL(false),
        QUATERNARY(false),
        TERTIARY(false),
        SECONDARY(false),
        PRIMARY(false),
        IDENTICAL_PREFIX(true),
        QUATERNARY_PREFIX(true),
        TERTIARY_PREFIX(true),
        SECONDARY_PREFIX(true),
        PRIMARY_PREFIX(true);

        public final boolean prefix;

        private Strength(boolean prefix) {
            this.prefix = prefix;
        }
    }

    public static class UnknownFileFormat extends RuntimeException {
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
        byte[] getContent() throws IOException;
        String getContentType() throws IOException;
    }

    static final public class Blob extends Keyed {

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

        public byte[] getContent() throws IOException {
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

        protected final RandomAccessFile file;
        protected final long posOffset;
        protected final long dataOffset;
        protected final SizeType posSize;
        private final Map<Integer, T> cache;
        private int hits;
        private int misses;

        public ItemList(RandomAccessFile file, long offset, SizeType countSize, SizeType offsetSize) throws IOException {
            this(file, offset, countSize, offsetSize, 32);
        }

        public ItemList(RandomAccessFile file, long offset, SizeType countSize, SizeType offsetSize, int cacheSize) throws IOException {
            file.seek(offset);
            this.file = file;
            this.count = countSize.read(file);
            this.posOffset = file.getFilePointer();
            this.posSize = offsetSize;
            this.dataOffset = this.posOffset + this.posSize.byteSize*this.count;
            this.cache = new LruCache<Integer, T>(cacheSize);
            this.hits = 0;
            this.misses = 0;
        }

        protected long readPointer(long i) throws IOException {
            long pos = this.posOffset + this.posSize.byteSize*i;
            this.file.seek(pos);
            return this.posSize.read(this.file);
        }

        protected abstract T readItem() throws IOException;

        T read(long pointer) throws IOException {
            this.file.seek(this.dataOffset + pointer);
            return this.readItem();
        }

        public int size() {
            return (int)this.count;
        }

        synchronized public T get(int i) {
            T item = cache.get(i);
            System.out.println(String.format("Cache %s: size %d h/m: %d/%d", getClass().getName(), cache.size(), this.hits, this.misses));
            if (item != null) {
                this.hits++;
                return item;
            }
            try {
                long pointer = this.readPointer(i);
                item = this.read(pointer);
                cache.put(i, item);
                this.misses++;
                return item;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static class RefList extends ItemList<Ref> {

        final String encoding;

        public RefList(RandomAccessFile file, String encoding, long offset) throws IOException {
            super(file, offset, SizeType.UINT, SizeType.ULONG, 128);
            this.encoding = encoding;
        }

        @Override
        synchronized protected Ref readItem() throws IOException {
            String key = this.file.readText(encoding);
            long binIndex = this.file.readUnsignedInt();
            int itemIndex = this.file.readUnsignedShort();
            String fragment = this.file.readTinyText(encoding);
            return new Ref(key, binIndex, itemIndex, fragment);
        }
    }

    static class BinItem {
        final int contentTypeId;
        final byte[] content;

        public BinItem(int contentTypeId, byte[] content) {
            this.contentTypeId = contentTypeId;
            this.content = content;
        }
    }

    static class Bin extends AbstractList<BinItem> {

        final byte[] binBytes;

        private final int count;
        private final int posOffset;
        private final int dataOffset;

        public Bin(byte[] binBytes) throws IOException {
            this.binBytes = binBytes;//super(file, 0, SizeType.UINT, SizeType.UINT);
            this.count = (int)toUnsignedInt(binBytes);
            System.out.println("Bin item count: " + count);
            this.posOffset = 4;
            this.dataOffset = this.posOffset + this.count * 4;
        }

        protected BinItem readItem(int offset) throws IOException {
            int contentTypeId = toUnsignedByte(this.binBytes, offset);
            int contentLength = (int)toUnsignedInt(this.binBytes, offset+1);
            final byte[] content = new byte[contentLength];
            System.arraycopy(this.binBytes, offset + 1 + 4, content, 0, contentLength);
            return new BinItem(contentTypeId, content);
        }

        protected int readPointer(int i) throws IOException {
            return (int)toUnsignedInt(this.binBytes, this.posOffset + 4*i);
        }

        BinItem read(int pointer) throws IOException {
            return readItem(this.dataOffset + pointer);
        }

        public int size() {
            return (int)this.count;
        }

        public BinItem get(int i) {
            try {
                return this.read(this.readPointer(i));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    static class Store extends ItemList<Bin> {

        final Compressor compressor;
        final List<String> contentTypes;

        public Store(RandomAccessFile file, long offset, Compressor compressor,
                List<String> contentTypes) throws IOException {
            super(file, offset, SizeType.UINT, SizeType.ULONG);
            this.compressor = compressor;
            this.contentTypes = contentTypes;
        }

        @Override
        synchronized protected Bin readItem() throws IOException {
            long t0 = System.currentTimeMillis();
            long compressedLength = this.file.readUnsignedInt();
            System.out.println("Compressed length: " + compressedLength);
            byte[] compressed = new byte[(int)compressedLength];
            this.file.readFully(compressed);
            System.out.println("read compressed content in " + (System.currentTimeMillis() - t0));
            t0 = System.currentTimeMillis();
            byte[] decompressed = this.compressor.decompress(compressed);
            System.out.println("decompressed content in " + (System.currentTimeMillis() - t0));
            System.out.println("decompressed length: " + decompressed.length);
            return new Bin(decompressed);
        }

        synchronized ContentReader get(final long binIndex, final int itemIndex) {
            ContentReader reader = new ContentReader() {

                private BinItem getBinItem() {
                    return get((int)binIndex).get(itemIndex);
                }

                @Override
                public byte[] getContent() throws IOException {
                    return getBinItem().content;
                }

                @Override
                public String getContentType() throws IOException {
                    return contentTypes.get(getBinItem().contentTypeId);
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
    private RandomAccessFile g;
    Header header;
    RefList refList;
    Store store;

    public final File file;

    public Slob(File file) throws IOException {
        this.file = file;
        this.f = new RandomAccessFile(file, "r");
        this.header = this.readHeader();
        this.refList = new RefList(this.f, this.header.encoding, this.header.refsOffset);
        this.g = new RandomAccessFile(file, "r");
        this.store = new Store(this.g, this.header.storeOffset,
                               COMPRESSORS.get(this.header.compression),
                               this.header.contentTypes);
    }

    private Header readHeader() throws IOException {
        byte[] magic = new byte[8];
        this.f.read(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new UnknownFileFormat();
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
        if (other.getId().equals(this.getId()))
            return true;
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
        final int initialIndex = binarySearch(this, lookupEntry, comparator);
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
        this.g.close();
        this.closed = true;
    }

    public boolean isClosed() {
        return closed;
    }
    
    public static class KeyComparator implements Comparator<Keyed>{

        private final LruCache cache;
        RuleBasedCollator collator;

        public KeyComparator(int strength) {
            try {
                collator = (RuleBasedCollator)Collator.getInstance(Locale.ROOT).clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
            collator.setStrength(strength);
            collator.setAlternateHandlingShifted(true);
            cache = new LruCache<String, CollationKey>(128);
        }

        @Override
        public int compare(Keyed o1, Keyed o2) {
            return this.compare(o1.key, o2.key);
        }

        public int compare(String k1, String k2) {
            return collator.compare(k1, k2);
        }

        public CollationKey getCollationKey(String key) {
            CollationKey ck = (CollationKey)this.cache.get(key);
            if (ck != null) {
                return ck;
            }
            synchronized (collator) {
                ck = collator.getCollationKey(key);
                this.cache.put(key, ck);
                return ck;
            }
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

        public StartsWithKeyComparator(int strength) {
            super(strength);
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

    public static Map<Strength, KeyComparator> COMPARATORS = new EnumMap<Strength, KeyComparator>(Strength.class) {
        {
            put(Strength.IDENTICAL, new KeyComparator(Collator.IDENTICAL));
            put(Strength.QUATERNARY, new KeyComparator(Collator.QUATERNARY));
            put(Strength.TERTIARY, new KeyComparator(Collator.TERTIARY));
            put(Strength.SECONDARY, new KeyComparator(Collator.SECONDARY));
            put(Strength.PRIMARY, new KeyComparator(Collator.PRIMARY));
            put(Strength.IDENTICAL_PREFIX, new StartsWithKeyComparator(Collator.IDENTICAL));
            put(Strength.QUATERNARY_PREFIX, new StartsWithKeyComparator(Collator.QUATERNARY));
            put(Strength.TERTIARY_PREFIX, new StartsWithKeyComparator(Collator.TERTIARY));
            put(Strength.SECONDARY_PREFIX, new StartsWithKeyComparator(Collator.SECONDARY));
            put(Strength.PRIMARY_PREFIX, new StartsWithKeyComparator(Collator.PRIMARY));
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

    public static Iterator<Blob> find(String key, List<Slob> slobs) {
        return find(key, 100, slobs);
    }

    public static Iterator<Blob> find(final String key, int maxFromOne, Slob preferred, List<Slob> slobs) {
        long t0 = System.currentTimeMillis();

        List<Map.Entry<Slob, Strength>> variants = new ArrayList<Map.Entry<Slob, Strength>>();

        if (preferred != null) {
            for (Strength strength : Strength.values()) {
                if (!strength.prefix) {
                    variants.add(new AbstractMap.SimpleImmutableEntry<Slob, Strength>(preferred, strength));
                }
            }
        }

        for (Strength strength : Strength.values()) {
            for (Slob s : slobs) {
                if (preferred != null && s.equals(preferred) && !strength.prefix) {
                    continue;
                }
                variants.add(new AbstractMap.SimpleImmutableEntry<Slob, Strength>(s, strength));
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
        System.out.println("find returned in " + (System.currentTimeMillis() - t0));
        return result;
    }

    public static Iterator<Blob> find(String key, int maxFromOne, List<Slob> slobs) {
        return find(key, maxFromOne, null, slobs);
    }
}
