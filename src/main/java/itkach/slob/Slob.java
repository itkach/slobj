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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
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

public final class Slob extends AbstractList<Slob.Blob> {

    final static Logger L = Logger.getLogger(Slob.class.getName());

    public interface Compressor {
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
                    byte[] buf = new byte[input.length*8];
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
                    byte[] buf = new byte[input.length*4];
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

    private static Map<Integer, Map<String, CollationKey>> collationCaches = new HashMap<Integer, Map<String, CollationKey>>(){
        {
            put(Collator.QUATERNARY, newCollationKeyCache());
            put(Collator.TERTIARY, newCollationKeyCache());
            put(Collator.SECONDARY, newCollationKeyCache());
            put(Collator.PRIMARY, newCollationKeyCache());
        }
    };

    public enum Strength {
        IDENTICAL(Collator.IDENTICAL),
        QUATERNARY(Collator.QUATERNARY),
        TERTIARY(Collator.TERTIARY),
        SECONDARY(Collator.SECONDARY),
        PRIMARY(Collator.PRIMARY),
        QUATERNARY_PREFIX(true, Collator.QUATERNARY),
        TERTIARY_PREFIX(true, Collator.TERTIARY),
        SECONDARY_PREFIX(true, Collator.SECONDARY),
        PRIMARY_PREFIX(true, Collator.PRIMARY);

        public final boolean        prefix;
        public final int            level;
        public final KeyComparator  comparator;
        public final KeyComparator  stopComparator;

        Strength(int level) {
            this(false, level);
        }

        Strength(boolean prefix, int level) {
            this.prefix = prefix;
            this.level = level;
            Map<String, CollationKey> cache = collationCaches.get(level);
            comparator = new KeyComparator(level, cache);
            if (prefix) {
                stopComparator = new StartsWithKeyComparator(level, cache);
            }
            else {
                stopComparator = comparator;
            }
        }

    }

    public final static class UnknownFileFormatException extends IOException {

        UnknownFileFormatException(){
            super("Unknown file format");
        }
    }

    public final static class TruncatedFileException extends IOException {

        TruncatedFileException(){
            super("Truncated file");
        }
    }

    public final class UnexpectedFileException extends IOException {
        UnexpectedFileException() {
            super("Unexpected file");
        }
    }


    final static class RandomAccessFile extends java.io.RandomAccessFile {

        RandomAccessFile(File file) throws FileNotFoundException {
            this(file, "r");
        }

        private RandomAccessFile(File file, String mode) throws FileNotFoundException {
            super(file, mode);
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

    public final static class Content {
        public final String     type;
        public final ByteBuffer data;

        Content(String type, ByteBuffer data) {
            this.type = type;
            this.data = data;
        }
    }

    public final static class Blob extends Keyed {

        public final Slob   owner;
        public final String id;
        public final String fragment;

        private final int binIndex;
        private final int itemIndex;

        public Blob(Slob owner, String blobId, String key, String fragment) {
            super(key);
            this.id = blobId;
            this.fragment = fragment;
            this.owner = owner;
            int[] parts = owner.splitBlobId(blobId);
            this.binIndex = parts[0];
            this.itemIndex = parts[1];
        }

        public Content getContent() {
            return owner.getContent(binIndex, itemIndex);
        }

        public String getContentType() {
            return owner.getContentType(binIndex, itemIndex);
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


    final static class ItemListInfo {

        final long      count;
        final long      posOffset;
        final long      dataOffset;
        final SizeType  posSize;

        ItemListInfo(long count, long posOffset, long dataOffset, SizeType posSize) {
            this.count = count;
            this.posOffset = posOffset;
            this.dataOffset = dataOffset;
            this.posSize = posSize;
        }
    }

    static ItemListInfo readItemListInfo(RandomAccessFile f,
                                         long offset,
                                         SizeType countSize,
                                         SizeType offsetSize) throws IOException {
        f.seek(offset);
        long count = countSize.read(f);
        long posOffset = f.getFilePointer();
        SizeType posSize = offsetSize;
        long dataOffset = posOffset + posSize.byteSize*count;
        return new ItemListInfo(count, posOffset, dataOffset, posSize);
    }

    static abstract class ItemList<T> extends AbstractList<T> {

        public final long count;

        protected final RandomAccessFile file;
        protected final long posOffset;
        protected final long dataOffset;
        protected final SizeType posSize;
        private final Map<Integer, T> cache;

        public ItemList(RandomAccessFile file, ItemListInfo info, Map<Integer, T> cache) {
            this.file = file;
            this.count = info.count;
            this.posOffset = info.posOffset;
            this.cache = cache;
            this.posSize = info.posSize;
            this.dataOffset = info.dataOffset;
        }

        private long readPointer(long i) throws IOException {
            long pos = this.posOffset + this.posSize.byteSize*i;
            this.file.seek(pos);
            return this.posSize.read(this.file);
        }

        protected abstract T readItem() throws IOException;

        private T read(long pointer) throws IOException {
            this.file.seek(this.dataOffset + pointer);
            return this.readItem();
        }

        public int size() {
            return (int)this.count;
        }

        public T get(int i) {
            T item = cache.get(i);
            if (item != null) {
                return item;
            }
            try {
                long pointer = this.readPointer(i);
                item = this.read(pointer);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            cache.put(i, item);
            return item;
        }
    }

    final static class RefList extends ItemList<Ref> {

        final String encoding;

        public RefList(RandomAccessFile file,
                       String encoding,
                       ItemListInfo info,
                       Map<Integer, Ref> cache) {
            super(file,  info, cache);
            this.encoding = encoding;
        }

        @Override
        protected Ref readItem() throws IOException {
            String key = file.readText(encoding);
            long binIndex = file.readUnsignedInt();
            int itemIndex = file.readUnsignedShort();
            String fragment = file.readTinyText(encoding);
            return new Ref(key, binIndex, itemIndex, fragment);
        }
    }

    final static class KeyList extends ItemList<Keyed> {

        final String encoding;

        public KeyList(RandomAccessFile file,
                       String encoding,
                       ItemListInfo info,
                       Map<Integer, Keyed> cache) {
            super(file,  info, cache);
            this.encoding = encoding;
        }

        @Override
        protected Keyed readItem() throws IOException {
            String key = file.readText(encoding);
            return new Keyed(key);
        }
    }


    final static class Bin extends AbstractList<ByteBuffer> {

        private final byte[] binBytes;

        private final int count;
        private final int posOffset;
        private final int dataOffset;

        public Bin(byte[] binBytes, int count) {
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

    final static class StoreItem {

        final int[]     contentTypeIds;

        private byte[]  compressedContent;
        private Bin     bin;

        StoreItem(int[] contentTypeIds, byte[] compressedContent) {
            this.contentTypeIds = contentTypeIds;
            this.compressedContent = compressedContent;
        }

        private ByteBuffer getBinItem(int itemIndex, Compressor compressor) throws IOException {
            if (bin == null) {
                long t0 = System.currentTimeMillis();
                byte[] decompressed = compressor.decompress(this.compressedContent);
                this.compressedContent = null;
                L.fine("decompressed content in " + (System.currentTimeMillis() - t0));
                L.fine("decompressed length: " + decompressed.length);
                bin = new Bin(decompressed, this.contentTypeIds.length);
            }
            return bin.get(itemIndex);
        }

    }

    final static class Store extends ItemList<StoreItem> {

        final Compressor compressor;
        final List<String> contentTypes;

        public Store(RandomAccessFile file,
                     Compressor compressor,
                     List<String> contentTypes,
                     ItemListInfo info,
                     Map<Integer, StoreItem> cache
                    ) {
            super(file, info, cache);
            this.compressor = compressor;
            this.contentTypes = contentTypes;
        }

        @Override
        protected StoreItem readItem() throws IOException {
            long t0 = System.currentTimeMillis();
            long binItemCount = file.readUnsignedInt();
            int[] contentTypeIds = new int[(int)binItemCount];
            for (int i = 0; i < binItemCount; i++) {
                contentTypeIds[i] = file.readUnsignedByte();
            }
            long compressedLength = file.readUnsignedInt();
            L.fine("Compressed length: " + compressedLength);
            byte[] compressed = new byte[(int)compressedLength];
            file.readFully(compressed);
            L.fine("read compressed content bytes in " + (System.currentTimeMillis() - t0));
            return new StoreItem(contentTypeIds, compressed);
        }

        String getContentType(int binIndex, int itemIndex) {
            StoreItem storeItem = get(binIndex);
            return contentTypes.get(storeItem.contentTypeIds[itemIndex]);
        }

        ByteBuffer getContentData(int binIndex, int itemIndex) {
            StoreItem storeItem = get(binIndex);
            try {
                return storeItem.getBinItem(itemIndex, compressor);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    static interface FileOperation<T> {
        T run(RandomAccessFile f);
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

    public final Header header;

    private ItemListInfo refListInfo;
    private ItemListInfo storeListInfo;

    private Map<Integer, Ref> refCache = Collections.synchronizedMap(new LruCache<Integer, Ref>(256));
    private Map<Integer, Keyed> keyCache = Collections.synchronizedMap(new LruCache<Integer, Keyed>(256));
    private Map<Integer, StoreItem> storeCache = Collections.synchronizedMap(new LruCache<Integer, StoreItem>(4));

    public final File file;

    public Slob(File file) throws IOException {
        this.file = file;
        RandomAccessFile f = new RandomAccessFile(file, "r");
        try {
            this.header = this.readHeader(f);
            if (this.header.size != f.length()) {
                throw new TruncatedFileException();
            }
            this.refListInfo = readItemListInfo(f, this.header.refsOffset, SizeType.UINT, SizeType.ULONG);
            this.storeListInfo = readItemListInfo(f, this.header.storeOffset, SizeType.UINT, SizeType.ULONG);
        }
        finally {
            f.close();
        }
    }


    private void checkFile(RandomAccessFile f) throws IOException {
        f.seek(0);
        byte[] magic = new byte[8];
        f.read(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new UnknownFileFormatException();
        }
        UUID uuid = f.readUUID();
        if (!uuid.equals(this.header.uuid)) {
            throw new UnexpectedFileException();
        }
    }

    private Header readHeader(RandomAccessFile f) throws IOException {
        byte[] magic = new byte[8];
        f.read(magic);
        if (!Arrays.equals(magic, MAGIC)) {
            throw new UnknownFileFormatException();
        }
        UUID uuid = f.readUUID();
        String encoding = f.readTinyText("UTF-8");
        String compression = f.readTinyText(encoding);
        Map<String, String> tags = readTags(f, encoding);
        List<String> contentTypes = readContentTypes(f, encoding);
        long blobCount = f.readUnsignedInt();
        long storeOffset = f.readLong();
        long size = f.readLong();
        long refsOffset = f.getFilePointer();
        return new Header(
                magic, uuid, encoding, compression, tags,
                contentTypes, blobCount, storeOffset, refsOffset, size);
    }

    private Map<String, String> readTags(RandomAccessFile f, String encoding) throws IOException {
        HashMap<String, String> tags = new HashMap<String, String>();
        int length = f.readUnsignedByte();
        for (int i = 0; i < length; i++) {
            String key = f.readTinyText(encoding);
            String value = f.readTinyText(encoding);
            tags.put(key, value);
        }
        return Collections.unmodifiableMap(tags);
    }

    private List<String> readContentTypes(RandomAccessFile f, String encoding) throws IOException {
        ArrayList<String> contentTypes = new ArrayList<String>();
        int length = f.readUnsignedByte();
        for (int i = 0; i < length; i++) {
            contentTypes.add(f.readText(encoding));
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
        return (int)this.refListInfo.count;
    }

    private Store newStoreInstance(RandomAccessFile f) {
        return new Store(
                f, COMPRESSORS.get(header.compression),
                header.contentTypes,
                storeListInfo,
                storeCache);
    }

    private RefList newRefListInstance(RandomAccessFile f) {
        return new RefList(
                f, header.encoding,
                refListInfo,
                refCache);
    }

    private KeyList newKeyListInstance(RandomAccessFile f) {
        return new KeyList(
                f, header.encoding,
                refListInfo,
                keyCache);
    }


    public Blob get(final int i) {

        return run(new FileOperation<Blob>() {
            @Override
            public Blob run(RandomAccessFile f) {
                RefList refList = newRefListInstance(f);
                Ref ref = refList.get(i);
                return new Blob(Slob.this,
                        String.format("%s-%s", ref.binIndex, ref.itemIndex),
                        ref.key, ref.fragment);
            }
        });
    }

    int[] splitBlobId(String blobId) {
        String[] parts = blobId.split("-", 2);
        return new int[] {Integer.parseInt(parts[0]), Integer.parseInt(parts[1])};
    }

    public String getContentType(String blobId) {
        int[] parts = splitBlobId(blobId);
        return getContentType(parts[0], parts[1]);
    };

    String getContentType(final int binIndex, final int itemIndex) {
        return run(new FileOperation<String>() {
            @Override
            public String run(RandomAccessFile f) {
                Store store = newStoreInstance(f);
                return store.getContentType(binIndex, itemIndex);
            }
        });
    };


    public Content getContent(final String blobId) {
        int[] parts = splitBlobId(blobId);
        return getContent(parts[0], parts[1]);
    };

    Content getContent(final int binIndex, final int itemIndex) {
        return run(new FileOperation<Content>() {
            @Override
            public Content run(RandomAccessFile f) {
                Store store = newStoreInstance(f);
                ByteBuffer data = store.getContentData(binIndex, itemIndex);
                String type = store.getContentType(binIndex, itemIndex);
                return new Content(type, data);
            }
        });
    };



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

    private <T> T run(FileOperation<T> fileOperation) {
        RandomAccessFile f;
        try {
            f = new RandomAccessFile(this.file);
            checkFile(f);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            return fileOperation.run(f);
        }
        finally {
            try {
                f.close();
            } catch (IOException e) {
                new RuntimeException(e);
            }
        }
    }

    private int indexOf(final Keyed lookupEntry, final Comparator comparator) {
        return run(new FileOperation<Integer>() {
            @Override
            public Integer run(RandomAccessFile f) {
                KeyList keyList = newKeyListInstance(f);
                return binarySearch(keyList, lookupEntry, comparator);
            }
        });
    }

    public Iterator<Blob> find(final String key, Strength strength) {
        final Comparator<Keyed> comparator = strength.comparator;
        final Comparator<Keyed> stopComparator = strength.stopComparator;

        final Keyed lookupEntry = new Keyed(key);
        long t0 = System.currentTimeMillis();
        final int initialIndex = indexOf(lookupEntry, comparator);
        if (L.isLoggable(Level.FINE)) {
            L.info(String.format("%s: done binary search for %s (strength %s) in %s",
                    getTags().get("label"), key, strength, System.currentTimeMillis() - t0));
        }
        Iterator<Blob> iterator = new Iterator<Blob>() {

            int index = initialIndex;
            Blob nextEntry;

            {
                prepareNext();
            }

            private void prepareNext() {
                if (index < size()) {
                    Blob matchedEntry = get(index);
                    nextEntry = (0 == stopComparator.compare(matchedEntry, lookupEntry)) ? matchedEntry : null;
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


    public static class KeyComparator implements Comparator<Keyed>{

        protected Map<String, CollationKey> cache;
        protected RuleBasedCollator collator;

        public KeyComparator(int strength, Map<String, CollationKey> cache) {
            this(strength, cache, true);
        }

        public KeyComparator(int strength, Map<String, CollationKey> cache, boolean alternateHandlingShifted) {
            try {
                collator = (RuleBasedCollator)Collator.getInstance(Locale.ROOT).clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
            collator.setStrength(strength);
            collator.setAlternateHandlingShifted(alternateHandlingShifted);
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

    public final static Iterator<Blob> EMPTY_RESULT = new Iterator<Blob>(){

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Blob next() {
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    };


    static final class FindResult {

        final Iterator<Blob> iterator;
        final Strength       strength;

        FindResult(Iterator<Blob> iterator, Strength strength) {
            this.iterator = iterator;
            this.strength = strength;
        }
    }

    static final class MergeBufferItem {

        final Blob           blob;
        final Strength       strength;

        MergeBufferItem(Blob blob, Strength strength) {
            this.blob = blob;
            this.strength = strength;
        }
    }

    static final class MergeBufferItemComparator implements Comparator<MergeBufferItem> {

        private final Slob preferred;

        MergeBufferItemComparator(Slob preferred) {
            this.preferred = preferred;
        }

        @Override
        public int compare(MergeBufferItem o1, MergeBufferItem o2) {
            Strength s1 = o1.strength;
            Strength s2 = o2.strength;
            Slob dict1 = o1.blob.owner;
            Slob dict2 = o2.blob.owner;
            if (!s1.prefix && !s2.prefix && !dict1.equals(dict2)) {
                if (preferred != null) {
                    if (dict1.equals(preferred)) {
                        return -1;
                    }
                    if (dict2.equals(preferred)) {
                        return 1;
                    }
                    String uri1 = dict1.getURI();
                    String uri2 = dict2.getURI();
                    if (!uri1.equals(uri2)) {
                        String preferredURI = preferred.getURI();
                        if (uri1.equals(preferredURI)) {
                            return -1;
                        }
                        if (uri2.equals(preferredURI)) {
                            return 1;
                        }
                    }
                }
            }

            if (s1 == s2) {
                Comparator<Keyed> cmp = s1.comparator;
                return cmp.compare(o1.blob, o2.blob);
            }
            return s1.level < s2.level ? 1 : -1;
        }
    }

    public interface PeekableIterator<E> extends  Iterator<E> {
        E peek();
    }

    static final class MatchIterator implements PeekableIterator<Blob> {

        private Set<String>                 seen = new HashSet<String>();
        private List<MergeBufferItem>       mergeBuffer;
        private MergeBufferItemComparator   comparator;
        private Map<Slob, FindResult>       iterators = new HashMap<Slob, FindResult>();
        private String                      key;
        private final Strength              upToStrength;

        MatchIterator(Slob[] slobs, String key, Slob preferred, Strength upToStrength) {
            this.key = key;
            this.upToStrength = upToStrength;
            for (Slob slob : slobs) {
                iterators.put(slob, nextResult(slob));
            }
            comparator = new MergeBufferItemComparator(preferred);
            mergeBuffer = new ArrayList<MergeBufferItem>();
            for (Slob slob : slobs) {
                updateMergeBuffer(slob);
            }
        }

        Strength nextStrength(Strength s) {
            switch (s) {
                case IDENTICAL: return Strength.QUATERNARY;
                case QUATERNARY: return Strength.TERTIARY;
                case TERTIARY: return Strength.SECONDARY;
                case SECONDARY: return Strength.PRIMARY;
                case PRIMARY: return Strength.QUATERNARY_PREFIX;
                case QUATERNARY_PREFIX: return Strength.TERTIARY_PREFIX;
                case TERTIARY_PREFIX: return Strength.SECONDARY_PREFIX;
                case SECONDARY_PREFIX: return Strength.PRIMARY_PREFIX;
                default: return null;
            }
        }


        FindResult nextResult(Slob slob) {
            Strength strength;
            if (!iterators.containsKey(slob)) {
                strength = Strength.QUATERNARY;
            }
            else {
                FindResult currentResult = iterators.get(slob);
                if (currentResult.strength == upToStrength) {
                    strength = null;
                }
                else {
                    strength = nextStrength(currentResult.strength);
                }
            }
            if (strength == null) {
                return null;
            }
            Iterator<Blob> iter;
            try {
                iter = slob.find(this.key, strength);
            }
            catch (Exception ex) {
                L.log(Level.WARNING,
                        String.format("Lookup in %s from %s failed",
                                slob.getId(), slob.file.getAbsoluteFile()), ex);
                iter = EMPTY_RESULT;
            }

            return new FindResult(iter, strength);
        }

        void updateMergeBuffer(Slob slob) {
            long t0 = System.currentTimeMillis();
            FindResult findResult = iterators.get(slob);
            if (findResult == null) {
                return;
            }
            Iterator<Blob> iter = findResult.iterator;
            while (iter.hasNext()) {
                Blob maybeNext = iter.next();
                String dedupKey = mkDedupKey(maybeNext);
                if (seen.contains(dedupKey)) {
                    L.fine("Ignoring dupe " + dedupKey);
                    continue;
                } else {
                    seen.add(dedupKey);
                    mergeBuffer.add(new MergeBufferItem(maybeNext, findResult.strength));
                    L.fine("Updated merge buffer (found next) in " + (System.currentTimeMillis() - t0));
                    return;
                }
            }
            if (!iter.hasNext()) {
                iterators.put(slob, nextResult(slob));
                updateMergeBuffer(slob);
            }
            L.fine("Updated merge buffer in " + (System.currentTimeMillis() - t0));
        }

        private String mkDedupKey(Blob b) {
            return String.format("%s:%s#%s", b.owner.getId(), b.id, b.fragment);
        }

        public Blob peek() {
            try {
                Collections.sort(mergeBuffer, comparator);
            }
            catch (Exception ex) {
                L.log(Level.SEVERE, "Sorting merge buffer failed", ex);
            }
            return mergeBuffer.get(0).blob;
        }

        public Blob next() {
            try {
                Collections.sort(mergeBuffer, comparator);
            }
            catch (Exception ex) {
                L.log(Level.SEVERE, "Sorting merge buffer failed", ex);
            }
            MergeBufferItem result = mergeBuffer.remove(0);
            Blob blob = result.blob;
            updateMergeBuffer(blob.owner);
            return blob;
        }

        public boolean hasNext() {
            return mergeBuffer.size() > 0;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public static PeekableIterator<Blob> find(String key, Slob[] slobs, Slob preferred, Strength upToStrength) {
        long t0 = System.currentTimeMillis();
        MatchIterator result = new MatchIterator(slobs, key, preferred, upToStrength);
        L.fine("find returned in " + (System.currentTimeMillis() - t0));
        return result;
    }

    public static PeekableIterator<Blob> find(String key, Slob[] slobs, Slob preferred) {
        return find(key, slobs, preferred, null);
    }

    public static PeekableIterator<Blob> find(String key, Slob ... slobs) {
        return find(key, slobs, null, null);
    }

}
