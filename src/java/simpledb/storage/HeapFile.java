package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;

    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pNo = pid.getPageNumber();
        RandomAccessFile f = null;
        try {
            f = new RandomAccessFile(file,"r");
            if((long) (pNo + 1) *BufferPool.getPageSize() > f.length()){
                f.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            f.seek((long) pNo * BufferPool.getPageSize());
            // big end
            int read = f.read(bytes,0,BufferPool.getPageSize());
            if(read != BufferPool.getPageSize()){
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pNo, read));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(),pid.getPageNumber());
            return new HeapPage(id,bytes);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                f.close();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNumber = page.getId().getPageNumber();
        if (pageNumber > numPages()) {
            throw new IllegalArgumentException("page is not in the heap file or page'id in wrong");
        }

        RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
        randomAccessFile.seek(pageNumber * BufferPool.getPageSize());
        byte[] pageData = page.getPageData();
        randomAccessFile.write(pageData);
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.floor(file.length()*1.0/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        List<Page> dirtyPages = new ArrayList<>();
        for(int i = 0;i<numPages();i++){
            PageId pageId = new HeapPageId(getId(),i);
            HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,pageId,Permissions.READ_WRITE);
            if (heapPage.getNumEmptySlots() == 0) continue;
            heapPage.insertTuple(t);
            dirtyPages.add(heapPage);
            return dirtyPages;
        }
        // 所有页都满了
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file,true));
        byte[] data = HeapPage.createEmptyPageData();
        bufferedOutputStream.write(data);
        bufferedOutputStream.close();
        // 再次写入
        PageId pid = new HeapPageId(getId(), numPages() - 1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.insertTuple(t);
        dirtyPages.add(page);
        return dirtyPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> dirtyPages = new ArrayList<>();
        HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid,t.getRecordId().getPageId(),Permissions.READ_WRITE);
        heapPage.deleteTuple(t);
        if (heapPage == null) throw new DbException("the tuple is not a member of this table");
        dirtyPages.add(heapPage);
        return dirtyPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIterator dbFileIterator = new HeapFileIterator(this,tid);
        return dbFileIterator;
    }

    private static final class HeapFileIterator implements DbFileIterator {

        private final HeapFile heapFile;

        private final TransactionId tid;

        private Iterator<Tuple> it;

        private int whichPage;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.heapFile = file;
            this.tid = tid;
        }

        /**
         * Opens the iterator
         *
         * @throws DbException when there are problems opening/accessing the database.
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            whichPage = 0;
            it = getPageTuples(whichPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws DbException, TransactionAbortedException {
            if (pageNumber >= 0 && pageNumber < heapFile.numPages()) {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNumber);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            } else {
                throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber, heapFile.getId()));
            }
        }

        /**
         * @return true if there are more tuples available, false if no more tuples or iterator isn't open.
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) {
                return false;
            }
            if (!it.hasNext()) {
                while (whichPage < (heapFile.numPages() - 1)) {
                    whichPage++;
                    it = getPageTuples(whichPage);
                    if (it.hasNext()) {
                        return true;
                    }
                }
                if (whichPage >= (heapFile.numPages() - 1)) {
                    return false;
                }
            } else {
                return true;
            }
            return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         *
         * @return The next tuple in the iterator.
         * @throws NoSuchElementException if there are no more tuples
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null || !it.hasNext()) {
                throw new NoSuchElementException();
            }
            return it.next();
        }

        /**
         * Resets the iterator to the start.
         *
         * @throws DbException When rewind is unsupported.
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * Closes the iterator.
         */
        @Override
        public void close() {
            it = null;
        }
    }

}

