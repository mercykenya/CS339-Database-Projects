package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.util.NoSuchElementException;


import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
    private File file;      // the file backing this HeapFile
    private TupleDesc td;   // the schema for tuples in this HeapFile
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
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
        // TODO: some code goes here
        // Generating a unique ID for this heapfile by hashing the absolute file name
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        // Returning the TupleDesc object of this HeapFile
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here

        if (pid == null || pid.getPageNumber() < 0 || pid.getPageNumber() >= numPages()) {
            throw new IllegalArgumentException("Invalid page id: " + pid);
        }
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            // Calculate the offset in the file corresponding to the specified page id
            int offset = pid.getPageNumber() * BufferPool.getPageSize();
            // Set the file pointer to the correct position
            raf.seek(offset);
            // Read the bytes corresponding to the page from disk
            byte[] data = new byte[BufferPool.getPageSize()];
            raf.read(data, 0, BufferPool.getPageSize());
            // Close the file
            raf.close();
            // Return a new HeapPage object constructed from the bytes read from disk
            return new HeapPage((HeapPageId) pid, data);
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalArgumentException("Unable to read page from file: " + pid);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        // Calculate the file size in bytes
        long fileSize = file.length();
        // Calculate the number of pages in the file
        int numPages = (int) Math.ceil((double) fileSize / BufferPool.getPageSize());
        return numPages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this.getId(), this.numPages());
    }
    

    private class HeapFileIterator implements DbFileIterator {
  
        TransactionId tid;
        int pageCounter;
        int tableId;
        int numPages;
        Page page;
        Iterator<Tuple> tuples;
        HeapPageId pid;

        public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
            this.tid = tid;
            this.pageCounter = 0;
            this.tableId = tableId;
            this.numPages = numPages;
        }

        private Iterator<Tuple> getTuples(int pageNumber) throws  DbException, TransactionAbortedException {
            pid = new HeapPageId(tableId, pageNumber);
            HeapPage heapPage = (HeapPage) Database.getBufferPool()
                    .getPage(tid, pid, Permissions.READ_ONLY);
            return heapPage.iterator();
        }

        public void open() throws DbException, TransactionAbortedException {
            pageCounter = 0;
            tuples = getTuples(pageCounter);
        }

       
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // If there are no tuples
            if(tuples == null)
                return false;
            // Check if tuple has next
            if(tuples.hasNext())
                return true;
            // Check if all pages are iterated
            if(pageCounter + 1 >= numPages)
                return false;
            // Else check if there is next page
            // If Page is exhausted get new page tuples
            while(pageCounter + 1 < numPages && !tuples.hasNext()){
                // Get tuples of next page
                pageCounter++;
                tuples = getTuples(pageCounter);
            }
            return this.hasNext();
        }

        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // If there are no tuples, throw exception
            if(tuples == null) 
                throw new NoSuchElementException();
            return tuples.next();
        }

        
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            tuples = null;
            pid = null;
        }
    }

}

