package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

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

    private final File f;
    private final TupleDesc td;
    private final int tableid;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.tableid = f.getAbsoluteFile().hashCode();
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
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
        return tableid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        HeapPageId id = (HeapPageId) pid;

        try (BufferedInputStream bis = new BufferedInputStream(new FileInputStream(f))) {
            byte[] pageBuf = new byte[BufferPool.getPageSize()];
            if (bis.skip((long) id.getPageNumber() * BufferPool.getPageSize()) != (long) id
                    .getPageNumber() * BufferPool.getPageSize()) {
                throw new IllegalArgumentException(
                        "Unable to seek to correct place in heapfile");
            }
            int retval = bis.read(pageBuf, 0, BufferPool.getPageSize());
            if (retval == -1) {
                throw new IllegalArgumentException("Read past end of table");
            }
            if (retval < BufferPool.getPageSize()) {
                throw new IllegalArgumentException("Unable to read "
                        + BufferPool.getPageSize() + " bytes from heapfile");
            }
            Debug.log(1, "HeapFile.readPage: read page %d", id.getPageNumber());
            return new HeapPage(id, pageBuf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        // Close the file on success or error
        // Ignore failures closing the file
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // DONE
        PageId pid = page.getId();

        RandomAccessFile File = new RandomAccessFile(f, "rw");
        File.seek(BufferPool.getPageSize() * pid.getPageNumber());
        File.write(page.getPageData(), 0, BufferPool.getPageSize());
        File.close();
        //page.markDirty(false, null);
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // XXX: this seems to be rounding it down. isn't that wrong?
        // XXX: (marcua) no - we only ever write full pages
        return (int) (f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // DONE
        // Create an empty list to store the modified pages
        List<Page> pages = new ArrayList<>();

        // Get the buffer pool instance
        BufferPool buffer = Database.getBufferPool();

        // Loop through the pages of this HeapFile
        for (int i = 0; i < numPages(); i++) {
            // Get the page ID
            PageId pid = new HeapPageId(this.getId(), i);

            // Get a read-only copy of the page from the buffer pool
            HeapPage page = (HeapPage) buffer.getPage(tid, pid, Permissions.READ_ONLY);

            // Check if there are any empty slots on the page
            if (page.getNumUnusedSlots() > 0) {
                // Get a read-write copy of the page from the buffer pool
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

                // Insert the tuple into the page
                page.insertTuple(t);

                // Add the modified page to the list of modified pages
                pages.add(page);

                // Exit the loop
                break;
            }
        }

        // If no modified pages were found, create a new page and insert the tuple into it
        if (pages.isEmpty()) {
            // Create a new page ID
            HeapPageId pid = new HeapPageId(this.getId(), this.numPages());

            // Create a new empty page
            HeapPage heapPage = new HeapPage(pid, HeapPage.createEmptyPageData());

            // Insert the tuple into the new page
            heapPage.insertTuple(t);

            // Write the new page to disk
            this.writePage(heapPage);

            // Add the new page to the list of modified pages
            pages.add(heapPage);
        }

        // Return the list of modified pages
        return pages;
       
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // DONE

        // Get the PageId of the page containing the Tuple to be deleted
        PageId pid = t.getRecordId().getPageId();
    
        // Get the HeapPage containing the Tuple and set it as READ_WRITE to delete the Tuple
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    
        // Delete the Tuple from the HeapPage
        page.deleteTuple(t);
    
        // Return a List containing the modified page (HeapPage) as its only element
        List<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
        
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

/**
 * Helper class that implements the Java Iterator for tuples on a HeapFile
 */
class HeapFileIterator extends AbstractDbFileIterator {

    Iterator<Tuple> it = null;
    int curpgno = 0;

    final TransactionId tid;
    final HeapFile hf;

    public HeapFileIterator(HeapFile hf, TransactionId tid) {
        this.hf = hf;
        this.tid = tid;
    }

    public void open() {
        curpgno = -1;
    }

    @Override
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (it != null && !it.hasNext())
            it = null;

        while (it == null && curpgno < hf.numPages() - 1) {
            curpgno++;
            HeapPageId curpid = new HeapPageId(hf.getId(), curpgno);
            HeapPage curp = (HeapPage) Database.getBufferPool().getPage(tid,
                    curpid, Permissions.READ_ONLY);
            it = curp.iterator();
            if (!it.hasNext())
                it = null;
        }

        if (it == null)
            return null;
        return it.next();
    }

    public void rewind() {
        close();
        open();
    }

    public void close() {
        super.close();
        it = null;
        curpgno = Integer.MAX_VALUE;
    }
}
