package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
//import simpledb.common.DbException;
//import simpledb.storage.Page;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    final int numPages;   // number of pages -- currently, not enforced
    final ConcurrentMap<PageId, Page> pages; // hash table storing current pages in memory

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // XXX Yuan points out that HashMap is not synchronized, so this is buggy.
        // XXX TODO(ghuo): do we really know enough to implement NO STEAL here?
        //     won't we still evict pages?
        Page p;
        synchronized (this) {
            p = pages.get(pid);
            if (p == null) {
                if (pages.size() >= numPages) {
                    evictPage();
                }

                p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                pages.put(pid, p);
            }
        }

        return p;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // DONE

        // Get the DbFile corresponding to the table
        DbFile File = Database.getCatalog().getDatabaseFile(tableId);
        // Call insertTuple() on the database file to insert the tuple and get a list of modified pages
        List<Page> modifiedPages = File.insertTuple(tid, t);
    
        // Mark all modified pages as dirty and update the buffer pool
        for (Page p : modifiedPages) {
            // Mark the page as dirty to indicate that it has been modified
            p.markDirty(true, tid);

            // Get the page ID
            PageId pid = p.getId();
            pages.put(pid, p);
            
        }
        
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
            //DONE
            int tableId= t.getRecordId().getPageId().getTableId();

            // Get the DbFile corresponding to the table
            DbFile File = Database.getCatalog().getDatabaseFile(tableId);
            // Return a List containing the modified page (HeapPage) as its only element
            List<Page> modifiedPages = File.deleteTuple(tid, t);
    
            // Mark all modified pages as dirty and update the buffer pool
            for (Page p : modifiedPages) {
                // Mark the page as dirty to indicate that it has been modified
                p.markDirty(true, tid);

                // Get the page ID
                PageId pid = p.getId();
                this.pages.put(pid, p);
            
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // DONE
        for (PageId pid : pages.keySet()) {
            Page page = pages.get(pid);
            if (page.isDirty() != null) {
                DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
                file.writePage(page);
                page.markDirty(false, null);
            }
        }

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void removePage(PageId pid) {
        // DONE
        Page p = pages.remove(pid);
        if (p == null) {
            // Page is not in the buffer pool
            return;
        }
        p.markDirty(false, null);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // DONE
        Page page = pages.get(pid);
        if (page == null) {
            // Page not in buffer pool, nothing to flush
            return;
        }
        if (page.isDirty() != null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            dbFile.writePage(page);
            page.markDirty(false, null);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // DONE
        // Find a victim page to evict
    Page victimPage = null;
    for (Page p : pages.values()) {
        if (p.isDirty() == null) {
            // Found an eligible victim page
            victimPage = p;
            break;
        }
    }
    if (victimPage == null) {
        throw new DbException("All pages in buffer pool are dirty.");
    }

    // Flush the victim page to disk if it is dirty
    PageId victimPageId = victimPage.getId();
    try {
        flushPage(victimPageId);
    } catch (IOException e) {
        throw new DbException("Error flushing page to disk.");
    }

    // Remove the victim page from the buffer pool
    pages.remove(victimPageId);
   }
}
