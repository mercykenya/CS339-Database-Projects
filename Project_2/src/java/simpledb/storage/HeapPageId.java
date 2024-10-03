package simpledb.storage;

import java.util.Objects;

/**
 * Unique identifier for HeapPage objects.
 */
public class HeapPageId implements PageId {
    private final int tableId;
    private final int pageNumber;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo    The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // TODO: some code goes here
        // Set the table ID and page number for this HeapPageId
        this.tableId = tableId;
        this.pageNumber = pgNo;
    }

    /**
     * @return the table associated with this PageId
     */
    public int getTableId() {
        // TODO: some code goes here
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *         this PageId
     */
    public int getPageNumber() {
        // TODO: some code goes here
        return pageNumber;
    }

    /**
     * @return a hash code for this page, represented by a combination of
     *         the table number and the page number (needed if a PageId is used as a
     *         key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    //Come back and have a look at it
    public int hashCode() {
        // TODO: some code goes here
        // Generate a hash code for the tableId and pageNo fields using Objects.hash()
        int hash = Objects.hash(tableId, pageNumber);
        return hash;
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *         ids are the same)
     */
    public boolean equals(Object o) {
        // TODO: some code goes here
        // check if the object is the same as this object
        if (o == this) {
            return true;
        }

        // check if the object is an instance of HeapPageId
        if (!(o instanceof HeapPageId)) {
            return false;
        }

        // cast the object to HeapPageId
        HeapPageId other = (HeapPageId) o;
        // compare tableId and pageNo fields
        return (tableId == other.tableId) && (pageNumber == other.pageNumber);
    }

    /**
     * Return a representation of this object as an array of
     * integers, for writing to disk.  Size of returned array must contain
     * number of integers that corresponds to number of args to one of the
     * constructors.
     */
    public int[] serialize() {
        int[] data = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
