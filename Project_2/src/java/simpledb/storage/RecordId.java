package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private final PageId pid;
    private final int tupleNo;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     *
     * @param pid     the pageid of the page on which the tuple resides
     * @param tupleno the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // TODO: some code goes here
        this.pid = pid;
        this.tupleNo = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // TODO: some code goes here
        return tupleNo;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // TODO: some code goes here
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     *
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // TODO: some code goes here

        // Check if o is null or not an instance of RecordId
        if (o == null || !(o instanceof RecordId)) {
            return false;
        }

        // Cast o to a RecordId object
        RecordId other = (RecordId) o;
        // Compare the page ids and tuple numbers of this and other
        return this.pid.equals(other.pid) && this.tupleNo == other.tupleNo;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     *
     * @return An int that is the same for equal RecordId objects.
     */

    // Look at it again
    @Override
    public int hashCode() {
        // TODO: some code goes here
        // Combine the hash codes of the page id and tuple number using XOR (^)
        // The resulting hash code should be consistent for the same page id and tuple number
        // regardless of the order in which they are combined.

        int hash = pid.hashCode() ^ tupleNo;
        return hash;

    }
}
