package simpledb.execution;

import simpledb.transaction.TransactionId;

import java.nio.Buffer;
import java.io.IOException;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.common.Type;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private boolean hasFetched = false;
    private Tuple tuple = null;
    private int nInserted = 0;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // DONE
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
    }

    public TupleDesc getTupleDesc() {
        // DONE
        Type[] typeArr = {Type.INT_TYPE};
        String[] fieldArr = {"numInserted"};
        return new TupleDesc(typeArr, fieldArr);

    }

    public void open() throws DbException, TransactionAbortedException {
        // DONE
        super.open();
        child.open();
    }

    public void close() {
        // DONE
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // DONE
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // DONE
        if (hasFetched) {
            return null;
        }

        hasFetched=true;
        BufferPool bufferPool= Database.getBufferPool();
        try {
            while (child.hasNext()) {
                tuple = child.next();
                bufferPool.insertTuple(tid, tableId, tuple);
                nInserted++;
            }
        } catch (IOException e) {
            throw new DbException("IOException during tuple insertion: " + e.getMessage());
        }
        Tuple result=new Tuple(getTupleDesc());
        result.setField(0, new IntField(nInserted));
        return result;
        
    }

    @Override
    public OpIterator[] getChildren() {
        // DONE
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // DONE
        child = children[0];
    }
}
