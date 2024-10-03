package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.NoSuchElementException;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {
    private TransactionId tid;
    private int tableId;
    private String tableAlias;

    private DbFileIterator iterator;

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // TODO: some code goes here
        this.tid = tid;
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return return the table name of the table the operator scans. This should
     *         be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        // TODO: some code goes here
        if(tableAlias == null)
            return "null";
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     *
     * @param tableid    the table to scan.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // TODO: some code goes here
        this.tableId = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        iterator = Database.getCatalog().getDatabaseFile(tableId).iterator(tid);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        // Get the TupleDesc of the underlying HeapFile
        TupleDesc td = Database.getCatalog().getTupleDesc(tableId);

        // Create a new TupleDesc with the same types and sizes as the original TupleDesc,
        // but with field names prefixed with the table alias
        Type[] types = new Type[td.numFields()];
        String[] names = new String[td.numFields()];
        for (int i = 0; i < td.numFields(); i++) {
            types[i] = td.getFieldType(i);
            names[i] = tableAlias + "." + td.getFieldName(i);
        }
        return new TupleDesc(types, names);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if (iterator == null) {
            throw new IllegalStateException("Iterator is closed");
        }
        // check if there are more tuples in the iterator
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // TODO: some code goes here
        //check if the iterator is open, throw exception if not
        if (iterator == null) {
            throw new IllegalStateException("Iterator is closed");
        }
        // return the next tuple in the iterator
        return iterator.next();
    }

    public void close() {
        // TODO: some code goes here
        // close the iterator if it's not null
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // TODO: some code goes here
        // check if the iterator is open, throw exception if not
        if (iterator == null) {
            throw new IllegalStateException("Iterator is closed");
        }
        // rewind the iterator to the beginning
        iterator.rewind();
    }
}
