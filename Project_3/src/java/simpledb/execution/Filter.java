package simpledb.execution;

import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private Predicate predicate;
    private OpIterator iterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // DONE
        this.predicate=p;
        this.iterator=child;   
    }

    public Predicate getPredicate() {
        // DONE
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // DONE
        return iterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // DONE
        super.open();
        iterator.open();
    }

    public void close() {
        // DONE
        iterator.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // DONE
        iterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // DONE

        // Iterate over tuples from the child operator
        while (iterator.hasNext()) {
        Tuple tuple = iterator.next();
        // If the tuple passes the filter, return it
        if (predicate.filter(tuple)) {
            return tuple;
        }
    }
    // If no tuples pass the filter, return null
    return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // DONE
        return new OpIterator[]{iterator};
        
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // DONE
        iterator=children[0];
    }

}
