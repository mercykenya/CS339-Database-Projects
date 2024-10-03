package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
//import simpledb.execution.Aggregator.Op;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;



/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private  OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op op;

    private final Aggregator aggregator;
    private OpIterator it;
    private final TupleDesc td;


    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // DONE

        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.op = aop;

        Type gbFieldType = gfield == Aggregator.NO_GROUPING ? null : child.getTupleDesc().getFieldType(gfield);
        Type aFieldType = child.getTupleDesc().getFieldType(afield);

        if (aFieldType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gbFieldType, afield, aop);
        } else {
            aggregator = new StringAggregator(gfield, gbFieldType, afield, aop);
        }

        if (gfield == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[] {aFieldType}, new String[] {nameOfAggregatorOp(aop) + "(" + child.getTupleDesc().getFieldName(afield) + ")"});
        } else {
            td = new TupleDesc(new Type[] {gbFieldType, aFieldType}, new String[] {child.getTupleDesc().getFieldName(gfield), nameOfAggregatorOp(aop) + "(" + child.getTupleDesc().getFieldName(afield) + ")"});
        }

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // DONE
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     */
    public String groupFieldName() {
        // DONE
        if (gfield== Aggregator.NO_GROUPING) {
            return null;
        }else{
            return child.getTupleDesc().getFieldName(gfield);
        }   
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // DONE
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     */
    public String aggregateFieldName() {
        // DONE
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // DONE
        return op;
     }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // DONE
        super.open();
        child.open();
        while (child.hasNext()) {
        aggregator.mergeTupleIntoGroup(child.next());
         }
        it = aggregator.iterator();
        it.open();   
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // DONE

        if (it.hasNext()){
            return it.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // DONE
        it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // DONE
        return td;
       
    }

    public void close() {
        // DONE
        super.close();
        child.close();
        it.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // DONE
        return new OpIterator[]{child};
    }


    @Override
    public void setChildren(OpIterator[] children) {
        // DONE
        child = children[0];
    }
}
