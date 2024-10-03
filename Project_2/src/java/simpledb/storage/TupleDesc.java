package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private TDItem[] tdItems;

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return An iterator which iterates over all the field TDItems
     *         that are included in this TupleDesc
     */

    //  Returns iterator that:
    //      Goes through TDItem objects.
    public Iterator<TDItem> iterator() {
        // TODO: some code goes here

        return new Iterator<TDItem>(){
            private int index=0;
            //Methods to use

            //1.See if we have any more TDItems in the TupleDesc to iterate over
            @Override
            public boolean hasNext(){
                if(index <tdItems.length){
                    return true;
                }else{
                    return false;
                }
            }
            //2. Return next TDItem in TupleDesc/throw exception of no more are left
            @Override
            public TDItem next() throws NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more TDItems left in TupleDesc.");
                }
                TDItem item = tdItems[index];
                index++;
                return item;
            }
        };
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */

    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // TODO: some code goes here

        tdItems = new TDItem[typeAr.length];

        //Iterate and create new TDitem initialzing the fields correctly
        for (int i = 0; i < typeAr.length; i++) {
            tdItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // TODO: some code goes here

        this.tdItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            tdItems[i] = new TDItem(typeAr[i], null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // TODO: some code goes here
        return tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // TODO: some code goes here
        if (i < 0 || i >= tdItems.length) {
            throw new NoSuchElementException("Invalid field reference: " + i);
        }
        return tdItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // TODO: some code goes here
        if (i < 0 || i >= tdItems.length) {
            throw new NoSuchElementException("Invalid field index: " + i);
        }
        return tdItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int indexForFieldName(String name) throws NoSuchElementException {
        // TODO: some code goes here
        // Iterate through each TDItem in the TupleDesc
        for (int i = 0; i < tdItems.length; i++) {
            // If the name of the current TDItem matches the given name, return its index
            if (tdItems[i].fieldName != null && tdItems[i].fieldName.equals(name)) {
                return i;
            }
        }
        // If no matching field name is found, throw an exception
        throw new NoSuchElementException("No field with matching name found.");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // TODO: some code goes here
        int size = 0;
        for (TDItem item : tdItems) {
            // Calculate the size of each field
            int fieldSize = item.fieldType.getLen();
            // Add the size of the field to the total size
            size += fieldSize;
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // TODO: some code goes here
        int size = td1.numFields() + td2.numFields();
        Type[] TypeArray= new Type[size];
        String[] FieldArray= new String[size];

        // Add the fields from the first TupleDesc
        for (int i = 0; i < td1.numFields(); i++) {
            TypeArray[i] = td1.tdItems[i].fieldType;
            FieldArray[i] = td1.tdItems[i].fieldName;
        }

        // Add the fields from the second TupleDesc
        for (int i = 0; i < td2.numFields(); i++) {
            TypeArray[i+td1.numFields()] = td2.tdItems[i].fieldType;
            FieldArray[i+td1.numFields()]= td2.tdItems[i].fieldName;
        }
        return new TupleDesc(TypeArray, FieldArray);
    }


    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // TODO: some code goes here
        // Check if the object is null or not an instance of TupleDesc
        if (o == null || !(o instanceof TupleDesc)) {
            return false;
        }

        // Cast the object to a TupleDesc and compare the number of fields
        TupleDesc other = (TupleDesc) o;
        if (this.numFields() != other.numFields()) {
            return false;
        }

        // Compare the type of each field in this TupleDesc with the corresponding field in the other TupleDesc
        for (int i = 0; i < this.numFields(); i++) {
            if (this.getFieldType(i) != other.getFieldType(i)) {
                return false;
            }
        }

        // All fields are equal, so the TupleDescs are equal
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // TODO: some code goes here
        StringBuilder strBuilder = new StringBuilder();
        for (int i = 0; i < tdItems.length; i++) {
            TDItem item = tdItems[i];
            // append the field type
            strBuilder.append(item.fieldType);
            // if there is a field name
            if (item.fieldName != null) {
                // append it in parenthesis
                strBuilder.append("(").append(item.fieldName).append(")");
            }
            // if this is not the last item, append a comma and a space
            if (i != tdItems.length - 1) {
                strBuilder.append(", ");
            }
        }
        return strBuilder.toString();
    }
}
