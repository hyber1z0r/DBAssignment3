package simpledb.materialize;


import simpledb.query.Plan;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The Plan class for the <i>distinct</i> operator.
 * @author Jakob, Damjan & Vuk
 */
public class NoDupsSortPlan implements Plan {
    private Plan p;
    private Collection<String> distinctfields;
    private Schema sch = new Schema();

    /**
     * Creates a distinct plan for the underlying query.
     * The grouping is determined by the specified
     * collection of group fields,
     * and the aggregation is computed by the
     * specified collection of aggregation functions.
     * @param p a plan for the underlying query
     * @param distinctfields the group fields
     * @param tx the calling transaction
     */
    public NoDupsSortPlan(Plan p, Collection<String> distinctfields, Transaction tx) {
        List<String> distinctlist = new ArrayList<>();
        distinctlist.addAll(distinctfields);
        this.p = new SortPlan(p, distinctlist, tx);
        this.distinctfields = distinctfields;
        for (String fldname : distinctfields)
            sch.add(fldname, p.schema());
    }

    /**
     * This method opens a sort plan for the specified plan.
     * The sort plan ensures that the underlying records
     * will be appropriately distincted.
     * @see simpledb.query.Plan#open()
     */
    public Scan open() {
        Scan s = p.open();
        return new NoDupsSortScan(s, distinctfields);
    }

    /**
     * Returns the number of blocks required to
     * compute the aggregation,
     * which is one pass through the sorted table.
     * It does <i>not</i> include the one-time cost
     * of materializing and sorting the records.
     * @see simpledb.query.Plan#blocksAccessed()
     */
    public int blocksAccessed() {
        return p.blocksAccessed();
    }

    /**
     * Returns the number of groups.  Assuming equal distribution,
     * this is the product of the distinct values
     * for each grouping field.
     * @see simpledb.query.Plan#recordsOutput()
     */
    public int recordsOutput() {
        int numgroups = 1;
        for (String fldname : distinctfields)
            numgroups *= p.distinctValues(fldname);
        return numgroups;
    }

    /**
     * Returns the number of distinct values for the
     * specified field.  If the field is a grouping field,
     * then the number of distinct values is the same
     * as in the underlying query.
     * If the field is an aggregate field, then we
     * assume that all values are distinct.
     * @see simpledb.query.Plan#distinctValues(java.lang.String)
     */
    public int distinctValues(String fldname) {
        if (p.schema().hasField(fldname))
            return p.distinctValues(fldname);
        else
            return recordsOutput();
    }

    /**
     * Returns the schema of the output table.
     * The schema consists of the group fields,
     * plus one field for each aggregation function.
     * @see simpledb.query.Plan#schema()
     */
    public Schema schema() {
        return sch;
    }
}
