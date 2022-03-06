package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op op;
    private HashMap<Field,Integer> groupMap;
    private TupleDesc td;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        groupMap = new HashMap<>();
        if(gbfield == NO_GROUPING){
            td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"aggval"});
        }else {
            td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},new String[]{"gbval","aggval"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        StringField aField = (StringField) tup.getField(afield);
        if(op == Op.COUNT){
            groupMap.put(gbField,groupMap.getOrDefault(gbField,0)+1);
        }else{
            throw new IllegalArgumentException("Wrong Oprator!");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        for(Field field : groupMap.keySet()){
            Tuple tuple = new Tuple(td);
            if(gbfield == NO_GROUPING){
                tuple.setField(0,new IntField(groupMap.get(field)));
            }else {
                tuple.setField(0,field);
                tuple.setField(1,new IntField(groupMap.get(field)));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(td,tuples);
    }

}
