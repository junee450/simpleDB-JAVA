package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private Op aggregationOp;
    private TupleDesc td;
    private HashMap<Field,Integer> groupMap;
    private HashMap<Field, List<Integer>> avgMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.aggregationOp = what;
        groupMap = new HashMap<>();
        avgMap = new HashMap<>();
        if(gbfield == NO_GROUPING){
            td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"aggval"});
        }else{
            td = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},new String[]{"gbval","aggval"});
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = gbfield == NO_GROUPING ? null : tup.getField(gbfield);
        IntField aField = (IntField)tup.getField(afield);
        int val = aField.getValue();
        switch (aggregationOp){
            case MAX:
                groupMap.put(gbField,Math.max(groupMap.getOrDefault(gbField,val),val));
                break;
            case MIN:
                groupMap.put(gbField,Math.min(groupMap.getOrDefault(gbField,val),val));
                break;
            case SUM:
                groupMap.put(gbField,groupMap.getOrDefault(gbField,0)+val);
                break;
            case COUNT:
                groupMap.put(gbField,groupMap.getOrDefault(gbField,0)+1);
                break;
            case AVG:
                if(avgMap.containsKey(gbField)){
                    List<Integer> tmp = avgMap.get(gbField);
                    tmp.add(val);
                    avgMap.put(gbField,tmp);
                }else{
                    List<Integer> tmp = new ArrayList<>();
                    tmp.add(val);
                    avgMap.put(gbField,tmp);
                }
                break;
            default:
                throw new IllegalArgumentException("Wrong Oprator!");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        if(aggregationOp == Op.AVG){
            for (Field field : avgMap.keySet()){
                List<Integer> list = avgMap.get(field);
                int sum = 0;
                int avg = 0;
                for(int i = 0;i<list.size();i++){
                    sum += list.get(i);
                }
                avg = sum/list.size();
                Tuple tuple = new Tuple(td);
                if(gbfield == NO_GROUPING){
                    tuple.setField(0,new IntField(avg));
                }else {
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(avg));
                }
                tuples.add(tuple);
            }
        }else{
            for (Field field : groupMap.keySet()){
                Tuple tuple = new Tuple(td);
                if(gbfield == NO_GROUPING){
                    tuple.setField(0,new IntField(groupMap.get(field)));
                }else{
                    tuple.setField(0,field);
                    tuple.setField(1,new IntField(groupMap.get(field)));
                }
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td,tuples);
    }

}
