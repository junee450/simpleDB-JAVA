package simpledb;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionId;

import java.io.File;

public class test {
    public static void main(String[] args) {
        Type[] type= new Type[]{Type.INT_TYPE,Type.INT_TYPE,Type.INT_TYPE};
        String[] name = new String[]{"a","b","c"};
        TupleDesc descriptor = new TupleDesc(type,name);

        // create the table, associate it with some_data_file.txt
        // and tell the catalog about the schema of this table.
        HeapFile table1 = new HeapFile(new File("some_data_file.txt"), descriptor);
        Database.getCatalog().addTable(table1, "test");

        // construct the query: we use a simple SeqScan, which spoonfeeds
        // tuples via its iterator.
        TransactionId tid = new TransactionId();
        SeqScan f = new SeqScan(tid, table1.getId());

        try {
            // and run it
            f.open();
            while (f.hasNext()) {
                Tuple tup = f.next();
                System.out.println(tup);
            }
            f.close();
            Database.getBufferPool().transactionComplete(tid);
        } catch (Exception e) {
            System.out.println("Exception : " + e);

        }
    }
}
