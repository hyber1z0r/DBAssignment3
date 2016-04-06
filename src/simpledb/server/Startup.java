package simpledb.server;

import simpledb.metadata.TableMgr;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.remote.*;
import simpledb.tx.Transaction;

import java.rmi.registry.*;

public class Startup {

    private static final String tableName = "mytable";

    public static void main(String args[]) throws Exception {
        // configure and initialize the database
        SimpleDB.init(args[0]);

        // create a registry specific for the server on the default port
        Registry reg = LocateRegistry.createRegistry(1099);

        // and post the server entry in it
        RemoteDriver d = new RemoteDriverImpl();
        reg.rebind("simpledb", d);

        System.out.println("database server ready");

        Transaction tx = new Transaction();
        Schema schema = new Schema();
        schema.addStringField("name", TableMgr.MAX_NAME);
        schema.addIntField("age");
        schema.addFloatField("salary");
        schema.addBooleanField("single");
        SimpleDB.mdMgr().createTable(tableName, schema, tx);
        TableInfo tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        RecordFile file = new RecordFile(tableInfo, tx);
        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        file.setFloat("salary", 10.99f);
        file.setBoolean("single", true);
        file.insert();
        file.setString("name", "John");
        file.setInt("age", 23);
        file.setFloat("salary", 7.99f);
        file.setBoolean("single", true);
        file.insert();
        file.setString("name", "Ellen");
        file.setInt("age", 25);
        file.setFloat("salary", 15f);
        file.setBoolean("single", false);
        tx.commit();
    }
}
