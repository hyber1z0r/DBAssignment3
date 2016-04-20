package simpledb.server;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import simpledb.metadata.TableMgr;
import simpledb.query.*;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.io.IOException;

/**
 * Created by jakobgaardandersen on 20/04/2016.
 */
public class ClientTest {

    private static Transaction tx;
    private static final String tableName = "mytable";
    static final String dbName = "mytestdb";

    @BeforeClass
    public static void setupClass() {
        SimpleDB.init(dbName);
        tx = new Transaction();
        Schema schema = new Schema();
        schema.addStringField("name", TableMgr.MAX_NAME);
        schema.addIntField("age");
        schema.addFloatField("salary");
        SimpleDB.mdMgr().createTable(tableName, schema, tx);

        TableInfo tableInfo = SimpleDB.mdMgr().getTableInfo(tableName, tx);
        RecordFile file = new RecordFile(tableInfo, tx);

        file.insert();
        file.setString("name", "Peter");
        file.setInt("age", 23);
        file.setFloat("salary", 10.99f);
        file.insert();
        file.setString("name", "John");
        file.setInt("age", 23);
        file.setFloat("salary", 20.99f);
        file.insert();
        file.setString("name", "Ellen");
        file.setInt("age", 25);
        file.setFloat("salary", 5.50f);

        tx.commit();
    }


    @AfterClass
    public static void tearDownClass() throws IOException {
        SimpleDB.dropDatabase(dbName);
    }

    @After
    public void tearDown() {
        tx.commit();
    }

    @Test
    public void testGetData() {
        Plan queryPlan = SimpleDB.planner().createQueryPlan("SELECT name, age, salary FROM mytable", tx);
        int count = queryPlan.recordsOutput();
        System.out.println(count);
        Scan open = queryPlan.open();

        for (int i = 0; i < count; i++) {
            open.next();
            StringConstant name = (StringConstant) open.getVal("name");
            IntConstant age = (IntConstant) open.getVal("age");
            FloatConstant salary = (FloatConstant) open.getVal("salary");
            System.out.println(name + ", " + age + " years, " + salary + "$ USD/hr");
        }
    }
}