package simpledb.server;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import simpledb.materialize.NoDupsSortPlan;
import simpledb.metadata.TableMgr;
import simpledb.parse.QueryData;
import simpledb.planner.BasicQueryPlanner;
import simpledb.query.*;
import simpledb.record.RecordFile;
import simpledb.record.Schema;
import simpledb.record.TableInfo;
import simpledb.tx.Transaction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

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
        file.setInt("age", 25);
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
        System.out.println("Tuples: " + count);
        Scan open = queryPlan.open();

        for (int i = 0; i < count; i++) {
            open.next();
            StringConstant name = (StringConstant) open.getVal("name");
            IntConstant age = (IntConstant) open.getVal("age");
            FloatConstant salary = (FloatConstant) open.getVal("salary");
            System.out.println(name + ", " + age + " years, " + salary + "$ USD/hr");
        }
    }

    @Test
    public void testNoDuplicates() {
        Collection<String> distinctFields = new ArrayList<>();
        distinctFields.add("age");

        Collection<String> tables = new ArrayList<>();
        tables.add("mytable");

        QueryData data = new QueryData(distinctFields, tables, new Predicate());
        Plan plan = new BasicQueryPlanner().createPlan(data, tx);
        Plan queryPlan = new NoDupsSortPlan(plan, distinctFields, tx);

        int count = queryPlan.recordsOutput();
        System.out.println("Unique tuples distincted by age: " + count);

        Scan open = queryPlan.open();

        System.out.println("The distinct ages:");
        for (int i = 0; i < count; i++) {
            open.next();
            IntConstant age = (IntConstant) open.getVal("age");
            System.out.println(age);
        }
    }
}