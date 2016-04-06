package simpledb.query;

public class BoolConstant implements Constant {
    private Boolean val;

    public BoolConstant(boolean b) {
        val = new Boolean(b);
    }

    public Object asJavaVal() {
        return val;
    }

    public boolean equals(Object obj) {
        BoolConstant bc = (BoolConstant) obj;
        return bc != null && val.equals(bc.val);
    }

    public int compareTo(Constant c) {
        BoolConstant bc = (BoolConstant) c;
        return val.compareTo(bc.val);
    }

    public int hashCode() {
        return val.hashCode();
    }

    public String toString() {
        return val.toString();
    }
}
