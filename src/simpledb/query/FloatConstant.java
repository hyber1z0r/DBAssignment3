package simpledb.query;

public class FloatConstant implements Constant {
    private Float val;

    public FloatConstant(float n) {
        val = new Float(n);
    }

    public Object asJavaVal() {
        return val;
    }

    public boolean equals(Object obj) {
        FloatConstant fc = (FloatConstant) obj;
        return fc != null && val.equals(fc.val);
    }

    public int compareTo(Constant c) {
        FloatConstant fc = (FloatConstant) c;
        return val.compareTo(fc.val);
    }

    public int hashCode() {
        return val.hashCode();
    }

    public String toString() {
        return val.toString();
    }
}
