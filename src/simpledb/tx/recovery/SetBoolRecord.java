package simpledb.tx.recovery;


import simpledb.server.SimpleDB;
import simpledb.buffer.*;
import simpledb.file.Block;
import simpledb.log.BasicLogRecord;

class SetBoolRecord implements LogRecord {
    private int txnum, offset;
    private boolean val;
    private Block blk;

    /**
     * Creates a new setint log record.
     * @param txnum the ID of the specified transaction
     * @param blk the block containing the value
     * @param offset the offset of the value in the block
     * @param val the new value
     */
    public SetBoolRecord(int txnum, Block blk, int offset, boolean val) {
        this.txnum = txnum;
        this.blk = blk;
        this.offset = offset;
        this.val = val;
    }

    /**
     * Creates a log record by reading five other values from the log.
     * @param rec the basic log record
     */
    public SetBoolRecord(BasicLogRecord rec) {
        txnum = rec.nextInt();
        String filename = rec.nextString();
        int blknum = rec.nextInt();
        blk = new Block(filename, blknum);
        offset = rec.nextInt();
        val = rec.nextBoolean();
    }

    public int writeToLog() {
        Object[] rec = new Object[] {SETBOOL, txnum, blk.fileName(),
                blk.number(), offset, val};
        return logMgr.append(rec);
    }

    public int op() {
        return SETBOOL;
    }

    public int txNumber() {
        return txnum;
    }

    public String toString() {
        return "<SETBOOL " + txnum + " " + blk + " " + offset + " " + val + ">";
    }

    public void undo(int txnum) {
        BufferMgr buffMgr = SimpleDB.bufferMgr();
        Buffer buff = buffMgr.pin(blk);
        buff.setBoolean(offset, val, txnum, -1);
        buffMgr.unpin(buff);
    }
}
