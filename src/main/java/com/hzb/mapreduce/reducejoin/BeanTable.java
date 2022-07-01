package com.hzb.mapreduce.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author huzhibin
 * @date 2022-07-01 14:07
 */
public class BeanTable implements Writable {
    private String id;
    private String pid;
    private int amount;
    private String pname;
    private String filename;

    public BeanTable() {
    }

    @Override
    public String toString() {
        return id + '\t' + pname +'\t'+
                amount;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }




    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(filename);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.pid= dataInput.readUTF();
        this.amount= dataInput.readInt();
        this.pname= dataInput.readUTF();
        this.filename= dataInput.readUTF();
    }

}
