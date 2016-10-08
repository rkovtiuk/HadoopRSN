package com.hadoop_rsn.domen.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPair implements WritableComparable<IntPair> {

    private int id;
    private int key;

    public IntPair() {
    }

    public IntPair(int id, int second) {
        set(id, second);
    }

    private void set(int first, int second) {
        this.id = first;
        this.key = second;
    }

    public int getId() {
        return id;
    }

    public int getKey() {
        return key;
    }

    @Override
    public int compareTo(IntPair o) {
        int cmp = compare(id, o.id);
        if (cmp != 0){
            return cmp;
        }

        return compare(key, o.key);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(id);
        dataOutput.write(key);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        key = dataInput.readInt();
    }

    @Override
    public int hashCode() {
        return id * 163 + key;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof IntPair){
            IntPair ip = (IntPair) obj;
            return key == ip.key && id == ip.id;
        }
        return false;
    }

    @Override
    public String toString() {
        return id + "\t" + key;
    }

    public static int compare(int a, int b){
        return (a < b ? -1 : (a == b ? 0 : 1));
    }
}
