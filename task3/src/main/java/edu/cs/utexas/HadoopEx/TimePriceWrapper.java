package edu.cs.utexas.HadoopEx;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TimePriceWrapper implements WritableComparable <TimePriceWrapper>{
    private IntWritable time;
    private DoubleWritable price;

    public TimePriceWrapper() {
        this.time = new IntWritable();
        this.price = new DoubleWritable();
    }

    public TimePriceWrapper(IntWritable time, DoubleWritable price) {
        this.time = time;
        this.price = price;
    }

    public IntWritable getTime() {
        return this.time;
    }

    public DoubleWritable getPrice() {
        return this.price;
    }

    public int compareTo(TimePriceWrapper other) {
        double diff = this.price.get() - other.price.get();
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        }
        return 0;
    }
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((price == null) ? 0 : price.hashCode());
        result = prime * result + ((time == null) ? 0 : time.hashCode());
        return result;
    }
    public void readFields(DataInput in) throws IOException {
        this.time = new IntWritable(in.readInt());
        this.price = new DoubleWritable(in.readDouble());
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.time.get());
        out.writeDouble(this.price.get());
    }
}