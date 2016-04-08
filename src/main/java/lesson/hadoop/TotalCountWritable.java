package lesson.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TotalCountWritable implements Writable {

    private int counter;
    private long total;

    public TotalCountWritable() {
    }

    public TotalCountWritable(int counter, long total) {
        this.counter = counter;
        this.total = total;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(counter);
        out.writeLong(total);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        counter = in.readInt();
        total = in.readLong();
    }

    public int getCounter() {
        return counter;
    }

    public long getTotal() {
        return total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TotalCountWritable that = (TotalCountWritable) o;

        if (counter != that.counter) return false;
        return total == that.total;

    }

    @Override
    public int hashCode() {
        int result = counter;
        result = 31 * result + (int) (total ^ (total >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return total/counter + "," + total;
    }
}
