package lesson.hadoop;

import eu.bitwalker.useragentutils.OperatingSystem;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

public class CityBrowserWritable implements WritableComparable<CityBrowserWritable> {

    int city;
    OperatingSystem os;

    public CityBrowserWritable() {
    }

    public CityBrowserWritable(int city, OperatingSystem os) {
        this.city = city;
        this.os = os;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(city);
        out.writeUTF(os.name());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        city = in.readInt();
        os = OperatingSystem.valueOf(in.readUTF());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CityBrowserWritable that = (CityBrowserWritable) o;

        return city == that.city && os == that.os;

    }

    @Override
    public int hashCode() {
        int result = city;
        result = 31 * result + os.hashCode();
        return result;
    }

    @Override
    public int compareTo(CityBrowserWritable o) {
        return Comparator.<CityBrowserWritable>comparingInt(obj->obj.city).thenComparingInt(obj->obj.os.ordinal()).compare(this, o);
    }

    @Override
    public String toString() {
        return city + " " + os.getName();
    }
}
