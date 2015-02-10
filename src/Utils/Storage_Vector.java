package Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/**
 * @author machongshen
 */
public class Storage_Vector extends KNN_Storage<String,Float, String> implements Writable {
        public Storage_Vector() {
        }

        public Storage_Vector(String v1, Float v2, String v3) {
                this.v1 = v1;
                this.v2 = v2;
                this.v3 = v3;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
                v1 = in.readUTF();
                v2 = in.readFloat();
                v3 = in.readUTF();
        }

        @Override
        public void write(DataOutput out) throws IOException {
                out.writeUTF(v1);
                out.writeFloat(v2);
                out.writeUTF(v3);
        }
}