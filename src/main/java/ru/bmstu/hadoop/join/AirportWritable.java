package ru.bmstu.hadoop.join;

import org.apache.hadoop.io.Writable;
import ru.bmstu.hadoop.validators.Validator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Optional;

public class AirportWritable implements Writable {

    private static final int NAME_CSV_IDX = 1;
    private static final int CODE_CSV_IDX = 0;
    private int code;
    private String name;

    @SuppressWarnings("unused")
    public AirportWritable() { }

    private AirportWritable(int code, String name) {
        this.code = code;
        this.name = name;
    }

    static Optional<AirportWritable> read(String input) {
        String[] splitted = input.replaceAll("\"", "").split(",", 2);

        Optional<Integer> code = Validator.validateInteger(splitted[CODE_CSV_IDX]);
        return code.map(v -> new AirportWritable(v, splitted[NAME_CSV_IDX]));
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(code);
        out.writeUTF(name);
    }

    public void readFields(DataInput in) throws IOException {
        code = in.readInt();
        name = in.readUTF();
    }

    String getName() {
        return name;
    }

    int getCode() {
        return code;
    }
}