import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class SalesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable iw = new IntWritable(0);

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
        int res = 0;
        for (IntWritable dw : values) {
            res += dw.get();
        }
        iw.set(res);
        context.write(key, iw);
    }

}
