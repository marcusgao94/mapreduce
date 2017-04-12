import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by marcusgao on 4/5/17.
 */
public class SalesMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text word = new Text();
    IntWritable dw = new IntWritable(1);

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String reg = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] " +
                "\"(.+?)\" (\\d{3}) (\\d+|\\S+)";
        Pattern p = Pattern.compile(reg);
        Matcher matcher = p.matcher(line);
        if (!matcher.matches()) {
            System.out.println(line);
            return;
        }
        String request = matcher.group(1);
        word.set(request);
        dw.set(1);
        context.write(word, dw);
    }
}
