import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SMSCDRReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {

    protected void reduce(Text key, Iterable<IntWritable> values, Reducer.Context context) throws java.io.IOException, InterruptedException {
        int sum = 0;
        System.out.println("Key = "+key);
        System.out.println("values count = "+values.toString());
        for (IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}