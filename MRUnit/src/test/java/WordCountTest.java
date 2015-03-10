import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WordCountTest {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {

        WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
        WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();


        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws Exception {
        String inputString = "This map feature can be used when This map tasks";
        mapDriver.withInput(new LongWritable(), new Text(inputString));
        mapDriver.withOutput(new Text("map"), new IntWritable(1));
        mapDriver.withOutput(new Text("feature"), new IntWritable(1));
        mapDriver.withOutput(new Text("used"), new IntWritable(1));
        mapDriver.withOutput(new Text("when"), new IntWritable(1));
        mapDriver.withOutput(new Text("map"), new IntWritable(1));
        mapDriver.withOutput(new Text("tasks"), new IntWritable(1));

        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws Exception {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("map"), values);
        reduceDriver.withOutput(new Text("map"), new IntWritable(2));

        reduceDriver.runTest();
    }

}

