import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkDemo {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "F:\\hadoop");
        SparkConf conf = new SparkConf();
        conf.setAppName("Spark Demo Job");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("src/main/resources/deneme.txt");

        JavaRDD<String> filteredLines = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String line) throws Exception {
                return line.contains("spark");
            }
        });

        long linesCount = filteredLines.count();
        String firstLine = filteredLines.first();
        System.out.println("Total lines containing spark: " + linesCount);
        System.out.println("First line containing spark: " + firstLine);
        sc.close();
    }
}
