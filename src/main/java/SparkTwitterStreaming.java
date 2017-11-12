//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//
//public class SparkTwitterStreaming {
//    private static final String SEARCH_TERM = "nice";
//    private static final String HOME_PATH = "/home/berthold/";
//
//    public static void main(String[] args) {
//        loadOAuthAccess();
//
//        JavaStreamingContext ssc = new JavaStreamingContext("local[2]", "Twitter-Test", new Duration(1000));
//
//        JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(ssc);
//
//        streamContainingSearchTerm(ssc, stream);
//
//    }
//
//    private static void streamContainingSearchTerm(JavaStreamingContext ssc, JavaReceiverInputDStream<Status> stream) {
//        JavaDStream<String> statuses = stream.filter(new Function<Status, Boolean>() {
//
//            public Boolean call(Status status) throws Exception {
//
//                if (status.getText().contains(SEARCH_TERM)) {
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//        }).map(new Function<Status, String>() {
//            public String call(Status status) {
//
//                return status.getText();
//            }
//        });
//        statuses.window(new Duration(10000), new Duration(1000)).print();
//        ssc.checkpoint(HOME_PATH + "/twitter");
//        ssc.start();
//        System.out.println("Twitter stream started");
//    }
//
//    private static void loadOAuthAccess() {
//        Properties properties = new Properties();
//        try {
//            properties.load(new FileInputStream(HOME_PATH + "twitter.properties"));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.getProperties().putAll(properties);
//    }
//}
