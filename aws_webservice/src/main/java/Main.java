package cloudbreakers.services;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.lang3.StringEscapeUtils;

import cloudbreakers.pojos.DataSourceUtil;
import cloudbreakers.pojos.DataSourceUtil.DataSourceType;
import cloudbreakers.pojos.PDC_Encryption;
import cloudbreakers.pojos.ScoreCalculatorAndCensor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class Main extends AbstractVerticle {

    public static Logger logger = Logger.getLogger(Main.class.getName());

    private static final String ACCOUNT_DETAIL = "CloudBreakers3,7716-4451-0934";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(
            "yyyy-MM-dd");
    private static final SimpleDateFormat FULL_DATE_FORMAT = new SimpleDateFormat(
            "yyyy-MM-dd HH:mm:ss");

    // /**
    // * DB Settings
    // *
    // */

    private static final String DS_HOST_IP = "127.0.0.1"; // sql
    private static final String DS_PORT = "3306";
    private static final String DS_USER = "appuser";
    private static final String DS_PASS = "pass123";
    private static final String DS_DB_NAME = "tweet_db";

    private static final String Q2 = "SELECT tweet_id, content, score FROM tweets WHERE user_id=? AND timestamp=?";

    private static final String Q3 = ""
            + "select from_unixtime(left(timestamp, 10)) as tweet_time, score*(1+followercount) as impact_score, tweet_id, content "
            + "from tweets " + "where score!=0 and user_id=? "
            + "and timestamp between ? and ? "
            + "order by impact_score desc ,tweet_id;";

    private static final String Q5="select count(tweet_id) from tweets where user_id =?;";

    private static final String SQL_COL_TWEET_ID = "tweet_id";
    private static final String SQL_COL_CONTENT = "content";
    private static final String SQL_COL_TWEET_TIME = "tweet_time";
    private static final String SQL_COL_FOLL_COUNT = "followercount";
    private static final String SQL_COL_SCORE = "score";
    private static final String SQL_COL_IMPACT_SCORE = "impact_score";

    private static final char COLON = ':';
    private static final char COMMA = ',';
    private static final char NEWLINE = '\n';
    private static final Long ONE_DAY_MSEC = 86400000L;

    private static final JsonObject config = new JsonObject()
            .put("url",
                    "jdbc:mysql://" + DS_HOST_IP + ":" + DS_PORT + "/"
                            + DS_DB_NAME)
            .put("driver_class", "com.mysql.jdbc.Driver").put("user", DS_USER)
            .put("password", DS_PASS).put("max_pool_size", 800);

    private JDBCClient client;
    private int count=0;
    static {
        logger.setLevel(Level.OFF);
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        FULL_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
        FileHandler handler;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            handler = new FileHandler("server.log");
            handler.setFormatter(new SimpleFormatter());
            logger.addHandler(handler);

        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
    }

    @Override
    public void start(Future<Void> fut) {

        VertxOptions options = new VertxOptions();
        options.setMaxEventLoopExecuteTime(Long.MAX_VALUE);
        vertx = Vertx.vertx(options);
        client = JDBCClient.createShared(vertx, config);
        Router router = Router.router(vertx);
        router.route("/").handler(
                routingContext -> {
                    HttpServerResponse response = routingContext.response();
                    response.putHeader("content-type", "text/html").end(
                            "<h1>This server is run by CloudBreakers</h1>");
                });

        router.get("/q1").handler(this::executeQuery1);
        router.get("/q2").handler(this::executeQuery2);
        router.get("/q3").handler(this::executeQuery3);
        router.get("/q5").handler(this::executeQuery5);
        vertx.createHttpServer().requestHandler(router::accept)
                .listen(config().getInteger("http.port", 80), result -> {
                    if (result.succeeded()) {
                        fut.complete();
                    } else {
                        fut.fail(result.cause());
                    }
                });
    }

    private void executeQuery1(RoutingContext routingContext) {

        MultiMap map = routingContext.request().params();
        String key = map.get("key");
        String message = map.get("message");
        StringBuilder response = new StringBuilder();

        response.append(ACCOUNT_DETAIL).append(DATE_FORMAT.format(new Date()))
                .append("\n").append(PDC_Encryption.decrypt(key, message))
                .append("\n");

        routingContext.response().putHeader("content-type", "text/plain")
                .end(response.toString());
    }

    private void executeQuery2(RoutingContext routingContext) {
        final String user_id = routingContext.request().getParam("userid");
        String tt = routingContext.request().getParam("tweet_time");

        if (user_id == null) {
            routingContext.response().setStatusCode(400).end();
        } else {
            client.getConnection(ar -> {
                SQLConnection connection = ar.result();
                q2(user_id , tt, connection, result -> {
                    if (result.succeeded()) {
//						System.out.println("Sucess");
//						System.out.println("Result="+result.result());
                        routingContext.response().setStatusCode(200).putHeader("content-type", "text/html; charset=utf-8").end(result.result());
                    } else {
                        System.out.println("Failed");
                        routingContext.response().setStatusCode(404).end();
                    }
                    connection.close();
                });
            });
        }
    }

    private void q2(String user_id,String tweet_time, SQLConnection connection,
                    Handler<AsyncResult<String>> resultHandler) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(ACCOUNT_DETAIL);

        Long timestamp=dateStringToTs(tweet_time);
        connection.queryWithParams(Q2,
                new JsonArray().add(user_id).add(timestamp), ar -> {
                    if (ar.failed()) {
                        resultHandler.handle(Future.failedFuture("Row not found"));
                    } else {
                        if (ar.result().getNumRows() >= 1) {
                            ResultSet resultSet = ar.result();
                            List<JsonArray> results = resultSet.getResults();
                            for (JsonArray next : results) {
                                String tt = next.getLong(0) + ":" + next.getLong(2) + ":" + StringEscapeUtils.unescapeCsv(next.getString(1)) + "\n";
                                stringBuilder.append(tt);
//								System.out.println(stringBuilder.toString());
                            }
                            resultHandler.handle(Future.succeededFuture(stringBuilder.toString()));
                        } else {
                            resultHandler.handle(Future.succeededFuture("Row not found"));
                        }
                    }
                });
    }


    private void executeQuery3(RoutingContext routingContext) {

        MultiMap map = routingContext.request().params();
        String userID = map.get("userid");
        String startDate = map.get("start_date");
        String endDate = map.get("end_date");
        String n = map.get("n");
        String response = "";

        if (userID == null) {
            routingContext.response().setStatusCode(400).end();
        } else {
            client.getConnection(ar -> {
                SQLConnection connection = ar.result();
                q3(userID , startDate,endDate,Integer.parseInt(n), connection, result -> {
                    if (result.succeeded()) {
                        routingContext.response().setStatusCode(200).putHeader("content-type", "text/html; charset=utf-8").end(result.result());
                    } else {
                        routingContext.response().setStatusCode(404).end();
                    }
                    connection.close();
                });
            });
        }
    }

    private void q3(String user_id,String start_date, String end_date,int n, SQLConnection connection,
                    Handler<AsyncResult<String>> resultHandler) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(ACCOUNT_DETAIL).append(NEWLINE).append("Positive Tweets\n");

        Long startDate=dateStringToTsDayLevel(start_date);
        Long endDate=dateStringToTsDayLevel(end_date);
        List<String> q3results_Positive=new ArrayList<String>();
        List<String> q3results_Negative=new ArrayList<String>();
        connection.queryWithParams(Q3,
                new JsonArray().add(user_id).add(startDate).add(endDate), ar -> {
                    if (ar.failed()) {
                        resultHandler.handle(Future.failedFuture("Row not found"));
                    } else {
                        if (ar.result().getNumRows() >= 1) {
                            ResultSet resultSet = ar.result();
                            List<JsonArray> results = resultSet.getResults();
                            for (JsonArray next : results) {
                                String tt = next.getString(0) + "," + next.getInteger(1) + "," +next.getLong(2) + ","+StringEscapeUtils.unescapeCsv(next.getString(3))+"\n";
                                if(next.getInteger(1)<0)
                                    q3results_Negative.add(tt);
                                else
                                    q3results_Positive.add(tt);
                            }
                            for(int i=0;i<Math.min(n,q3results_Positive.size());i++){
                                stringBuilder.append(q3results_Positive.get(i)).append(NEWLINE);
                            }
                            stringBuilder.append("Negative Tweets\n");
                            for(int i=0;i<Math.min(n,q3results_Negative.size());i++){
                                stringBuilder.append(q3results_Negative.get(i)).append(NEWLINE);
                            }
                            resultHandler.handle(Future.succeededFuture(stringBuilder.toString()));
                        } else {
                            resultHandler.handle(Future.succeededFuture(""));
                        }
                    }
                });
    }

    private void executeQuery5(RoutingContext routingContext) {

        MultiMap map = routingContext.request().params();
        String userid_min = map.get("userid_min");
        String userid_max = map.get("userid_max");
        String response = "";

        client.getConnection(ar -> {
            SQLConnection connection = ar.result();
            q5(userid_min , userid_max, connection, result -> {
                if (result.succeeded()) {
                    System.out.println("Result="+result.result());
                    routingContext.response().putHeader("content-type", "text/html; charset=utf-8").end(result.result());
                } else {
                    routingContext.response().setStatusCode(404).end();
                }
                connection.close();
            });
        });

    }

    private void q5(String userid_min,String userid_max, SQLConnection connection,
                    Handler<AsyncResult<String>> resultHandler) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(ACCOUNT_DETAIL).append(NEWLINE);
        long i=Long.parseLong(userid_min);
        long limit=Long.parseLong(userid_max);
        while(i<=limit){
            connection.queryWithParams(Q5,
                    new JsonArray().add(i), ar -> {
                        if (ar.failed()) {
                            resultHandler.handle(Future.failedFuture("Row not found"));
                        } else {
                            if (ar.result().getNumRows() >= 1) {
                                ResultSet resultSet = ar.result();
                                List<JsonArray> results = resultSet.getResults();
                                for (JsonArray next : results) {
                                    int r = next.getInteger(0);
//									stringBuilder.append(tt);
                                    count+=r;
                                }
                            }
//							} else {
//								resultHandler.handle(Future.succeededFuture(""));
//							}
                        }
                    });
            i++;
        }
        stringBuilder.append(count+"\n");
        resultHandler.handle(Future.succeededFuture(stringBuilder.toString()));
    }

    public static Long dateStringToTs(String date) {
        // 2014-05-31+01:29:04
        try{
            return FULL_DATE_FORMAT.parse(date).getTime();
        }catch(ParseException e){

        }
        return null;
    }

    public static Long dateStringToTsDayLevel(String date)
    {
        // 2014-05-31
        try{
            return DATE_FORMAT.parse(date).getTime();
        }catch(ParseException e){

        }
        return null;
    }

    public static String dateTsToString(Long ts) throws ParseException {
        // 2014-05-31+01:29:04
        return FULL_DATE_FORMAT.format(new Date(ts));
    }

    public static String dateTsToStringDayLevel(Long ts) throws ParseException {
        // 2014-05-31
        return DATE_FORMAT.format(new Date(ts));
    }

    private class Query3Entity implements Comparable<Query3Entity> {

        private Long tweetId;
        private String text;
        private int impactScore;
        private String date;

        public Query3Entity(Long tweetId, String text, int impactScore,
                            String date) {
            this.tweetId = tweetId;
            this.text = text;
            this.impactScore = impactScore;
            this.date = date;
        }

        @Override
        public boolean equals(Object arg0) {

            Query3Entity that = (Query3Entity) arg0;
            if (that.impactScore == this.impactScore) {
                return this.tweetId.equals(that.tweetId);
            } else {
                return false;
            }
        }

        @Override
        public int compareTo(Query3Entity that) {
            if (that.impactScore == this.impactScore) {
                return -1 * that.tweetId.compareTo(this.tweetId);
            } else {
                if (that.impactScore < 0 && this.impactScore < 0) {
                    return (that.impactScore > this.impactScore) ? -1 : 1;
                } else {
                    return (that.impactScore > this.impactScore) ? 1 : -1;
                }
            }
        }
    }

}