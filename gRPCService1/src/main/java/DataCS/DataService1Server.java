package DataCS;

import DBBasicUtil.MongoDBUtil;
import com.OneGISDataService.DataService1.DataService1Grpc;
import com.OneGISDataService.DataService1.Request;
import com.OneGISDataService.DataService1.Response;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.bson.Document;
import org.json.JSONObject;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Server that manages startup/shutdown.
 */
public class DataService1Server {
    //IP、端口设置
    private int data_access_service_port=50051;
    public static String URL = "localhost";//"192.168.7.131"
    private static String databaseName="OneGISdataTag2";
    private static String collectionName="service1data";




    private static final Logger logger = Logger.getLogger(DataService1Server.class.getName());
    private static Map<String,Integer> queryParaJudge=new HashMap();

    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */

        server = ServerBuilder.forPort(data_access_service_port).addService(new GreeterImpl()).build().start();
        logger.info("Server started, listening on " + data_access_service_port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its
                // JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                DataService1Server.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon
     * threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Main launches the server from the command line.
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        queryParaJudge.put("111",0);
        queryParaJudge.put("011",1);
        queryParaJudge.put("101",2);
        queryParaJudge.put("110",3);
        queryParaJudge.put("100",4);
        queryParaJudge.put("010",5);
        queryParaJudge.put("001",6);
        queryParaJudge.put("000",7);
        final DataService1Server server = new DataService1Server();
        server.start();
        server.blockUntilShutdown();
    }

    public static FindIterable<Document> Query(List IDList, String startDateS, String endDateS,List<Double> leftDownList,List<Double> rightUpList, int Para) throws ParseException {
        MongoDBUtil util=new MongoDBUtil(URL);
        MongoCollection<Document> table = util.getCollection(databaseName, collectionName);
        FindIterable<Document> outcome = null;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long startDate,endDate;
        switch (Para) {
            case 0://全参数
                startDate=simpleDateFormat.parse(startDateS).getTime() / 1000;
                endDate=simpleDateFormat.parse(endDateS).getTime() / 1000;
                outcome = table.find(Filters.and(Filters.geoWithinBox("recCenter", leftDownList.get(0), leftDownList.get(1), rightUpList.get(0), rightUpList.get(1)),
                        Filters.gte("startDate", startDate), Filters.lte("endDate", endDate), Filters.in("ID", IDList))).batchSize(500)
                        .sort(new BasicDBObject("ID", 1).append("startDate", 1));
                break;
            case 1://缺少ID
                startDate=simpleDateFormat.parse(startDateS).getTime() / 1000;
                endDate=simpleDateFormat.parse(endDateS).getTime() / 1000;
                outcome = table.find(Filters.and(Filters.geoWithinBox("recCenter", leftDownList.get(0), leftDownList.get(1), rightUpList.get(0), rightUpList.get(1)),
                        Filters.gte("startDate", startDate), Filters.lte("endDate", endDate), Filters.nin("ID", IDList))).batchSize(500)
                        .sort(new BasicDBObject("ID", 1).append("startDate", 1));
                break;
            case 2://缺少Date
                outcome = table.find(Filters.and(Filters.geoWithinBox("recCenter", leftDownList.get(0), leftDownList.get(1), rightUpList.get(0), rightUpList.get(1)),
                        Filters.in("ID", IDList))).batchSize(500).sort(new BasicDBObject("ID", 1).append("startDate", 1));
                break;
            case 3://缺少Loc
                startDate=simpleDateFormat.parse(startDateS).getTime() / 1000;
                endDate=simpleDateFormat.parse(endDateS).getTime() / 1000;
                outcome = table.find(Filters.and(Filters.gte("startDate", startDate), Filters.lte("endDate", endDate), Filters.in("ID", IDList)))
                        .batchSize(500).sort(new BasicDBObject("ID", 1).append("startDate", 1));
                break;
            case 4://只有ID
                outcome = table.find(Filters.in("ID", IDList)).batchSize(500)
                        .sort(new BasicDBObject("ID", 1).append("startDate", 1));
                break;
            case 5://只有Date
                startDate=simpleDateFormat.parse(startDateS).getTime() / 1000;
                endDate=simpleDateFormat.parse(endDateS).getTime() / 1000;
                outcome = table.find(Filters.and(
                        Filters.gte("startDate", startDate), Filters.lte("endDate", endDate), Filters.nin("ID", IDList))).batchSize(500)
                        .sort(new BasicDBObject("ID", 1).append("startDate", 1));
                break;
            case 6://只有Loc
                outcome = table.find(Filters.and(Filters.geoWithinBox("recCenter", leftDownList.get(0), leftDownList.get(1), rightUpList.get(0), rightUpList.get(1)), Filters.nin("ID", IDList)
                )).batchSize(500).sort(new BasicDBObject("ID", 1).append("startDate", 1));
                break;
            case 7://全取
                outcome = table.find().batchSize(500).sort(new BasicDBObject("ID", 1).append("startDate", 1));
                break;
        }


        return outcome;

    }



    static class GreeterImpl extends DataService1Grpc.DataService1ImplBase {

        @Override
        public void mongoQuery(Request req, StreamObserver<Response> responseObserver) {
            List IDList = req.getIDList();
            String startDate = req.getStartDate();
            String endDate = req.getEndDate();
            List<Double> leftDownList = req.getLeftDownList();
            List<Double> rightUpList = req.getRightUPList();
            String IDjudge="1";
            String Datejudge="1";
            String Locjudge="1";
            if(req.getIDList().isEmpty()){
                IDjudge="0";
            }
            if(req.getStartDate().isEmpty()||req.getEndDate().isEmpty()){
                Datejudge="0";
            }
            if(req.getRightUPList().isEmpty()||req.getLeftDownList().isEmpty()){
                Locjudge="0";
            }
            int queryPara=queryParaJudge.get(IDjudge+Datejudge+Locjudge);


            List dataArray = new ArrayList();
            FindIterable<Document> outcome = null;
            try {
                outcome = Query(IDList,startDate , endDate, leftDownList, rightUpList, queryPara);
                System.out.println("查询参数为："+queryPara);
                String outcomeBlock;
                MongoCursor<Document> o = outcome.iterator();

                int count = 0;
                int round=0;
                while (o.hasNext()) {
                    count++;
                    dataArray.add(new JSONObject(o.next().toJson()));
                    if (count == 5) {
                        round++;
                        outcomeBlock = dataArray.toString();
//ByteString.copyFrom(outcomeBlock.getBytes()
                        responseObserver.onNext(Response.newBuilder().setDataList(outcomeBlock).build());
                        dataArray.clear();
                        count = 0;
                    }
                }
                round++;
                System.out.println(round);
                if(!dataArray.isEmpty()) {
                    responseObserver.onNext(Response.newBuilder().setDataList(dataArray.toString()).build());
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
            responseObserver.onCompleted();
        }
    }

}
