package DataCS;


import com.OneGISDataService.DataService1.DataService1Grpc;
import com.OneGISDataService.DataService1.Request;
import com.OneGISDataService.DataService1.Response;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple client that requests a greeting from the {@link DataService1Server}.
 */
public class DataService1Client {

  private static String listenURL="localhost";
  private static final Logger logger = Logger.getLogger(DataService1Client.class.getName());

  private final ManagedChannel channel;
  private final DataService1Grpc.DataService1BlockingStub blockingStub;

  /** Construct client connecting to HelloWorld server at {@code host:port}. */
  public DataService1Client(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port)
        // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
        // needing certificates.
        .usePlaintext(true)
        .build());
  }

  /** Construct client for accessing HelloWorld server using the existing channel. */
  DataService1Client(ManagedChannel channel) {
    this.channel = channel;
    blockingStub = DataService1Grpc.newBlockingStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  /** Say hello to server. */
  public void query(List IDList,List leftDown,List rightUp,String startDate,String endDate) {

    Request request = Request.newBuilder().addAllID(IDList).addAllLeftDown(leftDown).addAllRightUP(rightUp).setStartDate(startDate).setEndDate(endDate).build();

    try {
      Iterator<Response> responseIterator= blockingStub.mongoQuery(request);
      int count=0;
      while (responseIterator.hasNext()) {
        count++;
        Response response = responseIterator.next();
        System.out.println(response.getDataList());
      }
      System.out.println("stream次数："+count);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }

  }

  /**
   * Greet server. If provided, the first element of {@code args} is the name to use in the
   * greeting.
   */
  public static void main(String[] args) throws Exception {
      DataService1Client client = new DataService1Client(listenURL, 50051);
    try {
      /* Access a service running on the local machine on port 50051 */
      String para = "all";
      if (args.length > 0) {
        para = args[0]; /* Use the arg as the name to greet if provided */
      }
//"000","015","024","039","045","071","076","082","100","111"
      String[] ID = {"000","015","024","039","045","071","076","082","100","111"};
      String startDate = "2008-10-23 00:00:00";
      String endDate = "2009-8-28 00:00:00";
//      2008-10-23 00:00:00
//      2009-8-28 00:00:00
//      116.31, 39.95 116.35, 39.99
      double[] leftDown = {116.31, 39.95};
      double[] rightUp = {116.35, 39.99};
      List IDList=new ArrayList();
      List leftDownList=new ArrayList();
      List rightUpList=new ArrayList();
      for (int m = 0; m < ID.length; m++) {
        IDList.add(ID[m]);
      }
      for (int m = 0; m < leftDown.length; m++) {
        leftDownList.add(leftDown[m]);
      }
      for (int m = 0; m < rightUp.length; m++) {
        rightUpList.add(rightUp[m]);
      }
      long current1 = System.currentTimeMillis();
      client.query(IDList,leftDownList,rightUpList,startDate,endDate);
      long duration = System.currentTimeMillis() - current1;
      System.out.println("时间消耗为：" + duration);
    } finally {
      client.shutdown();
    }
  }
}

