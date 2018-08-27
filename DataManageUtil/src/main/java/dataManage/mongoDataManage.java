package dataManage;


import DBBasicUtil.MongoDBUtil;
import DBBasicUtil.MyJsonObject;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.json.JSONArray;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class mongoDataManage {
    private static String URL="localhost";
    public static void dataBatchInsert() throws Exception {
        /*
         * mongoDB的Document可以将JSON String直接转化为Document；相反也可以。
         */
        MongoDBUtil util=new MongoDBUtil(URL);
       MongoCollection<Document> table1 = util.getCollection("OneGISdata", "service1data");
        String fileName = "";
        DecimalFormat df = new DecimalFormat( "0.00000");
        for (int i = 0; i < 182; i++) {
            fileName=Integer.toString(i);
            if (i < 10) {
                fileName = "00" + i;
            } else if (i >= 10 && i < 100) {
                fileName = "0" + i;
            }

            System.out.println("id导入："+i);
            File filePath = new File("C:\\Users\\Eric Lee\\Desktop\\全空间项目\\Geolife-Trajectories-1.3\\Geolife-Trajectories-1.3\\Data\\"+fileName+"\\Trajectory\\");
            File[] tempList = filePath.listFiles();
            List outcomelist = new ArrayList();
            for (int j = 0; j < tempList.length; j++) {
                MyJsonObject oneRow = new MyJsonObject();
                JSONArray pointArray=new JSONArray();
                oneRow.put("ID", fileName);
                double lonMin=0;
                double lonMax=0;
                double latMin=0;
                double latMax=0;

                String encoding = "GBK";
                File file = tempList[j];
//                System.out.println(tempList[j]);
                if (file.isFile() && file.exists()) { // 判断文件是否存在

                    InputStreamReader read = null;
                    read = new InputStreamReader(new FileInputStream(file), encoding);// 考虑到编码格式
                    BufferedReader bufferedReader = new BufferedReader(read);
                    String lineTxt = null;
                    String[] lineAfterSplit = new String[7];


                    List it = new ArrayList();

                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                    int count = 0;
                    while ((lineTxt = bufferedReader.readLine()) != null) {
                        count++;
                        if (count >= 7){
                            lineAfterSplit = lineTxt.split(",");

                        if (count == 7) {
                            oneRow.put("startDate", simpleDateFormat.parse(lineAfterSplit[5] + " " + lineAfterSplit[6]).getTime() / 1000);
                            lonMin=Double.parseDouble(lineAfterSplit[1]);
                            lonMax=Double.parseDouble(lineAfterSplit[1]);
                            latMin=Double.parseDouble(lineAfterSplit[0]);
                            latMax=Double.parseDouble(lineAfterSplit[0]);

                        }
                        pointArray.put(new JSONArray().put(Double.parseDouble(lineAfterSplit[1])).put(Double.parseDouble(lineAfterSplit[0])));
                        if (Double.parseDouble(lineAfterSplit[1])<lonMin) {
                         lonMin=Double.parseDouble(lineAfterSplit[1]);
                        }
                        if (Double.parseDouble(lineAfterSplit[1])>lonMax) {
                            lonMax=Double.parseDouble(lineAfterSplit[1]);
                        }
                        if (Double.parseDouble(lineAfterSplit[0])<latMin) {
                            latMin=Double.parseDouble(lineAfterSplit[0]);
                        }
                        if (Double.parseDouble(lineAfterSplit[0])>latMax) {
                            latMax=Double.parseDouble(lineAfterSplit[0]);
                        }

                    }


                    }
                    oneRow.put("endDate", simpleDateFormat.parse(lineAfterSplit[5] + " " + lineAfterSplit[6]).getTime() / 1000);
                    oneRow.put("recCenter",new JSONArray().put(Double.parseDouble(df.format((lonMax+lonMin)/2))).put(Double.parseDouble(df.format((latMax+latMin)/2))));
                    oneRow.put("locList",pointArray);
                    System.out.println(oneRow.toString());
                    outcomelist.add(Document.parse(oneRow.toString()));
                }
            }
            table1.insertMany(outcomelist);
        }
        table1.createIndex(new BasicDBObject().append("ID",1).append("startDate",1));
        //不需要再用DBObject了。
        // DBObject bson = (DBObject)JSON.parse(oneRow.toString());


    }

    public static void main(String args[]) throws Exception {

//        dataBatchInsert();
      testTime();
    }

    public static void testTime() throws ParseException {
        String endDate = "2008-11-28 00:00:00";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date it=new Date();
        it.setTime(1245102088000L);

//        System.out.println(simpleDateFormat.parse(endDate).getTime()/1000);
        System.out.println(simpleDateFormat.format(it));
    }

    // public static void fileDataRead(){
    // String encoding = "GBK";
    // File file = new
    // File("E:\\D_Software\\Eclipse\\workspace0719\\smartRetrievalProject\\disasterData\\origionData\\DEM.json");
    // }

}
