import com.mongodb.*;
import com.mongodb.client.MongoDatabase;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class MongoDBTest {
   public MongoClient mongo;
   public DB db;
   public MongoDBTest(){
      mongo = new MongoClient("localhost", 27017);
      db = mongo.getDB("practiceTest");
   }
   public void insert(DBCollection collection){
      /**** 插入文档 ****/
      BasicDBObject document = null;
      for(int i=0;i<10;i++){
         String sex = null;
         if(i%2==0&&i<8){
            sex = "man";
         }else {
            sex = "woman";
         }
         document = new BasicDBObject();
         document.put("name", "m"+i);
         document.put("age", 20+i);
         document.put("sex", sex);
         collection.insert(document);
      }
   }
   public void AggregationAndGoup(DBCollection collection){
      DBObject groupFields = new BasicDBObject("_id","$sex");
      groupFields.put("age", new BasicDBObject("$sum", "$age"));
      DBObject group = new BasicDBObject("$group", groupFields);
      List<DBObject>  list = new ArrayList<DBObject>();
      list.add(group);
      AggregationOutput output = collection.aggregate(list);
      Iterator iterator = output.results().iterator();
      while(iterator.hasNext()){
         System.out.println(iterator.next());
      }
   }
   public void find(DBCollection collection,BasicDBObject searchQuery){
      /**** find ****/
      DBCursor cursor = collection.find(searchQuery);
      while (cursor.hasNext()) {
         System.out.println(cursor.next());
      }
   }
   public void limitResults(DBCollection collection,int limit){
      BasicDBObject searchQuery = new BasicDBObject();
      DBCursor cursor = collection.find(searchQuery);
      cursor.limit(limit);
      while (cursor.hasNext()) {
         System.out.println(cursor.next());
      }
   }
   public void limitResults(DBCollection collection,int skip,int limit){
      BasicDBObject searchQuery = new BasicDBObject();
      DBCursor cursor = collection.find(searchQuery);
      cursor.limit(limit);
      cursor.skip(skip);
      while (cursor.hasNext()) {
         System.out.println(cursor.next());
      }
   }
   public void limitResults(DBCollection collection,int skip,int limit, BasicDBObject sort){
      BasicDBObject searchQuery = new BasicDBObject();
      DBCursor cursor = collection.find(searchQuery);
      cursor.limit(limit);
      cursor.skip(skip);
      cursor.sort(sort);
      while (cursor.hasNext()) {
         System.out.println(cursor.next());
      }
   }
   public void remove(DBCollection collection,BasicDBObject remove){
      collection.remove(remove);
   }
   public void update(DBCollection collection){
      BasicDBObject newObj = new BasicDBObject();
      BasicDBObject oldObj = new BasicDBObject();
      oldObj.put("name","m1");
      BasicDBObject updateObj = new BasicDBObject("$set",newObj);
      newObj.put("name","liu");
      collection.update(oldObj,newObj);
   }
   public void count(DBCollection collection){
      System.out.println(collection.count());
   }
   public static void main(String[] args) {
         MongoDBTest mongoDBTest = new MongoDBTest();
         MongoClient mongo = mongoDBTest.mongo;
         DB db = mongoDBTest.db;
         DBCollection collection = db.getCollection("practicetest");
//         mongoDBTest.insert(collection);
//      mongoDBTest.find(collection,null);
         //AggregationAndGoup
//         mongoDBTest.AggregationAndGoup(collection);
         //limit skip sort
//         mongoDBTest.limitResults(collection,0,3,new BasicDBObject("age",-1));
//           mongoDBTest.limitResults(collection,2);
           mongoDBTest.limitResults(collection,1,2);
         //update
//          mongoDBTest.update(collection);
//          mongoDBTest.find(collection,null);
          //find
//          BasicDBObject searchQuery = new BasicDBObject();
//          searchQuery.put("name", "m3");
//          mongoDBTest.find(collection,searchQuery);
          //remove
//          BasicDBObject remove = new BasicDBObject();
//          remove.put("name","m2");
//          mongoDBTest.remove(collection,remove);
//          mongoDBTest.find(collection,null);
          //文档数量
//          mongoDBTest.count(collection);
   }
}
