package cn.gzsxt.jdbc;

import com.mongodb.MongoClient;
import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MongoJDBC {

    /**
     * 连接MongoDB 服务端
     */
    @Test
    public void testConn() {
        //与mongodb数据库连接
        MongoClient client = new MongoClient("192.168.232.140", 27017);
        //获取对应的database
        MongoDatabase database = client.getDatabase("gzsxt");
        //获取需要的集合
        MongoCollection<Document> collection = database.getCollection("coll");
        //查询所有文档数据
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            System.out.println(document.get("name"));
        }
        //关闭连接
        client.close();
    }

    /**
     * 创建集合
     */
    @Test
    public void testCreateCollection() {
        //与mongodb数据库连接
        MongoClient client = new MongoClient("192.168.232.140", 27017);
        //选择对应的数据库
        MongoDatabase database = client.getDatabase("gzsxt");
        //创建集合
        database.createCollection("testJavaDb");
        //获取所有集合
        ListCollectionsIterable<Document> documents = database.listCollections();
        //遍历所有集合
        for (Document document : documents
                ) {
            System.out.println(document);
        }

        client.close();
    }

    /**
     * 获取集合
     */
    @Test
    public void testGetCollection() {
        //与mongodb数据库连接
        MongoClient client = new MongoClient("192.168.232.140", 27017);
        //选择对应的数据库
        MongoDatabase database = client.getDatabase("gzsxt");
        //获取指定的集合
        MongoCollection<Document> mycol = database.getCollection("mycol");
        System.out.println("集合mycol文档的数量:" + mycol.count());
        //指定集合中查询所有数据
        FindIterable<Document> documents = mycol.find();

        //遍历所有数据
        for (Document document : documents
                ) {
            System.out.println(document);
        }
        client.close();
    }


    /**
     * 插入数据
     */
    @Test
    public void testInsertIntoCollection() {
        //与mongodb数据库连接
        MongoClient client = new MongoClient("192.168.232.140", 27017);
        //选择对应的数据库
        MongoDatabase database = client.getDatabase("gzsxt");

        MongoCollection<Document> testdb = database.getCollection("testdbl");
        /*
        Document document = new Document();
        document.append("title","MongoDB").append("description", "database").append("likes", 100).append("by", "Fly");
        */
        //单条数据我们可以使用insertOne了
        //插入批量数据 我们可以用insertMary
        //testdb.insertOne(document);

        List<Document> documents = new ArrayList<>();

        Document docuemt = new Document("name", "thinking in java").append("price", 89.5).append("author", "Bruce Eckel");

        documents.add(docuemt);

        docuemt = new Document("name", "java从入门到放弃").append("price", 35.0).append("author", "大神");

        documents.add(docuemt);

        testdb.insertMany(documents);

        System.out.println("集合testdb插入文档成功!!!");
        System.out.println("插入成功!");
        client.close();
    }

    /**
     * 检索文档数据
     */
    @Test
    public void testFindCollection() {
        //与mongodb数据库连接
        MongoClient client = new MongoClient("192.168.232.140", 27017);
        //选择对应的数据库
        MongoDatabase database = client.getDatabase("gzsxt");
        //获取指定集合
        MongoCollection<Document> collection = database.getCollection("testdbl");
        //获取迭代器FindInterable,
        //我们也可以直接遍历迭代器对象
        FindIterable<Document> documents = collection.find();
        //获取游标MongoCursor
        MongoCursor<Document> iterator = documents.iterator();
        //遍历游标检索出的文档集合
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }
        client.close();
    }


    /**
     * 指定条件封装查询
     */
    @Test
    public void testFindByCollection() {
        //与mongodb数据库连接
        MongoClient client = new MongoClient("192.168.232.140", 27017);
        //选择对应的数据库
        MongoDatabase database = client.getDatabase("gzsxt");
        //获取指定集合
        //MongoCollection<Document> collection = database.getCollection("testdbl");
        //创建Bson数据文档,把要指定的条件封装
        // Bson bson = new Document();
        //((Document) bson).append("name","java从入门到放弃");
        //指定条件的bson进去find查询
        //FindIterable<Document> documents = collection.find(bson);
        //获取游标
        // MongoCursor<Document> iterator = documents.iterator();
        //遍历游标
        // while (iterator.hasNext()){
        //     System.out.println(iterator.next());
        //}
        MongoCollection<Document> collection = database.getCollection("student");

        //age > 15 条件封装
        FindIterable<Document> documents = collection.find(new Document().append("age", new Document().append("$gt", 15)));

        MongoCursor<Document> iterator = documents.iterator();

        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

        client.close();
    }

    /**
     * 更新文档
     */
    @Test
    public void testUpdateCollection() {
        //与mongodb数据库连接
        MongoClient client = new MongoClient("192.168.232.140", 27017);
        //选择对应的数据库
        MongoDatabase database = client.getDatabase("gzsxt");

        MongoCollection<Document> collection = database.getCollection("testdbl");
        /**
         * Filters 父类其实就是BSON
         * MongoDB java 封装了Filters过滤条件  必须gt lte eq 这些条件
         */
        collection.updateMany(Filters.eq("name", "java从入门到放弃"), new Document("$set", new Document("author", "java大神")));

        FindIterable<Document> documents = collection.find();

        for (Document document: documents
             ) {
            System.out.println(document);
        }
        client.close();
    }


    /**
     * 删除文档
     */
    @Test
    public void testDeleteCollection(){
        //与mongodb数据库连接
        MongoClient client = new MongoClient("192.168.232.140", 27017);
        //选择对应的数据库
        MongoDatabase database = client.getDatabase("gzsxt");

        MongoCollection<Document> collection = database.getCollection("testdbl");

        collection.deleteOne(Filters.eq("name","java从入门到放弃"));

        FindIterable<Document> documents = collection.find();

        for (Document document: documents
             ) {
            System.out.println(document);
        }
        client.close();
    }


}
