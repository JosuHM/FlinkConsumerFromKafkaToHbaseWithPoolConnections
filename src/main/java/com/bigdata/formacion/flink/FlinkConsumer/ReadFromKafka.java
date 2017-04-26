package com.bigdata.formacion.flink.FlinkConsumer;

import java.io.IOException;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.net.ntp.TimeStamp;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Simple example on how to read with a Kafka consumer
 *
 * Note that the Kafka source is expecting the following parameters to be set
 *  - "bootstrap.servers" (comma separated list of kafka brokers)
 *  - "zookeeper.connect" (comma separated list of zookeeper servers)
 *  - "group.id" the id of the consumer group
 *  - "topic" the name of the topic to read data from.
 *
 * You can pass these required parameters using "--bootstrap.servers host:port,host1:port1 --zookeeper.connect host:port --topic testTopic"
 *
 * This is a valid input example:
 * 		--topic test --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myGroup
 *
 *
 */

public class ReadFromKafka {

	private static String hbaseZookeeperQuorum="lug041.zylk.net,lug040.zylk.net";
    private static String hbaseZookeeperClientPort="2181";
    private static TableName tableName = TableName.valueOf("mensajesDesdeKafka");
    private static final String columnFamily = "mensaje";
       
    /**
     * @param args the command line arguments     
     */
	public static void main(String[] args) {
		// create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//poner paralelismo igual a 3 a nivel de entorno de ejecucion
		env.setParallelism(3);
		//System.out.println(parale);
		// parse user parameters
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		 // create datastream with the data coming from Kafka
		DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer09<>
			(parameterTool.getRequired("topic"), 
			 new SimpleStringSchema(), 
			 parameterTool.getProperties()
			)
		);

		// print() will write the contents of the stream to the TaskManager's standard out stream
		// the rebalance call is causing a repartitioning of the data so that all machines
		// see the messages (for example in cases when "num kafka partitions" < "num flink operators"
		messageStream.rebalance().map(new MapFunction<String, String>() {
			private static final long serialVersionUID = -6867736771747690202L;

			@Override
			public String map(String value) throws Exception {
				//llamamos a la función que nos escribe los datos en HBase
				writeIntoHBase(value);
				System.out.println("Message written into HBase.");
				return "Kafka and Flink says: " + value;
			}
		}).print();

		try {
			env.execute();
		} catch (Exception ex) {
			 Logger.getLogger(ReadFromKafka.class.getName()).log(Level.SEVERE, null, ex);
			ex.printStackTrace();
		}
	}
	
	
	public static void writeIntoHBase(String m) throws IOException{                   
        // Instantiating Configuration class
        Configuration config = HBaseConfiguration.create();           
        config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);
        config.set("hbase.zookeeper.property.clientPort", hbaseZookeeperClientPort);        
        config.set("zookeeper.znode.parent", "/hbase-unsecure");
        Connection c = ConnectionFactory.createConnection(config);
        // Check if table exists
        Admin admin = c.getAdmin();
        	//si no existe crea una nueva tabla con el columnFamily que hemos indicado en la constante
        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(columnFamily)));
        }        
        // Instantiating Table class
        // (deprecated) HTable hTable = new HTable(config, tableName);
        Table t = c.getTable(tableName);        
        // Instantiating Put class. Accepts a row key.
        TimeStamp ts = new TimeStamp(new Date());
        Date d = ts.getDate(); 
        Put p = new Put(Bytes.toBytes(d.toString()));       
        // adding values using addColumn() method. Accepts column family name, qualifier/row name ,value.        
        // (deprecated) p.addColumn(Bytes.toBytes("messageJava"),Bytes.toBytes("java"),Bytes.toBytes(m));        
        p.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("java"),Bytes.toBytes(m));        
        // Saving the put Instance to the Table.
        t.put(p);
        // closing Table & Connection
        t.close();
        c.close();
	}

}


