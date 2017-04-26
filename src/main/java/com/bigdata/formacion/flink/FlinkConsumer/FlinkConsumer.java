package com.bigdata.formacion.flink.FlinkConsumer;


import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.hbase.client.Connection;

import com.bigdata.formacion.flink.FlinkConsumer.ManagePoolConnections;
import com.bigdata.formacion.flink.FlinkConsumer.PoolConnectionsFactory;



public class FlinkConsumer {
	
	private static final String columnFamily = "mensaje";
	
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {      
    
	    // create execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// parse user parameters
		ParameterTool parameterTool = ParameterTool.fromArgs(args);        
	    // create datastream with the data coming from Kafka
        DataStream<String> messageStream = env.addSource(new FlinkKafkaConsumer09<>(
                parameterTool.getRequired("topic"), 
                new SimpleStringSchema(), 
                parameterTool.getProperties()));
        
        messageStream.rebalance().map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;
            @Override
            public String map(String message) throws Exception {
            	ManagePoolConnections hb = new ManagePoolConnections(
            			new GenericObjectPool<Connection>(new PoolConnectionsFactory<Object>()));
                hb.writeIntoHBase(columnFamily, "java", message);
                System.out.println("Message written into HBase.");
                return "Kafka dice: " + message;
            }
        }).print();
        // execute enviroment
        try {
            env.execute();
        } catch (Exception ex) {
            Logger.getLogger(FlinkConsumer.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
