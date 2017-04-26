package com.bigdata.formacion.flink.FlinkConsumer;

import java.util.Date;
import java.util.Map;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.commons.pool2.ObjectPool;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;


public class ManagePoolConnections {

	private ObjectPool<Connection> pool;	
	public TableName tableName = TableName.valueOf("mensajesDesdeKafka");
		 
	
	public ManagePoolConnections(ObjectPool<Connection> pool) {
		this.pool = pool;
	}
	
	/**
	 * Escribir en Hbase
	 * @param colFamily		Nombre del Column Family
	 * @param colQualifier	Nombre del Qualifier
	 * @param value			Dato a escibir
	 * @throws Exception
	 */
	public void writeIntoHBase(String colFamily, String colQualifier, String value)
				throws Exception{                   
		
		//Solicitar conexion al Pool de Conexiones
		Connection c = null;  
        c = pool.borrowObject();
		
		 // Check if table exists
        Admin admin = c.getAdmin();
        	//si no existe crea una nueva tabla con el columnFamily que hemos indicado en la constante
        if (!admin.tableExists(tableName)) {
            admin.createTable(new HTableDescriptor(tableName).addFamily(new HColumnDescriptor(colFamily)));
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
        p.addColumn(
        		Bytes.toBytes(colFamily),
        		Bytes.toBytes(colQualifier),
        		Bytes.toBytes(value));        
        
       
        // Saving the put Instance to the Table.
        t.put(p);

        /**
         * Obtener la descripcion de la tabla
         */
	    HTableDescriptor tableDescriptor = t.getTableDescriptor();
	    System.out.println(tableDescriptor);
        
        // closing Table
        t.close();
        //Devolver conexión al Pool de Conexiones
        pool.returnObject(c); 
	}
	
	/**
	 * Obtener un número "i" de row de la tabla de HBase
	 * @param i				Número de rows a devolver
	 * @throws Exception
	 */
    public void getLimitRows(int i) throws Exception{
        Connection c = pool.borrowObject();              
        Table t = c.getTable(tableName); 
        Scan s = new Scan();
        ResultScanner scanner = t.getScanner(s);
        int j = 0;
        for (Result result = scanner.next(); result != null && j < i; result = scanner.next()){            
            Map<byte[],byte[]> qualifiers = result.getFamilyMap(Bytes.toBytes("msg"));
            for(int k = 0; k < qualifiers.size(); k++){
                Object[] values = qualifiers.values().toArray();
                Object[] keys = qualifiers.keySet().toArray();                
                String key = Bytes.toString((byte[]) keys[k]);
                String value = Bytes.toString((byte[]) values[k]);
                System.out.println("[msg:" + key + "] =  " + value);            
            }
            j++;
        }        
        scanner.close();
        t.close();
        pool.returnObject(c);   
    }

}
