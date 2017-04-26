package com.bigdata.formacion.flink.FlinkConsumer;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hbase.client.Connection;

import com.bigdata.formacion.flink.FlinkConsumer.ConnectionToHBase;

public class PoolConnectionsFactory<T> extends BasePooledObjectFactory<Connection> {

	/** 
	 * Creamos un objeto del tipo Connection que generará una conexión a HBase
	 */
	@Override
	public Connection create() throws Exception {
		return ConnectionToHBase.getInstance().createConnection();
	}

	/**
	 * Use the default PooledObject implementation.
	 */

	@Override
	public PooledObject<Connection> wrap(Connection c) {
		
		return new DefaultPooledObject<Connection>(c);
	}

}
