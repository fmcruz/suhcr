import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import java.util.Collection;
import java.util.Iterator;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;


import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.io.hfile.Compression;

import py4j.GatewayServer;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseTuner {

	/* note that these constant must be kept in sync with the python code */
	public static final int REGION_SERVER_READ_REQUEST_COUNT = 20;
	public static final int REGION_SERVER_WRITE_REQUEST_COUNT = 21;
	public static final int REGION_SERVER_BLOCK_CACHE_EVICTED_COUNT = 22;
	public static final int REGION_SERVER_BLOCK_CACHE_HIT_RATIO = 23;
	public static final int REGION_SERVER_BLOCK_CACHE_HIT_CACHING_RATIO = 24;
	public static final int REGION_SERVER_HDFS_BLOCKS_LOCALITY_INDEX = 25;
	public static final int REGION_SERVER_REQUESTS_PER_SECOND = 26;
	public static final int REGION_SERVER_NUMBER_OF_ONLINE_REGIONS = 27;
	public static final int REGION_SERVER_TOTAL_SCANS_SIZE = 28;

	private Configuration config;
	private static HBaseAdmin hbaseAdmin;
	private String ip;
	private int port;

	public HBaseTuner(String ip,int port){
		this.ip=ip;
		this.port=port;
	}

	public void connect()  throws MasterNotRunningException, ZooKeeperConnectionException 
	{
		config = HBaseConfiguration.create();
		config.clear();
		config.set("hbase.zookeeper.quorum", ip);
		config.set("hbase.zookeeper.property.clientPort", Integer.toString(port));

		hbaseAdmin = new HBaseAdmin(config);
	}

	public HBaseAdmin getHBaseAdmin(){
		return hbaseAdmin;
	}

	public static byte[] tobytes(String s){
		return Bytes.toBytes(s);
	}

	public static void createTable(String table, String cf){
		HTableDescriptor ht = new HTableDescriptor();
		ht.setName(table.getBytes());
		HColumnDescriptor colDesc = new HColumnDescriptor(cf);
    	colDesc.setCompressionType(Compression.Algorithm.SNAPPY);
    	ht.addFamily(colDesc);
    	try{
		    hbaseAdmin.createTable(ht);    		
    	}
    	catch(IOException e){
    		e.printStackTrace();
    	}

	}

	public static boolean getStorefiles(String table) throws InterruptedException, IOException{
		boolean storeFiles = false;
		Collection<ServerName> servers = hbaseAdmin.getMaster().getClusterStatus().getServers();
    	for(ServerName server: servers){
    		Map<byte[],RegionLoad> aux = hbaseAdmin.getClusterStatus().getLoad(server).getRegionsLoad();    		
    		Iterator<byte[]> iter = aux.keySet().iterator();
			while(iter.hasNext()){
				byte[] key = iter.next();
				String str = Bytes.toStringBinary(key);
				if (str.startsWith(table)){
					RegionLoad region = (RegionLoad) aux.get(key);
					if (region.getStorefiles() > 1) storeFiles = true;
				}
			}	
    	}
    	return storeFiles;
	}

	public static void removeTable(String table){		
    	try{
    		hbaseAdmin.disableTable(table);
		    hbaseAdmin.deleteTable(table);    		
    	}
    	catch(IOException e){
    		e.printStackTrace();
    	}

	}


	public static Map<Integer,Double> getRegionServerStats(String hostname,int port,boolean verbose) throws Exception {

		Map<Integer,Double> regionServerStats = new HashMap<Integer, Double>();

		String url = "service:jmx:rmi:///jndi/rmi://" + hostname + ":" + port + "/jmxrmi";
		
		System.out.println("Connecting to: " + url);
		
		
		
		JMXServiceURL serviceUrl = new JMXServiceURL(url);
		JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceUrl, null);
		try {
			MBeanServerConnection mbeanConn = jmxConnector.getMBeanServerConnection();
			
			Set<ObjectName> beanSet = mbeanConn.queryNames(new ObjectName("hadoop:service=RegionServer,name=RegionServerStatistics"), null);
			for(ObjectName obj : beanSet) {

				System.out.println("MBean is " + obj);

				double rRC = new Double((Long) mbeanConn.getAttribute(obj,"readRequestsCount"));
				double wRC = new Double((Long) mbeanConn.getAttribute(obj,"writeRequestsCount"));
				double bCEC = new Double((Long) mbeanConn.getAttribute(obj,"blockCacheEvictedCount"));
				double bCHR = new Double((Integer) mbeanConn.getAttribute(obj,"blockCacheHitRatio"));
				double bCHCR = new Double((Integer) mbeanConn.getAttribute(obj,"blockCacheHitCachingRatio"));
				double hBLI = new Double((Integer) mbeanConn.getAttribute(obj,"hdfsBlocksLocalityIndex"));
				double rPS = new Double((Float) mbeanConn.getAttribute(obj,"requests"));
				double nOR = new Double((Integer) mbeanConn.getAttribute(obj,"regions"));
				double tSS = new Double((Long) mbeanConn.getAttribute(obj,"totalScansSize"));

				regionServerStats.put(REGION_SERVER_READ_REQUEST_COUNT, rRC);
				regionServerStats.put(REGION_SERVER_WRITE_REQUEST_COUNT, wRC);
				regionServerStats.put(REGION_SERVER_BLOCK_CACHE_EVICTED_COUNT, bCEC);
				regionServerStats.put(REGION_SERVER_BLOCK_CACHE_HIT_RATIO, bCHR);
				regionServerStats.put(REGION_SERVER_HDFS_BLOCKS_LOCALITY_INDEX, hBLI);
				regionServerStats.put(REGION_SERVER_REQUESTS_PER_SECOND, rPS);
				regionServerStats.put(REGION_SERVER_NUMBER_OF_ONLINE_REGIONS, nOR);
				regionServerStats.put(REGION_SERVER_TOTAL_SCANS_SIZE, tSS);


				if (verbose) {
					System.out.println("readRequestsCount: " + rRC);
					System.out.println("writeRequestsCount: " + wRC);
					System.out.println("blockCacheEvictedCount: " + bCEC);
					System.out.println("blockCacheEvictedCount: " + bCHR);
					System.out.println("blockCacheHitCachingRatio: " + bCHCR);
					System.out.println("hdfsBlocksLocalityIndex: " + hBLI);
					System.out.println("requestsPerSecond: " + rPS);
					System.out.println("numberOfOnlineRegions: " + nOR);
					System.out.println("totalScansSize: " + tSS);
				}
			}
		} finally {
			jmxConnector.close();
		}


		return regionServerStats;

	}


	public static void main (String[] args) throws Exception{

		if (args != null && args.length < 2) {
			System.out.println("HBaseTuner ip port");
			System.exit(-1);
		}


		HBaseTuner hbTunner = new HBaseTuner(args[0],new Integer(args[1]));
        //hbTunner.connect();
        //hbTunner.getHBaseAdmin();
		GatewayServer gatewayServer = new GatewayServer(hbTunner,25333);
		gatewayServer.start();

		System.out.println("Gateway Started");


//		getRegionServerStats("10.0.8.6",10102, true);

		/*
		HBaseAdmin admin = hbTunner.getHBaseAdmin();

        //Get Cluster Status
		ClusterStatus status = admin.getClusterStatus();


        //Get All Server Names
        Collection<ServerName> servers = status.getServers();

        //iterate over all servers
        for (ServerName sname : servers){



            HServerLoad sload = status.getLoad(sname);

            System.out.println(sname.toString()+" has "+ sload.getNumberOfRequests() + " requests.");


            Map<byte[],RegionLoad> regions = sload.getRegionsLoad();
            for(byte[] regname : regions.keySet() ) {
                System.out.println("Region name:" + regions.get(regname).getNameAsString().split(",")[0] +
                        " WriteRequestsCount:" + regions.get(regname).getWriteRequestsCount() +
                        " ReadRequestsCount:" + regions.get(regname).getReadRequestsCount());
            }


        }
		 */
	}
}
