package org.researchgraph.app;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.researchgraph.configuration.Properties;
import org.researchgraph.resolver.Resolver;

public class App {
	private static final String CROSSREF_VERSION_FILE = "crossref";
	private static final String DATE_FORMAT = "yyyy-MM-dd";
		
	public static void main(String[] args) {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			
			Configuration properties = Properties.fromArgs(args);
	        
	        String neo4jFolder = properties.getString(Properties.PROPERTY_NEO4J_FOLDER);
	        if (StringUtils.isEmpty(neo4jFolder))
	            throw new IllegalArgumentException("Neo4j Folder can not be empty");
	        System.out.println("Neo4J: " + neo4jFolder);
	     
	        String crossrefCache = properties.getString(Properties.PROPERTY_CROSSREF_CACHE);
	        System.out.println("CrossRef: " + crossrefCache);
	        
	        String versionFolder = properties.getString(Properties.PROPERTY_VERSIONS_FOLDER);
	        String mysqlHost = properties.getString(Properties.PROPERTY_MYSQL_HOST);
	        int mysqlPort = properties.getInt(Properties.DEFAULT_MYSQL_PORT);
	        String mysqlUser = properties.getString(Properties.PROPERTY_MYSQL_USER);
	        String mysqlPassword = properties.getString(Properties.PROPERTY_MYSQL_PASSWORD);
	        String mysqlDatabase = properties.getString(Properties.PROPERTY_MYSQL_DATABASE);
	        
	        try (Resolver resolver = new Resolver(crossrefCache, mysqlHost, mysqlPort, mysqlUser, mysqlPassword, mysqlDatabase)) {
	        	resolver.resolveDOI();
	        }
	        
	        Files.write(Paths.get(versionFolder, CROSSREF_VERSION_FILE), 
	        		new SimpleDateFormat(DATE_FORMAT).format(new Date()).getBytes());
	        
		} catch (Exception e) {
            e.printStackTrace();
            
            System.exit(1);
		}       
	}
}
