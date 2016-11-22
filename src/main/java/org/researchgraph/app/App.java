package org.researchgraph.app;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.Label;
import org.researchgraph.configuration.Properties;
import org.researchgraph.crossref.CrossRef;
import org.researchgraph.graph.Graph;
import org.researchgraph.graph.GraphKey;
import org.researchgraph.graph.GraphRelationship;
import org.researchgraph.graph.GraphUtils;
import org.researchgraph.neo4j.Neo4jDatabase;

public class App {
	private static final String CROSSREF_VERSION_FILE = "crossref";
	private static final String DATE_FORMAT = "yyyy-MM-dd";
	
	private static CrossRef crossref;
	private static Neo4jDatabase neo4j;
	private static int counter;
	
	private static final Map<String, Set<GraphKey>> references = new HashMap<String, Set<GraphKey>>();
	
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
	        	        
	        crossref = new CrossRef(crossrefCache, mysqlHost, mysqlPort, mysqlUser, mysqlPassword, mysqlDatabase);
	        
	        neo4j = new Neo4jDatabase(neo4jFolder);	        
	        neo4j.importSchemas(crossref.getSchema());
	        
	        loadReferences("dryad", "referenced_by");
	        processReferences(GraphUtils.RELATIONSHIP_RELATED_TO);
	        
	        loadReferences("dara", GraphUtils.PROPERTY_DOI);
	        loadReferences("orcid", GraphUtils.PROPERTY_DOI);
//	        loadReferences(GraphUtils.SOURCE_CERN, GraphUtils.PROPERTY_DOI);
//	        loadReferences(GraphUtils.SOURCE_DLI, GraphUtils.PROPERTY_DOI);
	        loadReferences("ands", GraphUtils.PROPERTY_DOI);
	        	        
	        processReferences(GraphUtils.RELATIONSHIP_KNOWN_AS);
	        
	        Files.write(Paths.get(versionFolder, CROSSREF_VERSION_FILE), 
	        		new SimpleDateFormat(DATE_FORMAT).format(new Date()).getBytes());
	        
		} catch (Exception e) {
            e.printStackTrace();
            
            System.exit(1);
		}       
	}
	
	private static void loadReferences(final String source, final String property) {
		System.out.println("Loading source: " + source + ", reference: " + property);
		int exists = references.size();
		counter = 0;
	
		try {
			neo4j.createIndex(Label.label(source), property);
			neo4j.enumrateAllNodesWithLabelAndProperty(source, property, (node) -> {
				String keyValue = (String) node.getProperty(GraphUtils.PROPERTY_KEY);
				GraphKey key = new GraphKey(source, keyValue);
				Object dois = node.getProperty(property);
				if (dois instanceof String) {
					loadDoi(key, (String)dois); 	
				} else if (dois instanceof String[]) {
					for (String doi : (String[])dois)
						loadDoi(key, doi);
				}
				++counter;
									
				return true;
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Done. Processed " + counter + " nodes and loaded " + (references.size() - exists) + " new DOI's");
	}
	
	private static void loadDoi(GraphKey key, String ref) {
		String doi = GraphUtils.extractDoi(ref);
		if (null != doi) {
			Set<GraphKey> ids = references.get(doi);
			if (null == ids) 
				references.put(doi, ids = new HashSet<GraphKey>());
			ids.add(key);					
		}
	}
	
	private static void processReferences(String relationships) {
		System.out.println("Processing " + references.size() + " unique DOI's");
		
		counter = 0;
		
		Graph graph = new Graph();
		for (Map.Entry<String, Set<GraphKey>> entry : references.entrySet()) {
			Graph result = crossref.requestGraph(entry.getKey());
			if (null != result) {
				GraphKey root = result.getRootNode().getKey();
				graph.merge(result);
				
				for (GraphKey key : entry.getValue()) {
					graph.addRelationship(GraphRelationship.builder() 
						.withRelationship(relationships)
						.withStart(key)
						.withEnd(root)
						.build());
				}
			}
			
			if (++counter % 1000 == 0)
				System.out.println("Processed " + counter + " DOI's");
						
			if (graph.getNodesCount() >= 10000 
					|| graph.getRelationshipsCount() >= 10000) {
				System.out.println("Importing data to the Neo4j");
				
				neo4j.importGraph(graph);
				graph = new Graph();
			}				
		}
		
		neo4j.importGraph(graph);
		
		references.clear();
	}
}
