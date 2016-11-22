package org.researchgraph.configuration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

public class Properties {
	public static final Path CONFIG_FILE = Paths.get("import.conf");
	
	public static final String PROPERTY_NEO4J_FOLDER = "neo4j";
	public static final String PROPERTY_CROSSREF_CACHE = "cache";
	public static final String PROPERTY_SOURCE = "source";
	public static final String PROPERTY_VERSIONS_FOLDER = "versions-folder";
	public static final String PROPERTY_MYSQL_HOST = "mysql-host";
	public static final String PROPERTY_MYSQL_PORT = "mysql-port";
	public static final String PROPERTY_MYSQL_USER = "mysql-user";
	public static final String PROPERTY_MYSQL_PASSWORD = "mysql-password";
	public static final String PROPERTY_MYSQL_DATABASE = "mysql-database";
	public static final String PROPERTY_CONFIG_FILE = "config-file";
	public static final String PROPERTY_HELP = "help";
	
	public static final String DEFAULT_NEO4J_FOLDER = "neo4j";
	public static final String DEFAULT_CROSSREF_CACHE = "crossref/cache";
	public static final String DEFAULT_MYSQL_HOST = "localhost";
	public static final String DEFAULT_MYSQL_PORT = "3306";
	public static final String DEFAULT_MYSQL_DATABASE = "crossref";
	
	public static Configuration fromArgs(String[] args) throws Exception {
		CommandLineParser parser = new DefaultParser();
		
		// create the Options
		Options options = new Options();
		options.addOption( "n", PROPERTY_NEO4J_FOLDER, true, "Neo4J Folder" );
		options.addOption( "C", PROPERTY_CROSSREF_CACHE, true, "CrossRef Cache (Folder or S3 Bucket)" );
		options.addOption( "s", PROPERTY_SOURCE, true, "Source name" );
		options.addOption( "v", PROPERTY_VERSIONS_FOLDER, true, "Versions folder" );
		options.addOption( "H", PROPERTY_MYSQL_HOST, true, "MySQL Host" );
		options.addOption( "P", PROPERTY_MYSQL_PORT, true, "MySQL Port" );
		options.addOption( "u", PROPERTY_MYSQL_USER, true, "MySQL User" );
		options.addOption( "p", PROPERTY_MYSQL_PASSWORD, true, "MySQL Password" );
		options.addOption( "d", PROPERTY_MYSQL_DATABASE, true, "MySQL Database" );
		options.addOption( "c", PROPERTY_CONFIG_FILE, true, "configuration file (optional)" );
		options.addOption( "h", PROPERTY_HELP, false, "Print this message" );

		// parse the command line arguments
		CommandLine line = parser.parse( options, args );

		if (line.hasOption( PROPERTY_HELP )) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "java -jar neo4j-importer-[verion].jar [PARAMETERS]", options );
            
            System.exit(0);
		}
		
		// variables to store program properties
		CompositeConfiguration config = new CompositeConfiguration();
		
		BaseConfiguration defaultConfig = new BaseConfiguration();
		defaultConfig.setProperty( PROPERTY_NEO4J_FOLDER, DEFAULT_NEO4J_FOLDER );
		defaultConfig.setProperty( PROPERTY_CROSSREF_CACHE, DEFAULT_CROSSREF_CACHE );
		defaultConfig.setProperty( PROPERTY_MYSQL_HOST, DEFAULT_MYSQL_HOST );
		defaultConfig.setProperty( PROPERTY_MYSQL_PORT, DEFAULT_MYSQL_PORT );
		defaultConfig.setProperty( PROPERTY_MYSQL_DATABASE, DEFAULT_MYSQL_DATABASE );
		
		BaseConfiguration commandLineConfig = new BaseConfiguration();
		
		Path configurationFile = null;
		
		for (Option option : line.getOptions()) {
			if ( PROPERTY_CONFIG_FILE.equals(option.getLongOpt()) ) {
				configurationFile = Paths.get(option.getValue());
			} else {
				commandLineConfig.setProperty(option.getLongOpt(), option.getValue());
			}
		}
		
		if (null == configurationFile) {
			configurationFile = CONFIG_FILE;
		}
		
		if (Files.isRegularFile( configurationFile ) && Files.isReadable( configurationFile )) {
			config.addConfiguration(new PropertiesConfiguration( configurationFile.toFile() ));
		} else {
			if (CONFIG_FILE != configurationFile) {
				throw new Exception("Invalid configuration file: " + configurationFile.toString());
			}
		}
		
		config.addConfiguration(commandLineConfig);
		config.addConfiguration(defaultConfig);
					 
		return config;
	}
}