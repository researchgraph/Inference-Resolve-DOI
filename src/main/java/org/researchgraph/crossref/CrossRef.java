package org.researchgraph.crossref;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.researchgraph.graph.Graph;
import org.researchgraph.graph.GraphKey;
import org.researchgraph.graph.GraphNode;
import org.researchgraph.graph.GraphRelationship;
import org.researchgraph.graph.GraphSchema;
import org.researchgraph.graph.GraphUtils;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;

/**
 * Main class for CrossRef library
 * 
 * @author Dima Kudriavcev, dmitrij@kudriavcev.info
 * @version 1.0.0
 */
public class CrossRef {
	public static final String AUTHORITY_CROSSREF = "CrossRef";
	
	private static final String SOURCE_CROSSREF = "crossref";
	private static final String SOURCE_URL_CROSSREF ="crossref.org";
	
	private static final String URL_CROSSREF = "http://api.crossref.org/";
	private static final String URL_CROSSREF_DOI = "http://doi.crossref.org/";

	private static final String CACHE_WORKS = "works";
	private static final String CACHE_AUTHORITY = "authority";
	
	private static final String FUNCTION_WORKS = "works";
	private static final String FUNCTION_DOI_RA = "doiRA";
	/*private static final String FUNCTION_FUNDERS = "funders";
	private static final String FUNCTION_MEMBERS = "members";
	private static final String FUNCTION_TYPES = "types";
	private static final String FUNCTION_LICENSES = "licenses";
	private static final String FUNCTION_JOURNALS = "journals";*/
	
	private static final String URL_CROSSREF_WORKDS = URL_CROSSREF + FUNCTION_WORKS;
	private static final String URL_CROSSREF_DOI_RA = URL_CROSSREF_DOI + FUNCTION_DOI_RA;
	/*private static final String URL_CROSSREF_FUNDERS = URL_CROSSREF + FUNCTION_FUNDERS;
	private static final String URL_CROSSREF_MEMBERS = URL_CROSSREF + FUNCTION_MEMBERS;
	private static final String URL_CROSSREF_TYPES = URL_CROSSREF + FUNCTION_TYPES;
	private static final String URL_CROSSREF_LICENSES = URL_CROSSREF + FUNCTION_LICENSES;
	private static final String URL_CROSSREF_JOURNALS = URL_CROSSREF + FUNCTION_JOURNALS;*/
		
	private static final String URL_ENCODING = "UTF-8";
	
	/*private static final String PARAM_QUERY = "q";
	private static final String PARAM_HEADER = "header";*/
	
	private static final String STATUS_OK = "ok";
	
	private static final String MESSAGE_WORK = "work";
	private static final String MESSAGE_WORK_LIST = "work-list";
	
	private static final String EXT_JSON = ".json";
	
	private static final String PART_DOI = "doi:";
	
	private static final String PROTOCOL_S3 = "s3";
	
	private static final String PREFIX_ROOT = "/";
	
	private final File cache;
	
	private final Connection conn;
	private final PreparedStatement selectAutority;
	private final PreparedStatement selectWork;
	private final PreparedStatement selectAuthors;
	private final PreparedStatement insertAutority;
	private final PreparedStatement insertWork;
	private final PreparedStatement insertAuthor;
	
	private final AmazonS3 s3Client;
	private final String s3Bucket;
	private final String s3Prefix;
	
	private long maxAttempts = 10;
	private long attemptDelay = 1000;
	private boolean dbaEnabled = true;
	
	private static final ObjectMapper mapper = new ObjectMapper();   
	private static final TypeReference<Response<ItemList>> itemListType = new TypeReference<Response<ItemList>>() {};   
	private static final TypeReference<Response<Item>> itemType = new TypeReference<Response<Item>>() {};
	private static final TypeReference<List<Authority>> authorityListType = new TypeReference<List<Authority>>() {};

	
	public CrossRef(String cache, String host, int port, String user, String password, String database) throws SQLException {
		this.conn = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database + "?user=" + user + "&password=" + password);
		
		this.selectAutority = conn.prepareStatement("SELECT autority FROM doi_autority WHERE doi LIKE ?");
		this.selectWork = conn.prepareStatement("SELECT resolution_id, url, title, year FROM doi_resolution WHERE doi LIKE ?");
		this.selectAuthors = conn.prepareStatement("SELECT first_name, last_name, full_name, orcid FROM doi_author WHERE resolution_id=?");
		this.insertAutority = conn.prepareStatement("INSERT INTO doi_autority SET doi=?, autority=?, created=NOW()");
		this.insertWork = conn.prepareStatement("INSERT INTO doi_resolution SET doi=?, url=?, title=?, year=?, created=NOW()", Statement.RETURN_GENERATED_KEYS);
		this.insertAuthor = conn.prepareStatement("INSERT INTO doi_author SET resolution_id=?, first_name=?, last_name=?, full_name=?, orcid=?, created=NOW()");
		
		URI uri = URI.create(cache);
		if (null == uri.getScheme()) {
			// local cache
			this.cache = new File(cache);
			this.cache.mkdirs();
			
			File cacheWorks = new File(this.cache, CACHE_WORKS);
			cacheWorks.mkdirs();
			
			File cacheAuthority = new File(this.cache, CACHE_AUTHORITY);
			cacheAuthority.mkdirs();
			
			this.s3Client = null;
			this.s3Bucket = null;
			this.s3Prefix = null;
		} else if (uri.getScheme().toLowerCase().equals(PROTOCOL_S3)) {
			this.s3Client = new AmazonS3Client(new InstanceProfileCredentialsProvider());
			this.s3Bucket = uri.getHost();
			this.s3Prefix = StringUtils.isEmpty(uri.getPath()) ? PREFIX_ROOT : uri.getPath();
			
			this.cache  = null;
		} else {
			throw new IllegalArgumentException("Invalid cache sheme: " + uri.getScheme());
		}
		
		
	}
	/*
	static {
		SimpleModule module = new SimpleModule("DateModule");
		module = module.addDeserializer(Date.class, new CrossRefDateDeserializer());
		mapper.registerModule(module);
	}*/
	
	/**
	 * Request all works
	 * @return ItemList - a list of works
	 */
	public ItemList requestWorks() {
		try {
			String json = get(URL_CROSSREF_WORKDS);
			if (null != json) {			
				Response<ItemList> response = mapper.readValue(json, itemListType);
				
				//System.out.println(response);
				
				if (response.getStatus().equals(STATUS_OK) && 
					response.getMessageType().equals(MESSAGE_WORK_LIST)) 
					return response.getMessage();
			}		
			else
				System.err.println("Inavlid response");
			
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	private GraphNode createPublication(String doi, String url, String title, String year) {
		//String doiUri = GraphUtils.generateDoiUri(doi);
		return GraphNode.builder()
				.withKey(new GraphKey(SOURCE_CROSSREF, url))
				.withNodeSource(SOURCE_URL_CROSSREF)
				.withNodeType(GraphUtils.TYPE_PUBLICATION)
				.withLabel(SOURCE_CROSSREF)
				.withLabel(GraphUtils.TYPE_PUBLICATION)
				.withProperty(GraphUtils.PROPERTY_DOI, doi)
				.withProperty(GraphUtils.PROPERTY_URL, url)
				.withProperty(GraphUtils.PROPERTY_TITLE, title)
				.withProperty(GraphUtils.PROPERTY_PUBLISHED_YEAR, year)
				.build();
	}
	
	private GraphNode createAuthor(String key, String firstName, String lastName, String fullName, String orcid) {
		return GraphNode.builder()
				.withKey(new GraphKey(SOURCE_CROSSREF, key))
				.withNodeSource(SOURCE_URL_CROSSREF)
				.withNodeType(GraphUtils.TYPE_RESEARCHER)
				.withLabel(SOURCE_CROSSREF)
				.withLabel(GraphUtils.TYPE_RESEARCHER)
				.withProperty(GraphUtils.PROPERTY_FIRST_NAME, firstName)
				.withProperty(GraphUtils.PROPERTY_LAST_NAME, lastName)
				.withProperty(GraphUtils.PROPERTY_FULL_NAME, fullName)
				.withProperty(GraphUtils.PROPERTY_ORCID_ID, orcid)
				.build();
		
	}
	
	private GraphRelationship createRelationship(GraphNode publication, GraphNode author) { 
		return GraphRelationship.builder()
				.withRelationship(GraphUtils.RELATIONSHIP_RELATED_TO)
				.withStart(publication.getKey())
				.withEnd(author.getKey())
				.build();
	}

	/**
	 * Request work by doi identificator
	 * @param doi String containing doi identificator
	 * @return Item - work information
	 */
	public Item requestWork(String doi) {
		try {
			String encodedDoi = encodeWorkDoi(doi);
			String cachedFile = getWorkFileName(encodedDoi);
			String json = getCahcedFile(cachedFile);
				
			if (null == json) {
				json = getWork(encodedDoi);
				saveCacheFile(cachedFile, json);
			}
				
			if (null != json) {
				return parseWork(json);
			}
			
			
			System.err.println("Inavlid response");			
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	public String requestAuthority(String doi) {
		try {
			String autority = getAutorityFromDatabase(doi);
			if (StringUtils.isEmpty(autority)) {
				String encodedDoi = encodeAuthorityDoi(doi);
				String cachedFile = getAutorityFileName(encodedDoi);
				String json = getCahcedFile(cachedFile);
				
				if (null == json) {
					json = getAuthority(encodedDoi);
					saveCacheFile(cachedFile, json);
				}
				
				if (null != json) {
					autority = parseAuthority(json);
					if (!StringUtils.isEmpty(autority)) {
						saveAutorityToDatabase(doi, autority);
					} else {
						System.err.println("Inavlid response");
					}
				}
			}
			
			return autority;
		} catch (Exception e) {
			e.printStackTrace();
		} 
		
		return null;
	}
	
	private String getAutorityFromDatabase(String doi) throws SQLException {
		selectAutority.setString(1, doi);
		try (ResultSet rs = selectAutority.executeQuery()) {
			if (rs.next()) {
				return rs.getString(1);
			}
		}
		
		return null;
	}
	
	private boolean saveAutorityToDatabase(String doi, String autority) throws SQLException {
		insertAutority.setString(1, doi);
		insertAutority.setString(2, autority);
		return insertAutority.execute();
	}
	
	private Graph getWorkFromDatabase(String doi) throws SQLException {
		
		selectWork.setString(1, doi);
		try (ResultSet rsWork = selectWork.executeQuery()) {
			if (rsWork.next()) {
				Graph graph = new Graph();
				
				long resolutionId = rsWork.getLong(1);
				String key = rsWork.getString(2);
				String title = rsWork.getString(3);
				String year = rsWork.getString(4);
				
				GraphNode publication = createPublication(doi, key, title, year);
				
				graph.addNode(publication);
					
				selectAuthors.setLong(1, resolutionId);
				try (ResultSet rsAuthor = selectAuthors.executeQuery()) {
					while (rsAuthor.next()) {
						String firstName = rsAuthor.getString(1);
						String lastName = rsAuthor.getString(2);
						String fullName = rsAuthor.getString(3);
						String orcid = rsAuthor.getString(4);
						
						publication.addProperty(GraphUtils.PROPERTY_AUTHORS, fullName);
						
						GraphNode author = createAuthor(key, firstName, lastName, fullName, orcid);
						
						graph.addNode(author);
						graph.addRelationship(createRelationship(publication, author));
					}
				}
				
				return graph;
				
			}
		}
		
		return null;
	}
	
	private Long saveWorkToDatabase(String doi, String url, String title, String year) throws SQLException {
		insertWork.setString(1, doi);
		insertWork.setString(2, url);
		insertWork.setString(3, title);
		insertWork.setString(4, year);
		if (insertWork.execute()) {
			try (ResultSet rs = insertWork.getGeneratedKeys()) {
	            if(rs.next())
	            {
	                return rs.getLong(1);
	            }
			}
		}
		
		return null;
	}

	private boolean saveAuthorToDatabase(long resolutionId, String firstName, String lastName, 
			String fullName, String orcid) throws SQLException {
		insertAuthor.setLong(1, resolutionId);
		insertAuthor.setString(2, firstName);
		insertAuthor.setString(3, lastName);
		insertAuthor.setString(4, fullName);
		insertAuthor.setString(5, orcid);
		return insertAuthor.execute();
	}

	
	private String resolveString(List<String> list) {
		return null != list && list.size() > 0 ? list.get(0) : null;
	}
	
	public Graph requestGraph(String doi) {
		try {
			Graph graph = getWorkFromDatabase(doi);
			if (null == graph) {
				String authority = requestAuthority(doi);
				if (AUTHORITY_CROSSREF.equals(authority)) {
					Item work = requestWork(doi);
					if (null != work) {
						String title = resolveString(work.getTitle());
						if (null != title) {
							String key = GraphUtils.generateDoiUri(doi);
							String year = work.getIssuedString();
							graph = new Graph();
							GraphNode publication = createPublication(doi, key, title, year);
							Long workId = saveWorkToDatabase(doi, key, title, year);
							graph.addNode(publication);
							
                            if (null != workId && null != work.getAuthor()) {
                                for (Author author : work.getAuthor()) {
                                        String firstName = author.getGiven();
                                        String lastName = author.getFamily();
                                        String fullName = author.getFullName();
                                        String orcid = author.getOrcid();
                                        String authorKey = doi + ":" + fullName;

                                        publication.addProperty(GraphUtils.PROPERTY_AUTHORS, fullName);
                                        
                                        GraphNode researcher = createAuthor(authorKey, 
                                        		firstName, lastName, fullName, orcid);
                                        saveAuthorToDatabase(workId, firstName, lastName, 
                                    			fullName, orcid); 

                                        graph.addNode(researcher);
                                        graph.addRelationship(createRelationship(publication, researcher));
                                }
                            }
						}
					}
				}
			}
			
			
			return graph;
		
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	
	private String get( final String url ) {
		System.out.println("Downloading: " + url);
						
		long delay = attemptDelay;
		long attemps = maxAttempts;
		for (;;) {
			try {
				ClientResponse response = Client.create()
										  .resource( url )
										  .accept( MediaType.APPLICATION_JSON ) 
										  .get( ClientResponse.class );
				
				if (response.getStatus() == 200) 
					return response.getEntity( String.class );
				else
					return null;
				
			} catch (Exception e) {
				if (attemps <= 0)
					throw e;
				
				--attemps;
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e1) {
					throw e;
				}
				if (dbaEnabled)
					delay = delay * 2;
			}
		}
    } 
	
	private String encodeWorkDoi(String doi) throws UnsupportedEncodingException {
		return URLEncoder.encode(PART_DOI + doi, URL_ENCODING);
	}

	private String encodeAuthorityDoi(String doi) throws UnsupportedEncodingException {
		return URLEncoder.encode(doi, URL_ENCODING);
	}

	private String getAutorityFileName(String encodedDoi) {
		return CACHE_WORKS + "/" + encodedDoi + EXT_JSON;
	}
	
	private String getWorkFileName(String encodedDoi) {
		return CACHE_AUTHORITY + "/" + encodedDoi + EXT_JSON;
	}
	
	private String getS3Key(String file) {
		return s3Prefix + file;
	}
	
	private String getCahcedFile(String file) throws IOException { 
		if (null != cache) {
			File f = new File(cache, file);
			if (f.exists() && !f.isDirectory()) {
				return FileUtils.readFileToString(f); 
			}
			
		} else if (null != s3Client) {
			S3Object o = s3Client.getObject(new GetObjectRequest(s3Bucket, getS3Key(file)));
			if (null != o) {
				try (InputStream is = o.getObjectContent()) {
					return IOUtils.toString(is);
				}
			}
		}
		
		return null;
	}

	private void saveCacheFile(String file, String json) throws IOException {
		if (null != file && null != json && !json.isEmpty()) {
			if (null != cache) {
				FileUtils.write(new File(cache, file), json);
			} else if (null != s3Client) {
				byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
				
				ObjectMetadata metadata = new ObjectMetadata();
		        metadata.setContentEncoding(StandardCharsets.UTF_8.name());
		        metadata.setContentType("text/json");
		        metadata.setContentLength(bytes.length);
		
		        InputStream inputStream = new ByteArrayInputStream(bytes);
				
				s3Client.putObject(new PutObjectRequest(s3Bucket, getS3Key(file), inputStream, metadata));
			}
		}
	}
	
	private String getWork(String encodedDoi) {
		return get(URL_CROSSREF_WORKDS + "/" + encodedDoi.replace("%2F", "/"));
	}
	
	private String getAuthority(String encodedDoi) {
		return get(URL_CROSSREF_DOI_RA + "/" + encodedDoi.replace("%2F", "/"));
	}
	
	private Item parseWork(String json) throws JsonParseException, JsonMappingException, IOException {
		Response<Item> response = mapper.readValue(json, itemType);
		
		//System.out.println(response);
		
		if (response.getStatus().equals(STATUS_OK) && 
			response.getMessageType().equals(MESSAGE_WORK)) 
			return response.getMessage();
		else
			return null;
	}
	
	private String parseAuthority(String json) throws JsonParseException, JsonMappingException, IOException {
		List<Authority> authorities = mapper.readValue(json, authorityListType);
		
		//System.out.println(response);
		
		if (null == authorities)
			return null;
		
		for (Authority authority : authorities) {
			if (authority.getAuthority() != null)
				return authority.getAuthority();
			
			if (authority.getStatus() != null) 
				System.err.println(authority.getStatus());
		}
		
		return null;
	}
	

	public long getMaxAttempts() {
		return maxAttempts;
	}

	public void setMaxAttempts(long maxAttempts) {
		this.maxAttempts = maxAttempts;
	}

	public long getAttemptDelay() {
		return attemptDelay;
	}

	public void setAttemptDelay(long attemptDelay) {
		this.attemptDelay = attemptDelay;
	}

	public boolean isDbaEnabled() {
		return dbaEnabled;
	}

	public void setDbaEnabled(boolean dbaEnabled) {
		this.dbaEnabled = dbaEnabled;
	}
	
	public List<GraphSchema> getSchema() {
		List<GraphSchema> schemas = new ArrayList<GraphSchema>();
		schemas.add(new GraphSchema(SOURCE_CROSSREF, GraphUtils.PROPERTY_KEY, true));
		schemas.add(new GraphSchema(SOURCE_CROSSREF, GraphUtils.PROPERTY_DOI, false));
		schemas.add(new GraphSchema(SOURCE_CROSSREF, GraphUtils.PROPERTY_URL, false));
		//schemas.add(new GraphSchema(SOURCE_CROSSREF, GraphUtils.PROPERTY_ORCID_ID, false));
		
		return schemas;
	}
	
}
