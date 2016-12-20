package org.researchgraph.crossref;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

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

	public CrossRef(String cache) {
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
			String encodedDoi = encodeAuthorityDoi(doi);
			String cachedFile = getAutorityFileName(encodedDoi);
			String json = getCahcedFile(cachedFile);
			
			if (null == json) {
				json = getAuthority(encodedDoi);
				saveCacheFile(cachedFile, json);
			}
			
			if (null != json) {
				return parseAuthority(json);
			}

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
}
