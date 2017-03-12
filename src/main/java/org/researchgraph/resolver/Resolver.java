package org.researchgraph.resolver;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.researchgraph.crossref.Author;
import org.researchgraph.crossref.CrossRef;
import org.researchgraph.crossref.Item;
import org.researchgraph.graph.GraphUtils;

public class Resolver implements Closeable {
	private final CrossRef crossref;
	
	private final Connection con;
	private final PreparedStatement selectAuthority;
	private final PreparedStatement insertAuthority;
	private final PreparedStatement insertWork;
	private final PreparedStatement insertAuthor;
	
	public Resolver(String cache, String host, int port, String user, String password, String database) throws SQLException {
		this.crossref = new CrossRef(cache);
		
		this.con = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + database + "?user=" + user + "&password=" + password);
		
		this.selectAuthority = con.prepareStatement("SELECT autority FROM doi_autority WHERE doi LIKE ?");
		this.insertAuthority = con.prepareStatement("INSERT INTO doi_autority SET doi=?, autority=?, created=NOW()");
		this.insertWork = con.prepareStatement("INSERT INTO doi_resolution SET doi=?, url=?, title=?, year=?, resolved=NOW()", Statement.RETURN_GENERATED_KEYS);
		this.insertAuthor = con.prepareStatement("INSERT INTO doi_author SET resolution_id=?, first_name=?, last_name=?, full_name=?, orcid=?");
	}
	
	private static boolean isCrossbredAuthority(String authority) {
		return CrossRef.AUTHORITY_CROSSREF.equals(authority);
	}
	
	public void resolveDOI() throws Exception {
		try (Statement s = con.createStatement()) {
			enumerateDOI(s);
		}
	}
	
	private void enumerateDOI(Statement s) throws Exception {
		int counter = 0;
		try (ResultSet rs = s.executeQuery("select id, doi from doi_resolution where resolved is null")) {
			while (rs.next()) {
				long resolutionId = rs.getLong(1);
				String doi = rs.getString(2);
				
				String authority = resolveAuthority(doi);
				if (isCrossbredAuthority(authority)) {
					resolveCrossRefDOI(resolutionId, doi);
				}
				
				++counter;
				
				if (counter % 1000 == 0) {
					System.out.println("Processed " + counter + " doi's");
				}
			}
		}
		
		System.out.println("Done. Processed " + counter + " DOI's");
	}
	
	private String resolveAuthority(String doi) throws SQLException {
		String autority = getAuthorityFromDatabase(doi);
		if (StringUtils.isEmpty(autority)) {
			autority = crossref.requestAuthority(doi);
			if (!StringUtils.isEmpty(autority)) {
				saveAutorityToDatabase(doi, autority);
			}
		} 
		
		return autority;
	}
	
	private void resolveCrossRefDOI(long resolutionId, String doi) throws Exception {
		Item work = crossref.requestWork(doi);
		if (null != work) {
			String title = resolveString(work.getTitle());
			if (null != title) {
				
				con.setAutoCommit(false);
				try {
					
					String key = GraphUtils.generateDoiUri(doi);
					String year = work.getIssuedString();
	
					Long workId = saveWorkToDatabase(doi, key, title, year);
					
	                if (null != workId && null != work.getAuthor()) {
	                    for (Author author : work.getAuthor()) {
	                            String firstName = author.getGiven();
	                            String lastName = author.getFamily();
	                            String fullName = author.getFullName();
	                            String orcid = author.getOrcid();
	                            // String authorKey = doi + ":" + fullName;
	
	                            saveAuthorToDatabase(workId, firstName, lastName, 
	                        			fullName, orcid); 
	                    }
	                }
	               
	                con.commit();
				} catch (Exception e) {
					con.rollback();
					
					throw e;
				} finally {
					con.setAutoCommit(true);
				}
			}
		}
	}
	
	private String resolveString(List<String> list) {
		return null != list && list.size() > 0 ? list.get(0) : null;
	}
	
	private String getAuthorityFromDatabase(String doi) throws SQLException {
		selectAuthority.setString(1, doi);
		try (ResultSet rs = selectAuthority.executeQuery()) {
			if (rs.next()) {
				return rs.getString(1);
			}
		}
		
		return null;
	}
	
	private boolean saveAutorityToDatabase(String doi, String autority) throws SQLException {
		insertAuthority.setString(1, doi);
		insertAuthority.setString(2, autority);
		return insertAuthority.execute();
	}
	
	
	private Long saveWorkToDatabase(String doi, String url, String title, String year) throws Exception {
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

	
	@Override
	public void close() throws IOException {
		try {
			insertWork.close();
			
			con.close();
		} catch (SQLException e) {
			throw new IOException("Unable to close database connection", e);
		}
	}


}