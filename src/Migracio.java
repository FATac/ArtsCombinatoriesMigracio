import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.csvreader.CsvReader;
import com.google.gson.Gson;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.ibm.icu.text.SimpleDateFormat;

class CustomMap extends HashMap<String, Object>{
	private static final long serialVersionUID = -1812690206134151827L;
	
	public String put(String key, String value) {
		if (key==null) return null;
		key = key.trim();
		Object prev = super.get(key);
		if (prev!=null && value!=null) {
			if (prev instanceof String) {
				super.put(key, new String[]{(String)prev, value});
			} else {
				String[] arr = (String[])prev;
				String[] curr = new String[arr.length+1];
				int i = 0;
				for (String s : arr) curr[i++] = s;
				curr[i] = value;
				
				super.put(key, curr);
			}
		} else {
			super.put(key, value);
		}
		
		return value;
	}
	
	public CustomMap(Map<String, String> map) {
		Set<Map.Entry<String, String>> es = map.entrySet();
		for (Map.Entry<String, String> e : es) {
			this.put(e.getKey(), e.getValue());
		}
	}
	
	public CustomMap() {
		super();
	}
	
}

class CustomList extends ArrayList<Map<String, String>> {
	private static final long serialVersionUID = 6605926931871216809L;

	@Override
	public boolean add(Map<String, String> e) {
		if (e.get("id")!=null) {
			for(Map<String, String> x : this) {
				if (e.get("id").equals(x.get("id"))) {
					return false;
				}
			}
		}
		
		return super.add(e);
	}
}

/* task list
 * TODO: classificar events segons tipus d'activity
 * TODO: migració de relacions
 * TODO: migració subdocuments dels events
 * TODO: migració publications
 * TODO: aclarir què fer amb els subobjectes de publications: editor, distributor,etc. (crear-los i reutilitzar-los?)
 * TODO: migrar dates correctament
 * TODO: migrar relacions entre objectes (última tasca)
 * 
 */

public class Migracio {
	
	/* Objectes recurrents */
	static CustomMap backupAgents = new CustomMap();
	
	static boolean MIGRAR = false;
	static boolean DOWNLOAD_DATA = false;
	
	static int roleCount = 0;
	
	static CustomMap errors = new CustomMap();
	
	static String catalaUri = null;
	static String espanyolUri = null;
	static String anglesUri = null;
	
	/* Statistics */
	private static int error_count = 0;
	
	@SuppressWarnings("unchecked")
	private static List<String> find(String f, String v, String c) throws Exception {
		URL url = new URL("http://localhost:8080/ArtsCombinatoriesRest/specific?f="+f+"&v="+v+"&c="+c);
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestMethod("GET");
		
	    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	    String str;
	    StringBuffer sb = new StringBuffer();
	    while ((str = rd.readLine()) != null) {
	    	sb.append(str);
	    	sb.append("\n");
	    }

	    rd.close();
	    conn.disconnect();
	    
	    return new Gson().fromJson(sb.toString(), List.class);
	}
	
	private static String uploadObject(CustomMap data) throws Exception {
		if (data==null) return null;
		if (data.get("className")==null) {
			System.out.println("HEREEEE!!!");
			System.exit(0);
		}
		System.out.println("Migrating data: " + data);
		if (MIGRAR) {
			URL url = new URL("http://localhost:8080/ArtsCombinatoriesRest/objects/upload");
		    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		    conn.setRequestProperty("Content-Type", "application/json");
		    conn.setRequestMethod("POST");
		    conn.setDoOutput(true);
		    OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
		    wr.write(new Gson().toJson(data));
		    wr.flush();
		    wr.close();
		    
		    // Get the response
		    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		    String str;
		    StringBuffer sb = new StringBuffer();
		    while ((str = rd.readLine()) != null) {
		    	sb.append(str);
		    	sb.append("\n");
		    }

		    rd.close();
		    conn.disconnect();
		    
		    if (sb.toString().equals("error")) {
		    	error_count++;
		    	throw new Exception("Error uploading object");
		    }
		    
		    String res = sb.toString();
		    System.out.println("SERVER RESPONSE: " +  res);
		    return res;
		}
		
		return null;
	}
	
	private static String updateObject(String id, CustomMap data) throws Exception {
		//System.out.println("Migrating data: id=" + id + " " + data);
		if (data==null) return null;
		if (MIGRAR) {
			URL url = new URL("http://localhost:8080/ArtsCombinatoriesRest/objects/"+id+"/update");
		    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		    conn.setRequestProperty("Content-Type", "application/json");
		    conn.setRequestMethod("POST");
		    conn.setDoOutput(true);
		    OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
		    wr.write(new Gson().toJson(data));
		    wr.flush();
		    wr.close();
		    
		    // Get the response
		    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		    String str;
		    StringBuffer sb = new StringBuffer();
		    while ((str = rd.readLine()) != null) {
		    	sb.append(str);
		    	sb.append("\n");
		    }

		    rd.close();
		    conn.disconnect();
		    
		    if (sb.toString().equals("error")) throw new Exception("Error updating object");
		    
		    String res = sb.toString();
		    System.out.println("SERVER RESPONSE: " +  res);
		    return res;
		}
		
		return null;
	}
	
	@SuppressWarnings("unchecked")
	private static CustomMap getObject(String id) throws Exception {
		URL url = new URL("http://localhost:8080/ArtsCombinatoriesRest/"+id);
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestMethod("GET");
		
	    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	    String str;
	    StringBuffer sb = new StringBuffer();
	    while ((str = rd.readLine()) != null) {
	    	sb.append(str);
	    	sb.append("\n");
	    }

	    rd.close();
	    conn.disconnect();
	    
	    return new Gson().fromJson(sb.toString(), CustomMap.class);
	}
	
	private static void migrarPersons() {
		try {
			// baixar info de persones 
			if (DOWNLOAD_DATA) {
				System.out.println("Downloading persons rdf data...");
				URL personsUrl = new URL("http://www.fundaciotapies.org/site/spip.php?page=all-participants-rdf");
				BufferedReader in = new BufferedReader(new InputStreamReader(personsUrl.openStream()));
				
				File personsFile = new File("persons.rdf");
				FileWriter personsFileWriter = new FileWriter(personsFile);
		
				char[] input = new char[255];
				int b = 0;
				while ((b = in.read(input)) > 0) personsFileWriter.write(input, 0, b);
				in.close();
			
				personsFileWriter.flush();
				personsFileWriter.close();
			}
			
			Model model = ModelFactory.createDefaultModel();
			model.read("file:persons.rdf");
			
			// 	per a cada persona...
			StmtIterator it1 = model.listStatements();
			while(it1.hasNext()) {
				Statement stmt = it1.next();
			    //Resource  subject   = stmt.getSubject();     			// get the subject
			    Property  predicate = stmt.getPredicate();   			// get the predicate
			    RDFNode   object    = stmt.getObject();      			// get the object
			   
			    // obtenir les seves dades completes...
			    if (predicate.toString().equals("http://www.fundaciotapies.org/terms/0.1/rdfuri")) {
			    	try {
			    		String url = object.toString();
				    	URL personUrl = new URL(url);
				    	//URL personUrl = new URL("http://www.fundaciotapies.org/site/spip.php?page=xml-participant&id_article=4081");
				    	
				    	BufferedReader in = new BufferedReader(new InputStreamReader(personUrl.openStream()));
						
						File personFile = new File("currentPerson.rdf");
						if (personFile.exists()) {
							personFile.delete();
							personFile = new File("currentPerson.rdf");
						}
						
						FileWriter personFileWriter = new FileWriter(personFile);
				
						char[] input = new char[255];
						int b = 0;
						while ((b = in.read(input)) > 0) personFileWriter.write(input, 0, b);
						in.close();
						
						personFileWriter.flush();
						personFileWriter.close();
						
						Model model1 = ModelFactory.createDefaultModel();
						model1.read("file:currentPerson.rdf");
						
						StmtIterator it2 = model1.listStatements();
						CustomMap data = new CustomMap();
						String fatacId = url.substring(url.length()-4);
						if (fatacId.startsWith("=")) fatacId = fatacId.substring(1);
						data.put("FatacId", fatacId );
						data.put("className", "foaf:Person");
						//System.out.println("FatacId " + fatacId);
						
						System.out.println("Reading person...");
						
						while(it2.hasNext()) {
							Statement stmt1 = it2.next();
							
							Resource  subject1   = stmt1.getSubject();     	// get the subject
						    Property  predicate1 = stmt1.getPredicate();   	// get the predicate
						    RDFNode   object1   = stmt1.getObject();      	// get the object
						    
						    //System.out.println(stmt1);
						    String content = predicate1.toString();
							
						    if (subject1.isURIResource()) {
						    	String currentUri = subject1.toString();
						    	
						    	if (currentUri.contains("participants")) {
						    		if (content.equals("http://xmlns.com/foaf/0.1/givenName")) {
										data.put("foaf:givenName", object1.toString());
										//System.out.println("name " + object1.toString());
									} else if (content.equals("http://xmlns.com/foaf/0.1/familyName")) {
										data.put("foaf:lastName", object1.toString());
										//System.out.println("surname " + object1.toString());
									} else if (content.equals("http://purl.org/vocab/bio/0.1/biography")) {
										String bio = object1.toString().replaceAll(" class=\"spip\"", "").replaceAll(" class=\"spip_out\"","");	
										data.put("CV", bio);
										//System.out.println("CV " + bio);
									} else if (content.equals("http://purl.org/vocab/bio/0.1/date")) {
										data.put("BirthDate", object1.toString());
										//System.out.println("BirthDate " + object1.toString());
									}
						    	}
						    }
						}
						
						System.out.println("Uploading person...");
						uploadObject(data);
						System.out.println("Uploaded. ");
						
						System.exit(0);
			    	} catch (Exception e) {
			    		System.out.println("ERROR with person " + object.toString());
			    		e.printStackTrace();
			    	}
			    }
			}
		} catch (Exception e) {
			System.out.println("Error migrating persons " + e);
			e.printStackTrace();
		}
	}
	
	public static void migrarEvents() {
		try {
			// migrar events
			if (DOWNLOAD_DATA) {
				System.out.println("Downloading events rdf data...");
				URL eventsUrl = new URL("http://www.fundaciotapies.org/site/spip.php?page=all-events-rdf");
				BufferedReader in = new BufferedReader(new InputStreamReader(eventsUrl.openStream()));
				
				File eventsFile = new File("events.rdf");
				FileWriter eventsFileWriter = new FileWriter(eventsFile);
		
				char[] input = new char[255];
				int b = 0;
				while ((b = in.read(input)) > 0) eventsFileWriter.write(input, 0, b);
				in.close();
				
				eventsFileWriter.flush();
				eventsFileWriter.close();
			}
			
			Model model = ModelFactory.createDefaultModel();
			model.read("file:events.rdf");
			
			//	per a cada event...
			StmtIterator it1 = model.listStatements();
			while(it1.hasNext()) {
				Statement stmt = it1.next();
		    	System.out.println(stmt);
			    //Resource  subject   = stmt.getSubject();     			// get the subject
			    Property  predicate = stmt.getPredicate();   			// get the predicate
			    RDFNode   object    = stmt.getObject();      			// get the object
			   
			    // obtenir les seves dades completes...
			    if (predicate.toString().equals("http://www.fundaciotapies.org/terms/0.1/rdfuri")) {
			    	try {
			    		String url = object.toString();
				    	URL eventUrl = new URL(url);
				    	
				    	BufferedReader in = new BufferedReader(new InputStreamReader(eventUrl.openStream()));
						
						File eventFile = new File("currentEvent.rdf");
						if (eventFile.exists()) {
							eventFile.delete();
							eventFile = new File("currentEvent.rdf");
						}
						FileWriter eventFileWriter = new FileWriter(eventFile);
				
						char[] input = new char[255];
						int b = 0;
						while ((b = in.read(input)) > 0) eventFileWriter.write(input, 0, b);
						in.close();
						
						eventFileWriter.flush();
						eventFileWriter.close();
						
						Model model1 = ModelFactory.createDefaultModel();
						model1.read("file:currentEvent.rdf");
						
						StmtIterator it2 = model1.listStatements();
						CustomMap data = new CustomMap();
						data.put("className", "FrameActivity");
						CustomMap caseFile = new CustomMap();
						caseFile.put("className", "CulturalManagement");
						
						String fatacId = url.substring(url.length()-4);
						if (fatacId.startsWith("=")) fatacId = fatacId.substring(1);
						data.put("FatacId", fatacId );
						
						Map<String, Map<String, String>> documents = new HashMap<String, Map<String,String>>();
						Map<String, String> currDoc = new HashMap<String, String>();
						
						String lastDocument = null;
						
						System.out.println("Reading event...");
						while(it2.hasNext()) {
							Statement stmt1 = it2.next();
							
							Resource  subject1 = stmt1.getSubject();     	// get the subject
						    Property  predicate1 = stmt1.getPredicate();   	// get the predicate
						    RDFNode   object1 = stmt1.getObject();      	// get the object
						    
						    String subjectURI = subject1.toString();
						    
						    if (subject1.isURIResource()) {
						    	String property = predicate1.toString();
						    	
							    if (subjectURI.contains("events")) {
							    	if (property.equals("http://purl.org/dc/elements/1.1/title")) {
										data.put("dcterms:Title", object1.toString());
										caseFile.put("dcterms:Description", object1.toString());
										//System.out.println("title" + object1.toString());
									} else if (property.equals("http://purl.org/dc/elements/1.1/date")) {
										data.put("StartDate", object1.toString());
										//System.out.println("date " + object1.toString());
									}
							    } else if (subjectURI.contains("documents")) {
							    	String[] uriparts = subjectURI.split("/");
							    	String currentDoc = uriparts[uriparts.length-1];
							    	
							    	if (!currentDoc.equals(lastDocument)) {
							    		if (lastDocument!=null) {
							    			if (currDoc.get("doctype").startsWith("Touring")) {
							    				// data.put("tookPlaceAt", currDoc.get("dcterms:Description"));
							    				// TODO: Parse description to extract location
							    			} else if (currDoc.get("doctype").startsWith("Introduction") || currDoc.get("doctype").startsWith("Activity")) {
							    				data.put("dcterms:Description", currDoc.get("dcterms:Description"));
							    			} else if (currDoc.get("doctype").startsWith("List of works")) {
							    				caseFile.put("isWorks", "?????"); // TODO: link properly Case-File and Text Document
							    				currDoc.put("className", "Text");
							    				documents.put(lastDocument, currDoc);
							    			} else if (currDoc.get("doctype").startsWith("Documentation")) {
							    				// TODO: pending
							    			} else if (currDoc.get("doctype").startsWith("Collection")) {
							    				// TODO: pending
							    			} else if (currDoc.get("doctype").startsWith("Event")) {
							    				currDoc.put("className", "SpecificActivity");
							    				documents.put(lastDocument, currDoc);
							    			}
							    		}
							    		currDoc = new HashMap<String, String>();
							    		lastDocument = currentDoc;
							    	}
							    	
							    	if (property.equals("http://purl.org/dc/elements/1.1/title")) {
							    		currDoc.put("dcterms:Title", object1.toString());
							    		//System.out.println("doc-title " + object1.toString());
							    	} else if (property.equals("http://purl.org/dc/elements/1.1/description")) {
							    		String desc = object1.toString().replaceAll(" class=\"spip\"", "").replaceAll(" class=\"spip_out\"","");
							    		currDoc.put("dcterms:Description", desc);
							    		//System.out.println("doc-description " + desc);
							    	} else if (property.equals("http://www.fundaciotapies.org/terms/0.1/doctype")) {
							    		currDoc.put("doctype", object1.toString());
							    	}
							    }
						    }
						}
						
						if (lastDocument!=null) {
							if (currDoc.get("doctype").startsWith("Touring")) {
			    				// data.put("tookPlaceAt", currDoc.get("dcterms:Description"));
			    				// TODO: Parse description to extract location
			    			} else if (currDoc.get("doctype").startsWith("Introduction") || currDoc.get("doctype").startsWith("Activity")) {
			    				data.put("dcterms:Description", currDoc.get("dcterms:Description"));
			    			} else if ("List of works".equals(currDoc.get("doctype"))) {
			    				caseFile.put("isWorks", "?????"); // TODO: reference properly Case-File and Text Document
			    				currDoc.put("className", "Text");
			    				documents.put(lastDocument, currDoc);
			    			} else if ("Documentation".equals(currDoc.get("doctype"))) {
			    				// TODO: pending
			    			} else if ("Collection".equals("doctype")) {
			    				// TODO: pending
			    			} else if ("Event".equals("doctype")) {
			    				currDoc.put("className", "SpecificActivity");
			    				documents.put(lastDocument, currDoc);
			    			}
						}
						
						System.out.println("Uploading event...");
						uploadObject(data);
						uploadObject(caseFile);
						System.out.println("Uploaded.");
						   
						System.out.println("Uploading event documents...");
						Iterator<Map.Entry<String, Map<String, String>>> it = documents.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String, Map<String, String>> entry = it.next();
							uploadObject(new CustomMap(entry.getValue()));
						}
						System.out.println("Uploaded.");
						
			    	} catch (Exception e) {
			    		System.out.println("ERROR with event " + object.toString());
			    		e.printStackTrace();
			    	}
			    }
			}
		} catch (Exception e) {
			System.out.println("Error " + e);
			e.printStackTrace();
		}
	}
	
	public static void migrarPublications() {
		try {
			// migrar publications
			if (DOWNLOAD_DATA) {
				System.out.println("Downloading publications rdf data...");
				URL publicationsUrl = new URL("http://www.fundaciotapies.org/site/spip.php?page=all-publications-rdf");
				BufferedReader in = new BufferedReader(new InputStreamReader(publicationsUrl.openStream()));
				
				File publicationsFile = new File("publications.rdf");
				FileWriter publicationsFileWriter = new FileWriter(publicationsFile);
		
				char[] input = new char[255];
				int b = 0;
				while ((b = in.read(input)) > 0) publicationsFileWriter.write(input, 0, b);
				in.close();
				
				publicationsFileWriter.flush();
				publicationsFileWriter.close();
			}
			
			Model model = ModelFactory.createDefaultModel();
			model.read("file:publications.rdf");
			
			//	per a cada publication...
			StmtIterator it1 = model.listStatements();
			while(it1.hasNext()) {
				Statement stmt = it1.next();
			    //Resource  subject   = stmt.getSubject();     			// get the subject
			    Property  predicate = stmt.getPredicate();   			// get the predicate
			    RDFNode   object    = stmt.getObject();      			// get the object
			    
			    // obtenir les seves dades completes...
			    if (predicate.toString().equals("http://www.fundaciotapies.org/terms/0.1/rdfuri")) {
			    	try {
				    	String url = object.toString();
				    	URL publicationUrl = new URL(url);
				    	
				    	BufferedReader in = new BufferedReader(new InputStreamReader(publicationUrl.openStream()));
						
						File publicationFile = new File("currentPublication.rdf");
						if (publicationFile.exists()) {
							publicationFile.delete();
							publicationFile = new File("currentPublication.rdf");
						}
						FileWriter publicationFileWriter = new FileWriter(publicationFile);
				
						char[] input = new char[255];
						int b = 0;
						while ((b = in.read(input)) > 0) publicationFileWriter.write(input, 0, b);
						in.close();
						
						publicationFileWriter.flush();
						publicationFileWriter.close();
						
						Model model1 = ModelFactory.createDefaultModel();
						model1.read("file:currentPublication.rdf");
						
						StmtIterator it2 = model1.listStatements();
						CustomMap data = new CustomMap();
						data.put("className", "Publication");
						
						String fatacId = url.substring(url.length()-4);
						if (fatacId.startsWith("=")) fatacId = fatacId.substring(1);
						data.put("FatacId", fatacId );
						//System.out.println("FatacId " + fatacId);
						
						System.out.println("Reading publication...");
						
						String editorLastId = null;
						Map<String, String> currEditor = null;
						CustomList editorsList = new CustomList();
						
						String distrLastId = null;
						Map<String, String> currDistr = null;
						CustomList distributorsList = new CustomList();

						while(it2.hasNext()) {
							Statement stmt1 = it2.next();
							System.out.println(stmt1);
							
							Resource subject1 = stmt1.getSubject();
						    Property predicate1 = stmt1.getPredicate();   	// get the predicate
						    RDFNode object1   = stmt1.getObject();      	// get the object
						    
						    String subjectURI = subject1.toString();
						    String predicateURI = predicate1.toString();
						    String value = object1.toString();
						    
						    if (subjectURI.contains("publications") || value.startsWith("ISBN")) {
						    	if (predicateURI.equals("http://purl.org/dc/elements/1.1/title")) {
									data.put("dcterms:Title", value);
									//System.out.println("title " + value);
								} else if (predicateURI.equals("http://purl.org/dc/elements/1.1/description")) {
									data.put("dcterms:Description", value);
									//System.out.println("description " + value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/cover")) {
									data.put("CoverPublications", value);
									//System.out.println("cover " + value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/pvp")) {
									data.put("pvp", value);
									//System.out.println("pvp " + value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/pvp_friends")) {
									data.put("pvp_friends ", value);
									//System.out.println("cover " + value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/internal_comments")) {
									data.put("internal_comments", value);
									//System.out.println("internal_comments " + value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/disponible")) {
									data.put("disponible", value);
									//System.out.println("disponible " + value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/internal_ref")) {
									data.put("internal_ref", value);
									//System.out.println("internal_ref " + value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/special_offer")) {
									data.put("special_offer", value);
									//System.out.println("special_offer " + value);
								} else if (value.startsWith("ISBN")) {
									data.put("ISBN", value.substring(5));
									//System.out.println(value);
								}
						    	
						    	// TODO: migrate publication contained objects
						    } else if (subjectURI.contains("editors")) {
						    	if (!subjectURI.equals(editorLastId)) {
						    		if (editorLastId!=null)	editorsList.add(currEditor);
						    		editorLastId = subjectURI;
						    		currEditor = new HashMap<String, String>();
						    		currEditor.put("className", "foaf:Organisation");
						    	}
						    	
						    	if (predicateURI.equals("http://purl.org/dc/elements/1.1/date")) {
						    		// TODO: Ontology - Add date datatype to Editor role
						    	} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/id")) {
						    		URL editorUrl = new URL("http://www.fundaciotapies.org/site/spip.php?page=xml-editor&id="+value);
						    		BufferedReader in1 = new BufferedReader(new InputStreamReader(editorUrl.openStream()));
									
									File editorFile = new File("currentEditor.rdf");
									if (editorFile.exists()) {
										editorFile.delete();
										editorFile = new File("currentEditor.rdf");
									}
									FileWriter editorFileWriter = new FileWriter(editorFile);
							
									char[] input1 = new char[255];
									int b1 = 0;
									while ((b1 = in1.read(input1)) > 0) editorFileWriter.write(input1, 0, b1);
									in1.close();
									
									editorFileWriter.flush();
									editorFileWriter.close();
									
									Model model2 = ModelFactory.createDefaultModel();
									try {
										model2.read("file:currentEditor.rdf");
									} catch (Exception e) {
										errors.put("Editor", value);
									}
									StmtIterator it3 = model2.listStatements();
									
									while(it3.hasNext()) {
										Statement stmt2 = it3.next();
										
										Property predicate11 = stmt2.getPredicate();   	// get the predicate
									    RDFNode object11   = stmt2.getObject();      	// get the object
									    
									    String predicateURI1 = predicate11.toString();
									    String value1 = object11.toString();
									    
									    if (predicateURI1.equals("http://purl.org/dc/elements/1.1/name")) {
									    	currEditor.put("foaf:firstName", value1);
									    } else if (predicateURI1.equals("http://purl.org/dc/elements/1.1/url")) {
									    	currEditor.put("foaf:Homepage", value1);
									    } else if (predicateURI1.equals("http://purl.org/dc/elements/1.1/location")) {
									    	// TODO: create or use location for relation!
									    } else if (predicateURI1.equals("http://purl.org/dc/elements/1.1/id")) {
									    	currEditor.put("id", value1);
									    }
									}
						    	}
						    } else if (subjectURI.contains("separata")) {
						    	if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/id")) {
						    		
								}
						    } else if (subjectURI.contains("distributors")) {
						    	if (!subjectURI.equals(distrLastId)) {
						    		if (distrLastId!=null)	editorsList.add(currDistr);
						    		distrLastId = subjectURI;
						    		currDistr = new HashMap<String, String>();
						    		currDistr.put("className", "foaf:Organisation");
						    	}
						    	
						    	if (predicateURI.equals("http://purl.org/dc/elements/1.1/location")) {
						    		
						    	} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/id")) {
						    		URL distributorUrl = new URL("http://www.fundaciotapies.org/site/spip.php?page=xml-distributor&id="+value);
						    		BufferedReader in1 = new BufferedReader(new InputStreamReader(distributorUrl.openStream()));
									
									File distributorFile = new File("currentDistributor.rdf");
									if (distributorFile.exists()) {
										distributorFile.delete();
										distributorFile = new File("currentDistributor.rdf");
									}
									
									FileWriter distributorFileWriter = new FileWriter(distributorFile);
							
									char[] input1 = new char[255];
									int b1 = 0;
									while ((b1 = in1.read(input1)) > 0) distributorFileWriter.write(input1, 0, b1);
									in1.close();
									
									distributorFileWriter.flush();
									distributorFileWriter.close();
									
									Model model2 = ModelFactory.createDefaultModel();
									try {
										model2.read("file:currentDistributor.rdf");
									} catch (Exception e) {
										errors.put("Distributor", value);
									}
									StmtIterator it3 = model2.listStatements();
									
									while(it3.hasNext()) {
										Statement stmt2 = it3.next();
										
										Property predicate11 = stmt2.getPredicate();   	// get the predicate
									    RDFNode object11   = stmt2.getObject();      	// get the object
									    
									    String predicateURI1 = predicate11.toString();
									    String value1 = object11.toString();
									    
									    if (predicateURI1.equals("http://purl.org/dc/elements/1.1/name")) {
									    	currDistr.put("foaf:firstName", value1);
									    } else if (predicateURI1.equals("http://purl.org/dc/elements/1.1/url")) {
									    	currDistr.put("foaf:Homepage", value1);
									    } else if (predicateURI1.equals("http://purl.org/dc/elements/1.1/location")) {
									    	// TODO: create or reuse location
									    } else if (predicateURI1.equals("http://purl.org/dc/elements/1.1/id")) {
									    	currDistr.put("id", value1);
									    }
									}
						    	}
						    }
						}
					
						System.out.println("Uploading publication...");
						uploadObject(data);
						System.out.println("Uploaded. ");
						
						System.out.println("Uploading publication editors...");
						for(Map<String, String> e : editorsList) uploadObject(new CustomMap(e));
						System.out.println("Uploaded.");
						
						System.out.println("Uploading publication distributors...");
						for(Map<String, String> d : distributorsList) uploadObject(new CustomMap(d));
						System.out.println("Uploaded.");
						
						// TODO: migrate separata's
						
						// TODO: create relations between Editors and Publications

			    	} catch (Exception e) {
			    		System.out.println("ERROR with publication " + object.toString());
			    		e.printStackTrace();
			    	}
			    }
			}
		} catch (Exception e) {
			System.out.println("Error " + e);
			e.printStackTrace();
		}
	}
	
	public static String getRealId(String c, String fatacId) {
		try {
			// Send data
		    URL url = new URL("http://localhost:8080/ArtsCombinatoriesRest/getRealId?class="+c+"&id="+fatacId);
		    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		    conn.setRequestMethod("GET");
	
		    // Get the response
		    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		    String str;
		    StringBuffer sb = new StringBuffer();
		    while ((str = rd.readLine()) != null) {
		    	sb.append(str);
		    	sb.append("\n");
		    }
	
		    rd.close();
		    
			return sb.toString();
		} catch (Exception e) {
			return null;
		}
	}
	
	private static String getRoleName(String roleId) {
		try {
			if (DOWNLOAD_DATA) {
				URL rolesUrl = new URL("http://www.fundaciotapies.org/site/spip.php?page=all-roles-rdf");
				BufferedReader in = new BufferedReader(new InputStreamReader(rolesUrl.openStream()));
				
				File rolesFile = new File("roles.rdf");
				FileWriter rolesFileWriter = new FileWriter(rolesFile);
		
				char[] input = new char[255];
				int b = 0;
				while ((b = in.read(input)) > 0) rolesFileWriter.write(input, 0, b);
				in.close();
				
				rolesFileWriter.flush();
				rolesFileWriter.close();
			}
			
			File rolesFile = new File("roles.rdf");
			
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(rolesFile);
			doc.getDocumentElement().normalize();
			NodeList nodeLst = doc.getElementsByTagName("ft:role");
			
			for (int s = 0; s < nodeLst.getLength(); s++) {

				Node fstNode = nodeLst.item(s);
			    
				if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
			  
					Element fstElmnt = (Element) fstNode;
				    NodeList fstNmElmntLst = fstElmnt.getElementsByTagName("dc:title");
				    Element fstNmElmnt = (Element) fstNmElmntLst.item(0);
				    NodeList fstNm = fstNmElmnt.getChildNodes();
				    String roleName = ((Node) fstNm.item(0)).getNodeValue();
				    
				    NodeList lstNmElmntLst = fstElmnt.getElementsByTagName("ft:id");
				    Element lstNmElmnt = (Element) lstNmElmntLst.item(0);
				    NodeList lstNm = lstNmElmnt.getChildNodes();
				    String roleId2 = ((Node) lstNm.item(0)).getNodeValue();
				    
				    if (roleId2.equals(roleId)) return roleName;
			    }
			}
				
		} catch (Exception e) {
			 e.printStackTrace();
		}
		
		return null;
	}
	
	private static void migrarRelation(String type1, String id1, String type2, String id2, String roleId) throws Exception {
		if (type1.equals("ft:event_id_1") && type2.equals("ft:event_id_2")) {
			String sid1 = getRealId("Event", id1);
			String sid2 = getRealId("Event", id2);
			String roleName = getRoleName(roleId);
			
			if ("Serie".equals(roleName)) {
				CustomMap event1 = new CustomMap();
				event1.put("continuesTo", sid2);
				updateObject(sid1, event1);
				
				CustomMap event2 = new CustomMap();
				event1.put("comesFrom", id1);
				updateObject(sid2, event2);
			}
			
		} else if (type1.equals("ft:participant_id") && type2.equals("ft:event_id")) {
			String sid1 = getRealId("Person", id1);
			String sid2 = getRealId("Event", id2);
			String roleName = getRoleName(roleId);
			
			if (roleName.equals("Workshop tutor")) roleName = "Workshop_tutor";
			if (roleName.equals("chair")) roleName = "Chair";
			if (roleName.equals("Director of the project")) roleName = "Director";
			if (roleName.equals("Poet and Novelist")) roleName = "Poet";
			if (roleName.equals("Director of the project in Barcelona")) roleName = "Director";
			
			CustomMap role = new CustomMap();
			role.put("className", roleName);
			role.put("appliesOn", sid2);
			role.put("FatacId", "r"+roleCount);
			uploadObject(role);
			
			String sid3 = getRealId(roleName, "r"+roleCount);
			roleCount++;
			
			CustomMap person = new CustomMap();
			person.put("performsRole", sid3);
			updateObject(sid1, person);
			
		} else if (type1.equals("ft:publication_id") && type2.equals("ft:event_id")) {
			String sid1 = getRealId("Publication", id1);
			String sid2 = getRealId("Event", id2);
			String roleName = getRoleName(roleId);
			
			// TODO: activity_publication -- necessito saber cada rol quina relació és: CATALOGUE, ARTIST BOOK, BOOK <---> isComposedOf, 
			
		} else if (type1.equals("ft:publication_id") && type2.equals("ft:participant_id")) {
			String sid1 = getRealId("Person", id2);
			String sid2 = getRealId("Publication", id1);
			String roleName = getRoleName(roleId);
			
			CustomMap role = new CustomMap();
			role.put("className", roleName);
			role.put("appliesOn", sid2);
			role.put("FatacId", "r"+roleCount);
			uploadObject(role);
			
			String sid3 = getRealId(roleName, "r"+roleCount);
			roleCount++;
			
			CustomMap person = new CustomMap();
			person.put("performsRole", sid3);
			updateObject(sid1, person);
			
		} else if (type1.equals("ft:participant_id_1") && type2.equals("ft:participant_id_2")) {
			String sid1 = getRealId("Person", id1);
			String sid2 = getRealId("Person", id2);
			String roleName = getRoleName(roleId);
			
			CustomMap role = new CustomMap();
			role.put("className", roleName);
			uploadObject(role);
			
			String sid3 = getRealId(roleName, "r"+roleCount);
			roleCount++;
			
			CustomMap person = new CustomMap();
			person.put("performsRole", sid3);
			updateObject(sid1, person);
		} else if (type1.equals("ft:separata_id")) {
			
		}
	}
	
	private static void migrarRelations() {
		try {
			// migrar relations
			if (DOWNLOAD_DATA) {
				System.out.println("Downloading relations rdf data...");
				URL relationsUrl = new URL("http://www.fundaciotapies.org/site/spip.php?page=all-relations-rdf");
				BufferedReader in = new BufferedReader(new InputStreamReader(relationsUrl.openStream()));
				
				File relationsFile = new File("relations.rdf");
				FileWriter relationsFileWriter = new FileWriter(relationsFile);
			
				char[] input = new char[255];
				int b = 0;
				while ((b = in.read(input)) > 0) relationsFileWriter.write(input, 0, b);
				in.close();
				
				relationsFileWriter.flush();
				relationsFileWriter.close();
			}
			
			File file = new File("relations.rdf");
			DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
			DocumentBuilder db = dbf.newDocumentBuilder();
			Document doc = db.parse(file);
			doc.getDocumentElement().normalize();
			NodeList nodeLst = doc.getElementsByTagName("ft:rel");
			
			for (int s = 0; s < nodeLst.getLength(); s++) {

				Node fstNode = nodeLst.item(s);
			    
				if (fstNode.getNodeType() == Node.ELEMENT_NODE) {
			  
					NodeList l = fstNode.getChildNodes();
					java.util.List<String> params = new ArrayList<String>();
					for(int i = 0;i<l.getLength();i++) {
						Node n = l.item(i);
						if (!n.getNodeName().equals("#text")) {
							params.add(n.getNodeName());
							params.add(n.getFirstChild().getNodeValue());
						}
					}
					
					//System.out.println(params);
					migrarRelation(params.get(0), params.get(1), params.get(2), params.get(3), params.get(5));
			    }
			}
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
	}
	
	private static void migrarFileMaker1() {
		try {
			CsvReader r = new CsvReader(new FileReader(new File("./fm/CF-fitxer-videos.csv")));
			r.readHeaders();
			
			while (r.readRecord()) {
				CustomMap caseFile = new CustomMap();
				caseFile.put("className", "Case-File");
				
				CustomMap media = new CustomMap();
				media.put("className", "Video");
				
				CustomMap event = new CustomMap();
				event.put("className", "Event");
				
				List<String> agentProductor = new ArrayList<String>();
				List<String> agentRealitzador = new ArrayList<String>();
				
				CustomMap lang = new CustomMap();
				lang.put("className", "Language");
				
				if (r.get("any")!=null && !"".equals(r.get("any"))) {
					media.put("StartDateLeng", r.get("any"));
				} 
				
				if (r.get("duració")!=null && !"".equals(r.get("duració"))) {
					media.put("DurationTime", r.get("duració"));
				} 
				
				if (r.get("exposició")!=null && !"".equals(r.get("exposició"))) {
					event.put("dcterms:Title", r.get("exposició"));
				} 
				
				if (r.get("núm. inventari")!=null && !"".equals(r.get("núm. inventari"))) {
					media.put("InventoryNumber", r.get("núm. inventari"));
				} 
				
				if (r.get("observacions")!=null && !"".equals(r.get("observacions"))) {
					caseFile.put("dcterms:Description", r.get("observacions"));
					media.put("dcterms:Description", r.get("observacions"));
					event.put("dcterms:Description", r.get("observacions"));
					//agent.put("dcterms:Description", r.get("observacions"));
				} 
				
				if (r.get("productor")!=null && !"".equals(r.get("productor"))) {
					String prodNames = r.get("productor").trim();
					String[] pl = prodNames.split(",");
					
					for(String p : pl) {
						List<String> l = find("foaf:firstName", p, "foaf:Organisation");
						l.addAll(find("foaf:firstName", p, "foaf:Person"));
						String uri = null;
						
						if (l.size()==0) {
							CustomMap agent = new CustomMap();
							agent.put("className", "foaf:Organisation"); // TODO: distingish between organisations and persons
							agent.put("foaf:firstName", p);
							uri = uploadObject(agent);
							
						} else {
							uri = l.get(0); 
						}
						
						agentProductor.add(uri);
					}
				} 
				
				if (r.get("Realització")!=null && !"".equals(r.get("Realització"))) {
					String realNames = r.get("Realització").trim();
					String[] pl = realNames.split(",");
					for(String p : pl) {
						List<String> l = find("foaf:firstName", p, "foaf:Organisation");
						l.addAll(find("foaf:firstName", p, "foaf:Person"));
						String uri = null;
						
						if (l.size()==0) {
							CustomMap agent = new CustomMap();
							agent.put("className", "foaf:Organisation"); // TODO: distingish between organisations and persons
							agent.put("foaf:firstName", p);
							uri = uploadObject(agent);
							
						} else {
							uri = l.get(0); 
						}
						
						agentRealitzador.add(uri);
					}
				} 
				
				if (r.get("so")!=null && !"".equals(r.get("so"))) {
					String v = r.get("so");
					if ("sense so".equals(v) || "sin sonido".equals(v)) {
						media.put("Mute", "true");
					} 
					if (v.contains("català") || v.contains("catalán") ) {
						media.put("hasLanguage", "Catalan"); 
					} 
					if (v.contains("español") || v.contains("castellano") || v.contains("castellà")) {
						media.put("hasLanguage", "Spanish");
					} 
					if (v.contains("english") || v.contains("anglès") || v.contains("inglés")) {
						media.put("hasLanguage", "English"); 
					}
				} 
				
				if (r.get("suport")!=null && !"".equals(r.get("suport"))) {
					media.put("OriginalSource", r.get("suport"));
				} 
				
				if (r.get("tipus")!=null && !"".equals(r.get("tipus"))) {
					String v = r.get("tipus");
					if (v.contains("color") || v.contains("Color")) media.put("ColorMode","Color");
					if (v.contains("Blanc i negre") || v.contains("b/n") || v.contains("blanco-negro")) media.put("ColorMode", "B/N");
				} 
				
				if (r.get("títol")!=null && !"".equals(r.get("títol"))) {
					media.put("dcterms:Title", r.get("títol"));
					event.put("dcterms:Title", r.get("títol"));					
				}
				
				uploadObject(media);
				String eventUri = uploadObject(event);
				
				caseFile.put("references", eventUri);
				uploadObject(caseFile);
				
				// TODO: link media to CF, Event
				
				for(String id : agentProductor) {
					CustomMap role = new CustomMap();
					role.put("className", "Producer");
					role.put("appliesOn", eventUri);
					String roleUri = uploadObject(role);
					
					CustomMap prod = new CustomMap();
					Object v = getObject(id).get("performsRole");
					if (v instanceof String) {
						prod.put("performsRole", (String)v);	
					} else { prod.put("performsRole", v); }
					
					prod.put("performsRole", roleUri);
					updateObject(id, prod);
				}
				
				for(String id : agentRealitzador) {
					CustomMap role = new CustomMap();
					role.put("className", "Author");
					role.put("appliesOn", eventUri);
					String roleUri = uploadObject(role);
					
					CustomMap prod = new CustomMap();
					Object v = getObject(id).get("performsRole");
					if (v instanceof String) {
						prod.put("performsRole", (String)v);	
					} else { prod.put("performsRole", v); }
					
					prod.put("performsRole", roleUri);
					updateObject(id, prod);
				}
			}
			
			r.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void migrarFileMaker2() {
		try {
			CsvReader r = new CsvReader(new FileReader(new File("./fm/CF_conferencies.csv")));
			r.readHeaders();
			
			SimpleDateFormat sdf1 = new SimpleDateFormat("ddMMyyyy");
			SimpleDateFormat sdf2 = new SimpleDateFormat("dd/MM/yyyy");
			
			while (r.readRecord()) {
				CustomMap caseFile = new CustomMap();
				caseFile.put("className", "Case-File");
				
				CustomMap lang = new CustomMap();
				lang.put("className", "Language");
				
				CustomMap event = new CustomMap();
				event.put("className", "Event");
				
				CustomMap media = new CustomMap();
				media.put("className", "Video");
				
				List<String> agentAutor = new ArrayList<String>();
				List<String> agentProductor = new ArrayList<String>();
				
				if (r.get("any")!=null && !"".equals(r.get("any"))) {
					Date d = sdf1.parse(r.get("any").trim());
					event.put("StartDate", sdf2.format(d));
				}
				
				if (r.get("autor")!=null && !"".equals(r.get("autor"))) {
					String prodNames = r.get("autor").trim();
					String[] pl = prodNames.split(",");
					
					for(String p : pl) {
						List<String> l = find("foaf:firstName", p, "foaf:Organisation");
						l.addAll(find("foaf:firstName", p, "foaf:Person"));
						String uri = null;
						
						if (l.size()==0) {
							CustomMap agent = new CustomMap();
							agent.put("className", "foaf:Organisation"); // TODO: distingish between organisations and persons
							agent.put("foaf:firstName", p);
							uri = uploadObject(agent);
							
						} else {
							uri = l.get(0); 
						}
						
						agentAutor.add(uri);
					}
				}
				
				if (r.get("contingut")!=null && !"".equals(r.get("contingut"))) {
					caseFile.put("dcterms:Description", r.get("contingut"));
					media.put("dcterms:Description", r.get("contingut"));
					event.put("dcterms:Description", r.get("contingut"));
				} 
				
				if (r.get("durada")!=null && !"".equals(r.get("durada"))) {
					// TODO: pending
				} 
				
				if (r.get("exposició")!=null && !"".equals(r.get("exposició"))) {
					event.put("dcterms:Title", r.get("exposició"));
				} 
				
				if (r.get("format")!=null && !"".equals(r.get("format"))) {
					// TODO: pending
				} 
				
				if (r.get("Idioma")!=null && !"".equals(r.get("Idioma"))) {
					String v = r.get("Idioma");
					if (v.contains("català") || v.contains("catalán") ) {
						media.put("hasLanguage", "Catalan"); 
					} 
					if (v.contains("español") || v.contains("castellano") || v.contains("castellà")) {
						media.put("hasLanguage", "Spanish");
					} 
					if (v.contains("english") || v.contains("anglès") || v.contains("inglés")) {
						media.put("hasLanguage", "English"); 
					}
					if (v.contains("àrab") || v.contains("árabe") || v.contains("arabic")) {
						media.put("hasLanguage", "Arabic"); 
					}
					if (v.contains("francès") || v.contains("francés") || v.contains("french")) {
						media.put("hasLanguage", "French"); 
					}
					if (v.contains("alemany") || v.contains("german") || v.contains("alemán")) {
						media.put("hasLanguage", "German"); 
					}
					if (v.contains("italià") || v.contains("italian") || v.contains("italiano")) {
						media.put("hasLanguage", "Italian"); 
					}
				} 
				
				if (r.get("notes")!=null && !"".equals(r.get("notes"))) {
					// TODO: pending
				} 
				
				if (r.get("núm. inv.")!=null && !"".equals(r.get("núm. inv."))) {
					media.put("InventoryNumber", r.get("núm. inv."));
				}
				
				if (r.get("productor")!=null && !"".equals(r.get("productor"))) {
					String prodNames = r.get("productor").trim();
					String[] pl = prodNames.split(",");
					
					for(String p : pl) {
						List<String> l = find("foaf:firstName", p, "foaf:Organisation");
						l.addAll(find("foaf:firstName", p, "foaf:Person"));
						String uri = null;
						
						if (l.size()==0) {
							CustomMap agent = new CustomMap();
							agent.put("className", "foaf:Organisation"); // TODO: distingish between organisations and persons
							agent.put("foaf:firstName", p);
							uri = uploadObject(agent);
							
						} else {
							uri = l.get(0); 
						}
						
						agentProductor.add(uri);
					}
				} 
				
				if (r.get("títol")!=null && !"".equals(r.get("títol"))) {
					media.put("dcterms:Title", r.get("títol"));
					event.put("dcterms:Title", r.get("títol"));
				}
				
				uploadObject(media);
				String eventUri = uploadObject(event);
				
				caseFile.put("references", eventUri);
				uploadObject(caseFile);
				
				for(String id : agentProductor) {
					CustomMap role = new CustomMap();
					role.put("className", "Producer");
					role.put("appliesOn", eventUri);
					String roleUri = uploadObject(role);
					
					CustomMap prod = new CustomMap();
					Object v = getObject(id).get("performsRole");
					if (v instanceof String) {
						prod.put("performsRole", (String)v);	
					} else { prod.put("performsRole", v); }
					
					prod.put("performsRole", roleUri);
					updateObject(id, prod);
				}
				
				for(String id : agentAutor) {
					CustomMap role = new CustomMap();
					role.put("className", "Author");
					role.put("appliesOn", eventUri);
					String roleUri = uploadObject(role);
					
					CustomMap prod = new CustomMap();
					Object v = getObject(id).get("performsRole");
					if (v instanceof String) {
						prod.put("performsRole", (String)v);	
					} else { prod.put("performsRole", v); }
					
					prod.put("performsRole", roleUri);
					updateObject(id, prod);
				}
			}
			
			r.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void migrarFileMaker3() {
		try {
			CsvReader r = new CsvReader(new FileReader(new File("./fm/SF-MASER-COLLECIO.csv")));
			r.readHeaders();
			
			while (r.readRecord()) {
				
				if (r.get("Núm. fotog.")!=null && !"".equals(r.get("Núm. fotog."))) {
					
				}
				if (r.get("Núm. ekta.")!=null && !"".equals(r.get("Núm. ekta."))) {
					
				}
				if (r.get("Nombre d'exemplars")!=null && !"".equals(r.get("Nombre d'exemplars"))) {
					
				}
				if (r.get("Any")!=null && !"".equals(r.get("Any"))) {
					
				}
				if (r.get("Autor i data de la fitxa")!=null && !"".equals(r.get("Autor i data de la fitxa"))) {
					
				}
				if (r.get("Bibliografia")!=null && !"".equals(r.get("Bibliografia"))) {
					
				}
				if (r.get("Descripció de l'objecte")!=null && !"".equals(r.get("Descripció de l'objecte"))) {
					
				}
				if (r.get("Estat de conservació")!=null && !"".equals(r.get("Estat de conservació"))) {
					
				}
				if (r.get("Exposicions")!=null && !"".equals(r.get("Exposicions"))) {
					
				}
				if (r.get("Lloc de procedència")!=null && !"".equals(r.get("Lloc de procedència"))) {
					
				}
				if (r.get("Localització")!=null && !"".equals(r.get("Localització"))) {
					
				}
				if (r.get("Mides")!=null && !"".equals(r.get("Mides"))) {
					
				}
				if (r.get("obra gràfica =")!=null && !"".equals(r.get("obra gràfica ="))) {
					
				}
				if (r.get("obra original =")!=null && !"".equals(r.get("obra original ="))) {
					
				}
				if (r.get("préstec a :")!=null && !"".equals(r.get("préstec a :"))) {
					
				}
				if (r.get("Reproduccions")!=null && !"".equals(r.get("Reproduccions"))) {
					
				}
				if (r.get("Restauracions")!=null && !"".equals(r.get("Restauracions"))) {
					
				}
				if (r.get("técnica")!=null && !"".equals(r.get("técnica"))) {
					
				}
				if (r.get("Tècnica")!=null && !"".equals(r.get("Tècnica"))) {
					
				}
				if (r.get("Títol")!=null && !"".equals(r.get("Títol"))) {
					
				}
				if (r.get("Valoració Econòmica en €")!=null && !"".equals(r.get("Valoració Econòmica en €"))) {
					
				}
				if (r.get("vegap:")!=null && !"".equals(r.get("vegap:"))) {
					
				}
				if (r.get("Causa de baixa i data")!=null && !"".equals(r.get("Causa de baixa i data"))) {
					
				}
				if (r.get("Objectes en relació indicacant n. registre")!=null && !"".equals(r.get("Objectes en relació indicacant n. registre"))) {
					
				}
				if (r.get("Edició")!=null && !"".equals(r.get("Edició"))) {
					
				}
				if (r.get("Editor")!=null && !"".equals(r.get("Editor"))) {
					
				}
				if (r.get("Exemplar")!=null && !"".equals(r.get("Exemplar"))) {
					
				}
				if (r.get("Mides planxa")!=null && !"".equals(r.get("Mides planxa"))) {
					
				}
				if (r.get("Marca paper")!=null && !"".equals(r.get("Marca paper"))) {
					
				}
				
				if (r.get("Tiratge")!=null && !"".equals(r.get("Tiratge"))) {
					
				}
				if (r.get("Núm. planxes")!=null && !"".equals(r.get("Núm. planxes"))) {
					
				}
				if (r.get("Filigrana")!=null && !"".equals(r.get("Filigrana"))) {
					
				}
				if (r.get("trad. castellano")!=null && !"".equals(r.get("trad. castellano"))) {
					
				}
				if (r.get("trad. english")!=null && !"".equals(r.get("trad. english"))) {
					
				}
				if (r.get("trad. français")!=null && !"".equals(r.get("trad. français"))) {
					
				}
				if (r.get("trad.castellano técnica:")!=null && !"".equals(r.get("trad.castellano técnica:"))) {
					
				}
				if (r.get("trad.english technique:")!=null && !"".equals(r.get("trad.english technique:"))) {
					
				}
				if (r.get("trad.français technique:")!=null && !"".equals(r.get("trad.français technique:"))) {
					
				}
				
				
			}
			
			r.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		DOWNLOAD_DATA = true;
		//MIGRAR = true;
		
		// ----- Migració de SPIP
		//migrarPersons(); // DONE.
		//migrarEvents();
		//migrarPublications();
		//migrarRelations();
		
		// ----- Migració de File-Maker
		migrarFileMaker1();
		migrarFileMaker2();
		migrarFileMaker3();
		
		// TODO: migrar arxius digitalitzats
	}

}
