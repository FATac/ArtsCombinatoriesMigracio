import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.csvreader.CsvReader;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.reflect.TypeToken;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.ibm.icu.util.GregorianCalendar;

/*
 * 

#

# SCRIPT SPARQL	QUE CAL EXECUTAR ABANS DE MIGRAR
 
 PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
 PREFIX ac: <http://www.fundaciotapies.org/ontologies/2011/4/ac.owl#>

# Idiomes

INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Language/Catalan> rdf:type ac:Language }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Language/Italian> rdf:type ac:Language }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Language/Spanish> rdf:type ac:Language }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Language/English> rdf:type ac:Language }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Language/French> rdf:type ac:Language }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Language/Arabic> rdf:type ac:Language }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Language/German> rdf:type ac:Language }


# KingArtWork individuals

INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/KindArtWork/Dance> rdf:type ac:KindArtWork }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/KindArtWork/Installation> rdf:type ac:KindArtWork }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/KindArtWork/Performance> rdf:type ac:KindArtWork }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/KindArtWork/Photo> rdf:type ac:KindArtWork }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/KindArtWork/Picture> rdf:type ac:KindArtWork }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/KindArtWork/Requiem> rdf:type ac:KindArtWork }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/KindArtWork/Sculpture> rdf:type ac:KindArtWork }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/KindArtWork/Sonata> rdf:type ac:KindArtWork }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/KindArtWork/Videoart> rdf:type ac:KindArtWork }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/KindArtWork/Graphic> rdf:type ac:KindArtWork }

# Material individuals

INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Material/Gold> rdf:type ac:Material }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Material/Iron> rdf:type ac:Material }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Material/Marble> rdf:type ac:Material }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Material/Pastle> rdf:type ac:Material }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Material/Stone> rdf:type ac:Material }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Material/Tempera> rdf:type ac:Material }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Material/Titanium> rdf:type ac:Material }
INSERT INTO GRAPH <http://localhost:8890/ACData> { <http://www.artscombinatories.cat/objects/Material/Wood> rdf:type ac:Material }
  
 */

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
	
	public String[] put(String key, String[] value) {
		if (key==null) return null;
		key = key.trim();
		Object prev = super.get(key);
		if (prev!=null && value!=null) {
			if (prev instanceof String) {
				String[] curr = new String[value.length+1];
				curr[0] = (String)prev;
				int i = 1;
				for (String s : value) curr[i++] = s;
				super.put(key, curr);
			} else {
				String[] arr = (String[])prev;
				String[] curr = new String[arr.length+value.length];
				int i = 0;
				for (String s : arr) curr[i++] = s;
				for (String s : value) curr[i++] = s;
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

class CustomMapDeserializer implements JsonDeserializer<CustomMap>{

	@Override
	public CustomMap deserialize(JsonElement j, Type arg1,
			JsonDeserializationContext arg2) throws JsonParseException {
		
		CustomMap r = new CustomMap();
		
		JsonObject o = j.getAsJsonObject();
		Iterator<Entry<String, JsonElement>> it = o.entrySet().iterator();
		while(it.hasNext()) {
			Entry<String, JsonElement> ent = it.next();
			if (ent.getValue().isJsonArray()) {
				Iterator<JsonElement> it2 = ent.getValue().getAsJsonArray().iterator();
				while(it2.hasNext()) {
					r.put(ent.getKey(), it2.next().getAsString());
				}
			} else {
				r.put(ent.getKey(), ent.getValue().getAsString());
			}
		}
		
		return r;
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

@SuppressWarnings("unused")
public class Migracio {
	
	/* Objectes recurrents */
	static CustomMap backupAgents = new CustomMap();
	
	static boolean MIGRAR = false;
	static boolean DOWNLOAD_DATA = false;
		
	static CustomMap errors = new CustomMap();
	
	static String hostport = "localhost:8080";
	
	/* Statistics */
	private static int error_count = 0;
	
	private static List<String> find(String f, String v, String c) throws Exception {
		String dir = "http://"+hostport+"/ArtsCombinatoriesRest/specific?f="+f+"&v="+URLEncoder.encode(v, "UTF-8")+"&c="+c;
		URL url = new URL(dir);
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestMethod("GET");

		BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	    String str;
	    StringBuffer sb = new StringBuffer();
	    while ((str = rd.readLine()) != null) {
	    	sb.append(str);
	    }

	    rd.close();
	    conn.disconnect();
	    
	    Type listType = new TypeToken<List<String>>(){}.getType();
	    return new Gson().fromJson(sb.toString(), listType);
	}
	
	private static String uploadObject(CustomMap data) throws Exception {
		if (data==null) return null;
		if (data.get("className")==null) {
			System.exit(0);
		}
		//System.out.println("Migrating data: " + data);
		if (MIGRAR) {
			URL url = new URL("http://"+hostport+"/ArtsCombinatoriesRest/objects/upload");
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
		    }

		    rd.close();
		    conn.disconnect();
		    
		    if (sb.toString().equals("error")) {
		    	error_count++;
		    	throw new Exception("Error uploading object");
		    }
		    
		    String res = sb.toString();
		   // System.out.println("SERVER RESPONSE: " +  res);
		    if ("error".equals(res)) System.out.print(data);
		    return res;
		}
		
		return null;
	}
	
	private static String updateObject(String id, CustomMap data) throws Exception {
		//System.out.println("Migrating data: id=" + id + " " + data);
		if (data==null) return null;
		if (MIGRAR) {
			URL url = new URL("http://"+hostport+"/ArtsCombinatoriesRest/objects/"+id+"/update");
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
		    }

		    rd.close();
		    conn.disconnect();
		    
		    if (sb.toString().equals("error")) {
		    	error_count++;
		    	throw new Exception("Error updating object");
		    }
		    
		    String res = sb.toString();
		   // System.out.println("SERVER RESPONSE: " +  res);
		    if ("error".equals(res)) System.out.print(data);
		    return res;
		}
		
		return null;
	}
	
	private static String uploadObjectFile(String id, String fileName) throws Exception {
		if (MIGRAR) {
			URL url = new URL("http://"+hostport+"/ArtsCombinatoriesRest/objects/"+id+"/update");
			HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		    conn.setRequestProperty("Content-Type", "application/json");
		    conn.setRequestMethod("POST");
		    conn.setDoOutput(true);
		    DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
		    FileInputStream fin = new FileInputStream(fileName);
		    byte[] input = new byte[255];
			int b = 0;
			while ((b = fin.read(input)) > 0) wr.write(input, 0, b);
		    wr.flush();
		    wr.close();
		    
		    // Get the response
		    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		    String str;
		    StringBuffer sb = new StringBuffer();
		    while ((str = rd.readLine()) != null) {
		    	sb.append(str);
		    }

		    rd.close();
		    conn.disconnect();
		    
		    if (sb.toString().equals("error")) {
		    	error_count++;
		    	throw new Exception("Error uploading file");
		    }
		    
		    String res = sb.toString();
		   // System.out.println("SERVER RESPONSE: " +  res);
		    return res;
		}
		
		return null;
	}
	
	private static CustomMap getObject(String id) throws Exception {
		URL url = new URL("http://"+hostport+"/ArtsCombinatoriesRest/objects/"+id);
		HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestMethod("GET");
		
	    BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()));
	    String str;
	    StringBuffer sb = new StringBuffer();
	    while ((str = rd.readLine()) != null) {
	    	sb.append(str);
	    }

	    rd.close();
	    conn.disconnect();
	    
	    try {
	    	GsonBuilder gson = new GsonBuilder();
	    	gson.registerTypeAdapter(CustomMap.class, new CustomMapDeserializer());
	    	CustomMap cm = gson.create().fromJson(sb.toString(), CustomMap.class);
	    	return cm;
	    } catch (Exception e) {
	    	System.out.println(sb);
	    	throw e;
	    }
	}
	
	
	
	private static void migrarPersons() {
		System.out.println(" ======================== MIGRACIO PERSONS ========================== ");
		
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
						String fatacId = url.substring(url.indexOf("id_article=")+11);
						data.put("FatacId", fatacId );
						data.put("className", "Person");
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
										data.put("givenName", object1.toString());
										//System.out.println("name " + object1.toString());
									} else if (content.equals("http://xmlns.com/foaf/0.1/familyName")) {
										data.put("lastName", object1.toString());
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
	
	 
	static Map<String, String> addedCities = new HashMap<String, String>();
	
	/* TODO: CAL una taula completa de ciutats.csv perquè aquesta funció sigui fiable */
	public static List<String> seekAndGenerateLocations(String desc) throws Exception {
		desc = desc.toLowerCase();
		CsvReader r = new CsvReader(new FileReader(new File("./masters/ciutats.csv")));
		r.readHeaders();
		
		List<String> res = new ArrayList<String>();
		
		while (r.readRecord()) {
			String uri = null;
			
			String nom = r.get("Català").toLowerCase().trim();
			String nombre = r.get("Castellà").toLowerCase().trim();
			String name = r.get("Anglès").toLowerCase().trim();
			
			if ((!"".equals(nom) && desc.contains(nom)) 
					|| (!"".equals(nombre) && desc.contains(nombre)) 
					|| (!"".equals(name) && desc.contains(name))) { 
				
				uri = addedCities.get(r.get("Català").trim());
				
				if (uri==null) {
					CustomMap city = new CustomMap();
					city.put("className", "City");
					city.put("firstName", r.get("Català").trim()+"@ca");
					city.put("firstName", r.get("Castellà").trim()+"@es");
					city.put("firstName", r.get("Anglès").trim()+"@en");
					uri = uploadObject(city);
					addedCities.put(r.get("Català").trim(), uri);
				}
				
				if (uri!=null) res.add(uri);	
			}
		}
		
		return res;
	}
	
	private static Map<String, String> objectExpedient = new HashMap<String, String>();
	
	public static void migrarEvents() {
		System.out.println(" ======================== MIGRACIO EVENTS ========================== ");
		
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
		    	//System.out.println(stmt);
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
						CustomMap event = new CustomMap();
						event.put("className", "FrameActivity");
						CustomMap caseFile = new CustomMap();
						caseFile.put("className", "CulturalManagement");
						
						String fatacId = url.substring(url.indexOf("id_article=")+11);
						event.put("FatacId", fatacId );
						
						Map<String, CustomMap> documents = new HashMap<String, CustomMap>();
						Map<String, CustomMap> specificEvents = new HashMap<String, CustomMap>();
						CustomMap currDoc = new CustomMap();
						
						String lastDocument = null;
						String idExpedient = null;
						
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
										event.put("Title", object1.toString());
										caseFile.put("Description", object1.toString());
										//System.out.println("title" + object1.toString());
									} else if (property.equals("http://purl.org/dc/elements/1.1/date")) {
										event.put("StartDate", object1.toString());
										//System.out.println("date " + object1.toString());
									} else if (property.equals("http://www.fundaciotapies.org/terms/0.1/expediente")) {
										idExpedient = object1.toString();
									}
							    } else if (subjectURI.contains("documents")) {
							    	String[] uriparts = subjectURI.split("/");
							    	String currentDoc = uriparts[uriparts.length-1];
							    	
							    	if (!currentDoc.equals(lastDocument)) {
							    		if (lastDocument!=null) {
							    			if (((String)currDoc.get("doctype")).startsWith("Touring")) {
							    				if (currDoc.get("Description")!=null) {
								    				Object d = currDoc.get("Description");
								    				List<String> ll = new ArrayList<String>();
								    				if (d instanceof String) {
								    					ll = seekAndGenerateLocations((String)d);
								    				} else ll = seekAndGenerateLocations(((String[])d)[0]);
								    				if (ll.size()>0)
								    					for (String l : ll) event.put("tookPlaceAt", l);
							    				}
							    			} else if (((String)currDoc.get("doctype")).startsWith("Introduction") || ((String)currDoc.get("doctype")).startsWith("Activity")) {
							    				event.put("Description", currDoc.get("Description"));
							    			} else if (((String)currDoc.get("doctype")).startsWith("List of works")) {
							    				currDoc.put("className", "Text");
							    				documents.put(lastDocument, currDoc);
							    			} else if (((String)currDoc.get("doctype")).startsWith("Documentation")) {
							    				// no action
							    			} else if (((String)currDoc.get("doctype")).startsWith("Collection")) {
							    				// no action
							    			} else if (((String)currDoc.get("doctype")).startsWith("Event")) {
							    				currDoc.put("className", "SpecificActivity");
							    				specificEvents.put(lastDocument, currDoc);
							    			}
							    		}
							    		currDoc = documents.get(currentDoc);
							    		if (currDoc == null) currDoc = new CustomMap();
							    		
							    		lastDocument = currentDoc;
							    	}
							    	
							    	if (property.equals("http://purl.org/dc/elements/1.1/title")) {
							    		currDoc.put("Title", object1.toString());
							    		//System.out.println("doc-title " + object1.toString());
							    	} else if (property.equals("http://purl.org/dc/elements/1.1/description")) {
							    		String desc = object1.toString().replaceAll(" class=\"spip\"", "").replaceAll(" class=\"spip_out\"","");
							    		currDoc.put("Description", desc);
							    		//System.out.println("doc-description " + desc);
							    	} else if (property.equals("http://www.fundaciotapies.org/terms/0.1/doctype")) {
							    		if (currDoc.get("doctype")==null) {
							    			currDoc.put("doctype", object1.toString().replace("@ca","").replace("@es", "").replace("@en", ""));
							    		}
							    	}
							    }
						    }
						}
						
						if (lastDocument!=null) {
							if (((String)currDoc.get("doctype")).startsWith("Touring")) {
			    				if (currDoc.get("Description")!=null) {
				    				Object d = currDoc.get("Description");
				    				List<String> ll = new ArrayList<String>();
				    				if (d instanceof String) {
				    					ll = seekAndGenerateLocations((String)d);
				    				} else ll = seekAndGenerateLocations(((String[])d)[0]);
				    				if (ll.size()>0)
				    					for (String l : ll) event.put("tookPlaceAt", l);
			    				}
			    			} else if (((String)currDoc.get("doctype")).startsWith("Introduction") || ((String)currDoc.get("doctype")).startsWith("Activity")) {
			    				event.put("Description", currDoc.get("Description"));
			    			} else if ("List of works".equals(currDoc.get("doctype"))) {
			    				currDoc.put("className", "Text"); // TODO: check whether set class is correct
			    				documents.put(lastDocument, currDoc);
			    			} else if ("Documentation".equals(currDoc.get("doctype"))) {
			    				// no action
			    			} else if ("Collection".equals(currDoc.get("doctype"))) {
			    				// no action
			    			} else if ("Event".equals(currDoc.get("doctype"))) {
			    				currDoc.put("className", "SpecificActivity");
			    				specificEvents.put(lastDocument, currDoc);
			    			}
						}
						
						System.out.println("Uploading event documents...");
						Iterator<Map.Entry<String, CustomMap>> it = documents.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String, CustomMap> entry = it.next();
							String uri = uploadObject(entry.getValue());
							caseFile.put("isWorks", uri);
						}
						System.out.println("Uploaded.");
						
						System.out.println("Uploading specific events...");
						it = specificEvents.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String, CustomMap> entry = it.next();
							String uri = uploadObject(entry.getValue());
							event.put("hasSpecificEvent", uri);
						}
						System.out.println("Uploaded.");
						
						System.out.println("Uploading event...");
						String eventUri = uploadObject(event);
						caseFile.put("references", eventUri);
						String caseFileUri = uploadObject(caseFile);
						System.out.println("Uploaded.");
						
						if (idExpedient!=null) {
							objectExpedient.put(caseFileUri, idExpedient);
						}
						
			    	} catch (Exception e) {
			    		System.out.println("ERROR with event " + object.toString() + "\n");
			    		e.printStackTrace();
			    	}
			    }
			}
		} catch (Exception e) {
			System.out.println("Error " + e);
			e.printStackTrace();
		}
	}
	
	private static Map<String, String> addedOrganizations = new HashMap<String, String>();
	
	public static void migrarPublications() {
		System.out.println(" ======================== MIGRACIO PUBLICATIONS ========================== ");
		
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
						CustomMap publication = new CustomMap();
						publication.put("className", "Publication");
						
						String fatacId = url.substring(url.indexOf("id=")+3);
						publication.put("FatacId", fatacId );
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
							//System.out.println(stmt1);
							
							Resource subject1 = stmt1.getSubject();
						    Property predicate1 = stmt1.getPredicate();   	// get the predicate
						    RDFNode object1   = stmt1.getObject();      	// get the object
						    
						    String subjectURI = subject1.toString();
						    String predicateURI = predicate1.toString();
						    String value = object1.toString();
						    
						    if (subjectURI.contains("publications") || value.startsWith("ISBN")) {
						    	if (predicateURI.equals("http://purl.org/dc/elements/1.1/title")) {
									publication.put("Title", value);
								} else if (predicateURI.equals("http://purl.org/dc/elements/1.1/description")) {
									publication.put("Description", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/cover")) {
									publication.put("Cover", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/pvp")) {
									publication.put("pvp", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/pvp_friends")) {
									publication.put("pvp_friends ", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/internal_comments")) {
									publication.put("internal_comments", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/disponible")) {
									publication.put("disponible", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/internal_ref")) {
									publication.put("internal_ref", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/special_offer")) {
									publication.put("special_offer", value);
								} else if (value.startsWith("ISBN")) {
									publication.put("ISBN", value.substring(5));
								}

						    } else if (subjectURI.contains("editors")) {
						    	if (!subjectURI.equals(editorLastId)) {
						    		if (editorLastId!=null)	editorsList.add(currEditor);
						    		editorLastId = subjectURI;
						    		currEditor = new HashMap<String, String>();
						    		currEditor.put("className", "Organisation");
						    	}
						    	
						    	if (predicateURI.equals("http://purl.org/dc/elements/1.1/date")) {
						    		currEditor.put("date", value);
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
									    
									    if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/name")) {
									    	currEditor.put("firstName", value1.trim());
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/url")) {
									    	currEditor.put("Homepage", value1);
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/location")) {
									    	List<String> l = seekAndGenerateLocations(value1);
									    	if (l.size()>0) currEditor.put("isLocatedAt", l.get(0));
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/id")) {
									    	currEditor.put("FatacId", value1);
									    }
									}
						    	}
						    } else if (subjectURI.contains("separata")) {
						    	//System.out.println(url + "<<<<< HERE");
						    	// TODO: migrar separata MANUALMENT ja que només n'hi ha 7
						    } else if (subjectURI.contains("distributors")) {
						    	if (!subjectURI.equals(distrLastId)) {
						    		if (distrLastId!=null)	distributorsList.add(currDistr);
						    		distrLastId = subjectURI;
						    		currDistr = new HashMap<String, String>();
						    		currDistr.put("className", "Organisation");
						    	}
						    	
						    	if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/id")) {
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
									    
									    if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/name")) {
									    	currDistr.put("firstName", value1);
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/url")) {
									    	currDistr.put("Homepage", value1);
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/location")) {
									    	List<String> l = seekAndGenerateLocations(value1);
									    	if (l.size()>0) currEditor.put("isLocatedAt", l.get(0));
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/id")) {
									    	currDistr.put("FatacId", value1);
									    }
									}
						    	}
						    }
						}
						
						if (editorLastId!=null)	editorsList.add(currEditor);
						if (distrLastId!=null)	distributorsList.add(currDistr);
					
						System.out.println("Uploading publication...");
						String puri = uploadObject(publication);
						System.out.println("Uploaded. ");
						
						System.out.println("Uploading publication editors...");
						for(Map<String, String> e : editorsList) {
							CustomMap role = new CustomMap();
							role.put("className","Editor");
							role.put("appliesOn", puri);
							if (e.get("date")!=null) role.put("Date", e.get("date"));
							String roleuri = uploadObject(role);
							
							String orguri = addedOrganizations.get(e.get("firstName"));
							if (orguri==null) {
								e.put("performsRole", roleuri);
								orguri = uploadObject(new CustomMap(e));
								addedOrganizations.put(e.get("firstName"), orguri);
							} else {
								CustomMap org = getObject(orguri);
								org.put("performsRole", roleuri);
								updateObject(orguri, org);
							}
						}
						System.out.println("Uploaded.");
						
						System.out.println("Uploading publication distributors...");
						for(Map<String, String> d : distributorsList) {
							CustomMap role = new CustomMap();
							role.put("className","Publisher");
							role.put("appliesOn", puri);
							String roleuri = uploadObject(role);
							
							String orguri = addedOrganizations.get(d.get("firstName"));
							if (orguri==null) {
								d.put("performsRole", roleuri);
								orguri = uploadObject(new CustomMap(d));
								addedOrganizations.put(d.get("firstName"), orguri);
							} else {
								CustomMap org = getObject(orguri);
								org.put("performsRole", roleuri);
								updateObject(orguri, org);
							}
						}
						System.out.println("Uploaded.");
						
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
	
	private static Set<String> participantsNotFound = new TreeSet<String>();
	
	private static void migrarRelation(String type1, String id1, String type2, String id2, String roleId) throws Exception {
		System.out.println("Uploading relation/role...");
		if (type1.equals("ft:event_id_1") && type2.equals("ft:event_id_2")) {
			String sid1 = getRealId("FrameActivity", id1);
			String sid2 = getRealId("FrameActivity", id2);
			String roleName = getRoleName(roleId);
			
			if ("Serie".equals(roleName)) {
				CustomMap event1 = getObject(sid1);
				event1.put("continuesTo", sid2);
				updateObject(sid1, event1);
				
				CustomMap event2 = getObject(sid2);
				event2.put("comesFrom", sid1);
				updateObject(sid2, event2);
			}
			
		} else if (type1.equals("ft:participant_id") && type2.equals("ft:event_id")) {
			String sid1 = getRealId("Person", id1);
			String sid2 = getRealId("FrameActivity", id2);
			String roleName = getRoleName(roleId);
			
			if (roleName.equals("Workshop tutor")) roleName = "Workshop_tutor";
			if (roleName.equals("chair")) roleName = "Chair";
			if (roleName.equals("Director of the project")) roleName = "Director";
			if (roleName.equals("Poet and Novelist")) roleName = "Poet";
			if (roleName.equals("Director of the project in Barcelona")) roleName = "Director";
			
			CustomMap role = new CustomMap();
			role.put("className", roleName);
			role.put("appliesOn", sid2);
			String sid3 = uploadObject(role);
			
			CustomMap person = getObject(sid1);
			person.put("performsRole", sid3);
			updateObject(sid1, person);
			
		} else if (type1.equals("ft:publication_id") && type2.equals("ft:event_id")) {
			String sid1 = getRealId("Publication", id1);
			String sid2 = getRealId("FrameActivity", id2);
			
			CustomMap pub = getObject(sid1);
			pub.put("isPublicationoftheEvent", sid2);
			updateObject(sid1, pub);
		} else if (type1.equals("ft:publication_id") && type2.equals("ft:participant_id")) {
			String sid1 = getRealId("Person", id2); // és correcte, no tocar
			String sid2 = getRealId("Publication", id1);
			String roleName = getRoleName(roleId);
			
			CustomMap role = new CustomMap();
			role.put("className", roleName);
			role.put("appliesOn", sid2);
			String sid3 = uploadObject(role);
			
			CustomMap person = getObject(sid1);
			person.put("performsRole", sid3);
			updateObject(sid1, person);
		} else if (type1.equals("ft:participant_id_1") && type2.equals("ft:participant_id_2")) {
			String sid1 = getRealId("Person", id1);
			String sid2 = getRealId("Person", id2);
			String roleName = getRoleName(roleId);
			
			CustomMap role = new CustomMap();
			role.put("className", roleName);
			role.put("appliesOn", sid2);
			String sid3 = uploadObject(role);
			
			CustomMap person = getObject(sid1);
			person.put("performsRole", sid3);
			updateObject(sid1, person);
		} else if (type1.equals("ft:separata_id")) {
			// TODO: manualment
		}
		System.out.println("Uploaded...");
	}
	
	private static void migrarRelations() {
		System.out.println(" ======================== MIGRACIO RELACIONS ========================== ");
		
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
					try {
						migrarRelation(params.get(0), params.get(1), params.get(2), params.get(3), params.get(5));
					} catch (Exception e) {
						System.out.println("WARNING: No s'ha pogut migrar la relació: (" + params.get(0) + " " + params.get(1) + ") (" + params.get(2) + " " + params.get(3) + ") " + params.get(5));
						//e.printStackTrace();
					}
			    }
			}
		 } catch (Exception e) {
			 e.printStackTrace();
		 }
	}
	
	private static List<String[]> mediaFileId = new ArrayList<String[]>();
	
	private static String[] searchAgent(String text) throws Exception {
		CsvReader r = new CsvReader(new FileReader(new File("./fm/llistaAgents.csv")));
		
		text = text.toLowerCase();
		List<String> pt = new ArrayList<String>();
		
		while(r.readRecord()) {
			String n = r.get(0);
			int idx = text.indexOf(n.toLowerCase());
			if (idx>0) pt.add((100000+idx+"").substring(1)+"____"+r.get(0)+"___"+r.get(1));
		}
		
		if (pt.size()==0)
			return null;
		else {
			Collections.sort(pt);
			String[] res = new String[pt.size()];
			for (int i=0;i<pt.size();i++) res[i] = pt.get(i).substring(pt.get(i).indexOf("____")+4);
			return res;
		}	 
	}
	
	private static void migrarFileMaker1() {
		System.out.println(" ======================== MIGRACIO FILEMAKER 1 ========================== ");
		
		try {
			CsvReader r = new CsvReader(new FileReader(new File("./fm/CF-fitxer-videos.csv")));
			r.readHeaders();
			
			String lastTitle = null;
			CustomMap caseFile = null;
			
			Set<String> linkedExpos = null;
			Set<String> prodExpoPair = null;
			Set<String> realExpoPair = null;
			
			while (r.readRecord()) {
				String[] mfid = {null, null};
				String eventUri = null;
				
				if (!r.get("títol").equals(lastTitle)) {
					if (lastTitle!=null && caseFile!=null) uploadObject(caseFile);
					caseFile = new CustomMap();
					caseFile.put("className", "Case-Files");
					lastTitle = r.get("títol");
					
					linkedExpos = new TreeSet<String>();
					prodExpoPair = new TreeSet<String>();
					realExpoPair = new TreeSet<String>();
				}
				
				CustomMap media = new CustomMap();
				media.put("className", "Video");
				
				List<String> agentProductor = new ArrayList<String>();
				List<String> agentRealitzador = new ArrayList<String>();
				
				if (r.get("any")!=null && !"".equals(r.get("any"))) {
					media.put("StartDate", r.get("any"));
				}
				
				if (r.get("Codi arxiu digital")!=null && !"".equals(r.get("Codi arxiu digital"))) {
					mfid[0] = r.get("Codi arxiu digital").trim();
				}
				
				if (r.get("núm. inventari")!=null && !"".equals(r.get("núm. inventari"))) {
					media.put("InventoryNumber", r.get("núm. inventari"));
				} 
				
				if (r.get("observacions")!=null && !"".equals(r.get("observacions"))) {
					media.put("Description", r.get("observacions"));
				}
				
				if (r.get("exposició")!=null && !"".equals(r.get("exposició"))) {
					String expo = r.get("exposició").trim();
					int idx = r.get("exposició").indexOf("(");
					if (idx!=-1) expo = expo.substring(0, idx).trim();
					
					if (!linkedExpos.contains(expo)) {
						linkedExpos.add(expo);
						
						List<String> l = find("Title", expo, "FrameActivity");
						if (l.size()==0) l = find("Title", expo, "SpecificActivity");
						
						if (l.size()>0) {
							eventUri = l.get(0);
							caseFile.put("references", eventUri);
						} else {
							System.out.println("WARNING: No s'ha trobat l'EVENT '" + expo + "' Cal relacionar-lo manualment" );
						}
					}
				}
				
				if (r.get("productor")!=null && !"".equals(r.get("productor"))) {
					String currProductor = r.get("productor").trim();
					String[] pl = searchAgent(currProductor);
				
					if (pl!=null) {
						for(String panoramix : pl) {
							int idx = panoramix.indexOf("___");
							String p = panoramix.substring(0, idx);
							String t = panoramix.substring(idx+3);
							
							List<String> l = null;
							if ("p".equals(t)) {
								l = find("givenName", p, "Person");
								if (l.size()==0) l = find("givenName", p.split(" ")[0], "Person");
							} else {
								l = find("firstName", p, "Organisation");
							}
							
							String uri = null;
							if (l.size()==0) {
								CustomMap agent = new CustomMap();
								if ("p".equals(t)) {
									String[] fname = p.split(" ");
									agent.put("className", "Person");
									agent.put("givenName", fname[0]);
									if (fname.length>1)	agent.put("lastName", p.replace(fname[0]+" ", ""));
								} else {
									agent.put("className", "Organisation");
									agent.put("firstName", p);
								}	 
								
								uri = uploadObject(agent);
							} else {
								uri = l.get(0); 
							}
							
							if (eventUri != null && !prodExpoPair.contains(uri+eventUri)) {
								agentProductor.add(uri);
								prodExpoPair.add(uri+eventUri);
							}
						}
					}
					
				} 
				
				if (r.get("Realització")!=null && !"".equals(r.get("Realització"))) {
					String currRealitzador = r.get("Realització").trim();
					String[] pl = searchAgent(currRealitzador);
					
					if (pl!=null) {
						for(String panoramix : pl) {
							int idx = panoramix.indexOf("___");
							String p = panoramix.substring(0, idx);
							String t = panoramix.substring(idx+3);
							
							List<String> l = null;
							if ("p".equals(t)) {
								l = find("givenName", p, "Person");
								if (l.size()==0) l = find("givenName", p.split(" ")[0], "Person");
							} else {
								l = find("firstName", p, "Organisation");
							}
							
							String uri = null;
							if (l.size()==0) {
								CustomMap agent = new CustomMap();
								if ("p".equals(t)) {
									String[] fname = p.split(" ");
									agent.put("className", "Person");
									agent.put("givenName", fname[0]);
									if (fname.length>1)	agent.put("lastName", p.replace(fname[0]+" ", ""));
								} else {
									agent.put("className", "Organisation");
									agent.put("firstName", p);
								}	 
								
								uri = uploadObject(agent);
							} else {
								uri = l.get(0); 
							}
							
							if (eventUri != null && !realExpoPair.contains(uri+eventUri)) {
								agentRealitzador.add(uri);
								realExpoPair.add(uri+eventUri);
							}
						}
					}
				}
				
				if (r.get("so")!=null && !"".equals(r.get("so"))) {
					String v = r.get("so");
					if ("sense so".equals(v) || "sin sonido".equals(v)) {
						media.put("Mute", "true");
					}
					if (v.contains("català") || v.contains("catalán") ) {
						media.put("hasLanguage", "Language/Catalan"); 
					}
					if (v.contains("español") || v.contains("castellano") || v.contains("castellà")) {
						media.put("hasLanguage", "Language/Spanish");
					}
					if (v.contains("english") || v.contains("anglès") || v.contains("inglés")) {
						media.put("hasLanguage", "Language/English"); 
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
					//if (caseFile.get("Description")==null)
					//	caseFile.put("Description", r.get("títol"));
					media.put("Title", r.get("títol"));
				}
				
				System.out.println("Uploading Media");
				String mediaUri = uploadObject(media);
				caseFile.put("hasMedia", mediaUri);
				
				if (mfid[0]!=null) {
					mfid[1] = mediaUri;
					mediaFileId.add(mfid);
				}
				
				if (eventUri!=null) {
					for(String id : agentProductor) {
						CustomMap role = new CustomMap();
						role.put("className", "Producer");
						role.put("appliesOn", eventUri);
						String roleUri = uploadObject(role);
						
						CustomMap prod = getObject(id);
						prod.put("performsRole", roleUri);
						updateObject(id, prod);
					}
					
					for(String id : agentRealitzador) {
						CustomMap role = new CustomMap();
						role.put("className", "Film-maker");
						role.put("appliesOn", eventUri);
						String roleUri = uploadObject(role);
						
						CustomMap prod = getObject(id);
						prod.put("performsRole", roleUri);
						updateObject(id, prod);
					}
				}
			}
			
			if (lastTitle!=null && caseFile!=null) uploadObject(caseFile);
			
			r.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void migrarFileMaker2() {
		System.out.println(" ======================== MIGRACIO FILEMAKER 2 ========================== ");
		
		try {
			CsvReader r = new CsvReader(new FileReader(new File("./fm/CF-fitxers-conferencies.csv")));
			r.readHeaders();

			CustomMap caseFile = null;
			String lastTitle = null;
			
			Set<String> linkedExpos = null;
			Set<String> prodExpoPair = null;
			Set<String> autorExpoPair = null;
			
			while (r.readRecord()) {
				String[] mfid = {null, null};
				
				String eventUri = null;
				
				if (!r.get("títol").equals(lastTitle)) {
					if (lastTitle!=null && caseFile!=null) uploadObject(caseFile);
					caseFile = new CustomMap();
					caseFile.put("className", "Case-Files");
					lastTitle = r.get("títol");
					
					linkedExpos = new TreeSet<String>();
					prodExpoPair = new TreeSet<String>();
					autorExpoPair = new TreeSet<String>();
				}
				
				CustomMap lang = new CustomMap();
				lang.put("className", "Language");
				
				CustomMap media = new CustomMap();
				media.put("className", "Audio");
				
				List<String> agentAutor = new ArrayList<String>();
				List<String> agentProductor = new ArrayList<String>();
				
				if (r.get("exposició")!=null && !"".equals(r.get("exposició"))) {
					String expo = r.get("exposició").trim();
					int idx = r.get("exposició").indexOf("(");
					if (idx!=-1) expo = expo.substring(0, idx).trim();
					
					if (!linkedExpos.contains(expo)) {
						linkedExpos.add(expo);
						List<String> l = find("Title", expo, "Event");
						
						if (l.size()>0) {
							eventUri = l.get(0); 
							caseFile.put("references", eventUri);
						} else {
							System.out.println("WARNING: No s'ha trobat l'EVENT '" + expo + "' Cal relacionar-lo manualment" );
						}
					}
				} 
				
				if (r.get("autor")!=null && !"".equals(r.get("autor"))) {
					String prodNames = r.get("autor").trim();
					String[] pl = prodNames.split(",");
					String prodTypes = r.get("persona/organització");
					String[] ptl = prodTypes.split(",");
					
					int i = 0;
					for(String p : pl) {
						String pt = "Person";
						if (ptl.length==1 && "o".equals(ptl[0]) 
							|| ptl.length>i && "o".equals(ptl[i].trim())) pt = "Organisation";
						List<String> l = find("firstName", p, pt);
						
						String uri = null;
						
						if (l.size()==0) {
							CustomMap agent = new CustomMap();
							
							agent.put("className", pt);
							agent.put("firstName", p);
							uri = uploadObject(agent);
						} else {
							uri = l.get(0); 
						}
						
						if (eventUri != null && !autorExpoPair.contains(uri+eventUri)) {
							agentAutor.add(uri);
							autorExpoPair.add(uri+eventUri);
						}

						i++;
					}
				}
				
				if (r.get("productor")!=null && !"".equals(r.get("productor"))) {
					String p = r.get("productor").trim();
					List<String> l = find("firstName", p, "Organisation");
					String uri = null;
					
					if (l.size()==0) {
						CustomMap agent = new CustomMap();
						agent.put("className", "Organisation");
						agent.put("firstName", p);
						uri = uploadObject(agent);
							
					} else {
						uri = l.get(0); 
					}
					
					if (eventUri != null && !prodExpoPair.contains(uri+eventUri)) {
						agentProductor.add(uri);
						prodExpoPair.add(uri+eventUri);
					}
				}
				
				if (r.get("contingut")!=null && !"".equals(r.get("contingut"))) {
					media.put("Description", r.get("contingut"));
				} 
				
				if (r.get("Codi arxiu digital")!=null && !"".equals(r.get("Codi arxiu digital"))) {
					mfid[0] = r.get("Codi arxiu digital").trim();
				}
				
				if (r.get("format")!=null && !"".equals(r.get("format"))) {
					media.put("OriginalSource", r.get("format").trim());
				} 
				
				if (r.get("Idioma")!=null && !"".equals(r.get("Idioma"))) {
					String v = r.get("Idioma");
					if (v.contains("català") || v.contains("catalán") ) {
						media.put("hasLanguage", "Language/Catalan"); 
					} 
					if (v.contains("español") || v.contains("castellano") || v.contains("castellà")) {
						media.put("hasLanguage", "Language/Spanish");
					} 
					if (v.contains("english") || v.contains("anglès") || v.contains("inglés")) {
						media.put("hasLanguage", "Language/English"); 
					}
					if (v.contains("àrab") || v.contains("árabe") || v.contains("arabic")) {
						media.put("hasLanguage", "Language/Arabic"); 
					}
					if (v.contains("francès") || v.contains("francés") || v.contains("french")) {
						media.put("hasLanguage", "Language/French"); 
					}
					if (v.contains("alemany") || v.contains("german") || v.contains("alemán")) {
						media.put("hasLanguage", "Language/German"); 
					}
					if (v.contains("italià") || v.contains("italian") || v.contains("italiano")) {
						media.put("hasLanguage", "Language/Italian"); 
					}
				} 
				
				if (r.get("núm. inv.")!=null && !"".equals(r.get("núm. inv."))) {
					media.put("InventoryNumber", r.get("núm. inv."));
				}

				if (r.get("títol")!=null && !"".equals(r.get("títol"))) {
					//if (caseFile.get("Description")==null)
					//	caseFile.put("Description", r.get("títol"));
					media.put("Title", r.get("títol"));
				}
				
				System.out.println("Uploading Media");
				String mediaUri = uploadObject(media);
				caseFile.put("hasMedia", mediaUri);
				
				if (mfid[0]!=null) {					
					mfid[1] = mediaUri;
					mediaFileId.add(mfid);
				}
				
				if (eventUri!=null) {
					for(String id : agentProductor) {
						CustomMap role = new CustomMap();
						role.put("className", "Producer");
						role.put("appliesOn", eventUri);
						String roleUri = uploadObject(role);
						
						CustomMap prod = getObject(id);
						prod.put("performsRole", roleUri);
						updateObject(id, prod);
					}
					
					for(String id : agentAutor) {
						CustomMap role = new CustomMap();
						role.put("className", "Lecturer");
						role.put("appliesOn", eventUri);
						String roleUri = uploadObject(role);
						
						CustomMap autor = getObject(id);					
						autor.put("performsRole", roleUri);
						updateObject(id, autor);
					}
				}
			}
			
			if (lastTitle!=null && caseFile!=null) uploadObject(caseFile);
			
			r.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	// TODO: relacionar ArtWorks i Case-Files
	private static void migrarFileMaker3() {
		System.out.println(" ======================== MIGRACIO FILEMAKER 3 ========================== ");
		
		try {
			CsvReader r = new CsvReader(new FileReader(new File("./fm/SF-MASER-COLLECIO.csv")));
			r.readHeaders();
			
			while (r.readRecord()) {
				CustomMap work = new CustomMap();
				work.put("className", "AT_Work_FAT_Collection");
				List<String> editorList = new ArrayList<String>();
				
				//List<CustomMap> lendings = new ArrayList<CustomMap>();
				
				if (r.get("Núm. fotog.")!=null && !"".equals(r.get("Núm. fotog."))) {
					work.put("NumberT", r.get("Núm. fotog."));
				}
				if (r.get("Nombre d'exemplars")!=null && !"".equals(r.get("Nombre d'exemplars"))) {
					work.put("NumberofCopies", r.get("Nombre d'exemplars"));
				}
				if (r.get("Any")!=null && !"".equals(r.get("Any"))) {
					work.put("Date", r.get("Any"));
				}
				if (r.get("Bibliografia")!=null && !"".equals(r.get("Bibliografia"))) {
					String[] bib = r.get("Bibliografia").split("\n");
					for (String b : bib) work.put("Bibliography", b);
				}
				if (r.get("Descripció de l'objecte")!=null && !"".equals(r.get("Descripció de l'objecte"))) {
					work.put("Description", r.get("Descripció de l'objecte"));
				}
				if (r.get("Estat de conservació")!=null && !"".equals(r.get("Estat de conservació"))) {
					work.put("Conservation", r.get("Estat de conservació"));
				}
				if (r.get("Exposicions")!=null && !"".equals(r.get("Exposicions"))) {
					// TODO: pocs registres i poc automatitzable: MANUALMENT
				}
				if (r.get("Localització")!=null && !"".equals(r.get("Localització"))) {
					work.put("CurrentSituation", r.get("Localització"));
					// TODO: falta un camp intern a exportar a excel
				}
				if (r.get("Mides")!=null && !"".equals(r.get("Mides"))) {
					work.put("measurement", r.get("Mides").trim());
				}
				if (r.get("obra gràfica =")!=null && !"".equals(r.get("obra gràfica ="))) {
					work.put("hasType", "KindArtWork/Graphic");
				}
				if (r.get("obra original =")!=null && !"".equals(r.get("obra original ="))) {
					work.put("hasType", "KindArtWork/Original");
				}
				if (r.get("préstec a :")!=null && !"".equals(r.get("préstec a :"))) {
					// TODO: pocs registres i poc automatitzable: MANUALMENT
				}
				if (r.get("Tècnica")!=null && !"".equals(r.get("Tècnica"))) {
					work.put("Technique", r.get("Tècnica")+"@ca");
				}
				if (r.get("técnica")!=null && !"".equals(r.get("técnica"))) {
					work.put("Technique", r.get("técnica")+"@es");
				} else if (r.get("trad.castellano técnica:")!=null && !"".equals(r.get("trad.castellano técnica:"))) {
					work.put("Technique", r.get("trad.castellano técnica:").trim()+"@es");
				}
				if (r.get("trad.english technique:")!=null && !"".equals(r.get("trad.english technique:"))) {
					work.put("Technique", r.get("trad.english technique:").trim()+"@en");
				}
				if (r.get("trad.français technique:")!=null && !"".equals(r.get("trad.français technique:"))) {
					work.put("Technique", r.get("trad.français technique:").trim()+"@fr");
				}
				
				if (r.get("Títol")!=null && !"".equals(r.get("Títol"))) {
					work.put("Title", r.get("Títol").trim()+"@ca");
				}
				if (r.get("trad. castellano")!=null && !"".equals(r.get("trad. castellano"))) {
					work.put("Title", r.get("trad. castellano").trim()+"@es");
				}
				if (r.get("trad. english")!=null && !"".equals(r.get("trad. english"))) {
					work.put("Title", r.get("trad. english").trim()+"@en");
				}
				if (r.get("trad. français")!=null && !"".equals(r.get("trad. français"))) {
					work.put("Title", r.get("trad. français").trim()+"@fr");
				}
				if (r.get("Valoració Econòmica en €")!=null && !"".equals(r.get("Valoració Econòmica en €"))) {
					work.put("EstimatedValue", r.get("Valoració Econòmica en €"));
				}
				if (r.get("Edició")!=null && !"".equals(r.get("Edició"))) {
					work.put("className", "Publication");
					
					if (r.get("Editor")!=null && !"".equals(r.get("Editor"))) {
						String[] editors = searchAgent(r.get("Editor"));
						
						if (editors!=null) {
							for(String panoramix : editors) {
								int idx = panoramix.indexOf("___");
								String p = panoramix.substring(0, idx);
								
								List<String> l = find("firstName", p, "Organisation");
								
								String uri = null;
								if (l.size()==0) {
									CustomMap agent = new CustomMap();
									agent.put("className", "Organisation");
									agent.put("firstName", p);
									uri = uploadObject(agent);
								} else {
									uri = l.get(0); 
								}	
								
								editorList.add(uri);
							}
						}
					}
					
					// TODO: aclarir què fer amb aquests camps
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
				}
				
				String puburi = uploadObject(work);
				for (String e : editorList) {
					CustomMap role = new CustomMap();
					role.put("className", "Editor");
					role.put("appliesOn", puburi);
					String roleUri = uploadObject(role);
					
					CustomMap editor = getObject(e);
					editor.put("performsRole", roleUri);
				}
			}
			r.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static List<String> getMediaList(String path) {
		File f = new File(path); 
		
		List<String> l = new ArrayList<String>();
		String[] subfiles = f.list();
		
		if (subfiles!=null) {
			for (String s : subfiles) l.addAll(getMediaList(path+"/"+s));
		} else {
			l.add(path);
		}
		return l;
	}
	
	private static void afegirACollection(String objectId, String codi) throws Exception {
		int idx = codi.indexOf("C0");
		String codiCol = codi.substring(idx,idx+4);
		for (String[] c : collectionList) {
			if (c[0].equals(codiCol)) {
				CustomMap media = getObject(objectId);
				media.put("isCollectedIn", c[2]);
				updateObject(objectId, media);
				break;
			}
		}
	}
	
	
	// TODO: probablement hi ha objectes que tenen més d'un video! <--- verificar
	private static void migrarMedia() {
		System.out.println(" ======================== MIGRACIO AUDIO/VIDEO ========================== ");
		
		//List<String> llistaFitxersMedia = getMediaList("BANC ÀUDIOVISUAL"); TODO set path
		// TODO: obtenir dades adicionals video (duració, format, bitrate, etc.)
		List<String> llistaFitxersMedia = getMediaList("/home/jordi.roig.prieto/Prova");
		
		try {
			for (String[] dup : mediaFileId) {
				String fileId = dup[0];
				String objectId = dup[1];
				
				for (String fn : llistaFitxersMedia) {
					if (fn.contains(fileId)) {
						System.out.println("Uploading media file for object " + objectId);
						uploadObjectFile(objectId, fn); 
						afegirACollection(objectId, fn);
						break;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static void putImageData(CustomMap image, String fn, String fn2) {
		if (fn2.startsWith("FF1")) {
			image.put("OriginalSource", "Paper");
		} else if (fn2.startsWith("FF2B")) {
			image.put("OriginalSource", "Positiu 135 mm b/n");
		} else if (fn2.startsWith("FF2C")) {
			image.put("OriginalSource", "Positiu 135 mm col.");
		} else if (fn2.startsWith("FF3B")) {
			image.put("OriginalSource", "Negatiu 135 mm b/n");
		} else if (fn2.startsWith("FF3C")) {
			image.put("OriginalSource", "Negatiu 135 mm col.");
		} else if (fn2.startsWith("FF4B")) {
			image.put("OriginalSource", "Positiu 120 mm b/n");
		} else if (fn2.startsWith("FF4C")) {
			image.put("OriginalSource", "Positiu 120 mm col.");
		} else if (fn2.startsWith("FF5B")) {
			image.put("OriginalSource", "Negatiu 120 mm b/n");
		} else if (fn2.startsWith("FF5C")) {
			image.put("OriginalSource", "Negatiu 120 mm col.");
		} else if (fn2.startsWith("FF6B")) {
			image.put("OriginalSource", "Positiu Placa b/n");
		} else if (fn2.startsWith("FF6C")) {
			image.put("OriginalSource", "Positiu Placa col.");
		} else if (fn2.startsWith("FF7A")) {
			image.put("OriginalSource", "Negatiu Placa b/n");
		} else if (fn2.startsWith("FF7B")) {
			image.put("OriginalSource", "Negatiu Placa col·");
		} else if (fn2.startsWith("FF8")) {
			image.put("OriginalSource", "Digital");
		}
		
		image.put("format",fn2.substring(fn2.length()-3));
		
		int idx = fn2.indexOf("C0");
		String codiCol = fn2.substring(idx,idx+4);
		for (String[] c : collectionList) {
			if (c[0].equals(codiCol)) {
				image.put("isCollectedIn", c[2]);
				break;
			}
		}
				
		// TODO: obtenir format imatge
	}
	
	private static void migrarImages() {
		System.out.println(" ======================== MIGRACIO IMAGES ========================== ");
		
		//List<String> llistaFitxersMedia = getMediaList("BANC D'IMATGES HISTÒRIC"); TODO set path
		List<String> llistaFitxersMedia = getMediaList("/home/jordi.roig.prieto/Prova");
		
		try {
			Set<Map.Entry<String, String>> ent = objectExpedient.entrySet();
			for (Map.Entry<String, String> e : ent) {
				String uriExp = e.getKey();
				String codiExp = e.getValue();
				
				for (String fn : llistaFitxersMedia) {
					int idx = fn.indexOf(codiExp+"_FF");
					if (idx==-1) continue;
					
					CustomMap image = new CustomMap();
					image.put("className", "Image");
					String fn2 = fn.substring(idx);
					
					putImageData(image, fn, fn2);
					
					System.out.println("Uploading image");
					String imageuri = uploadObject(image);
					System.out.println("Uploaded.");
					
					uploadObjectFile(imageuri, fn);
					
					CustomMap exp = getObject(uriExp);
					exp.put("hasMedia", imageuri);
					updateObject(uriExp, exp);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static String[][] collectionList = {
	
		{"C001", "Inauguracions", null, "Opening"},
		{"C002", "Conferències i seminaris", null, "LecturesandSeminars"},
		{"C003", "Roda de premsa", null, "PressReleases"},
		{"C004", "Sopars", null, "Dinners"},
		{"C005", "Acords institucionals", null, "InstitutionalAgreement"},
		{"C006", "Emissions i gravacions: TV, ràdio, cinema", null, "TvRadioCinema"},
		{"C007", "Visites protocol·làries", null, "ProtocolVisits"},
		{"C008", "Espectacles", null, "Show"},
		{"C009", "Activitats paral·les", null, "OtherActivities"},
		{"C010", "Itineràncies", null, "ExhibitionTourings"},
		{"C011", "Fotografies d'instal·lació", null, "PhotoInstallation"},
		{"C012", "Llibre d'instal·lació", null, "InstallationGuide"},
		{"C013", "Entrevistes", null, "Interviews"},
		{"C014", "Presentacions", null, "PresentationsorLaunchs"},
		{"C015", "Tallers i trobades", null, "WorkshopandMeeting"},
		{"C016", "Obra", null, "Work"},
		{"C017", "Material per premsa", null, "PressKits"},
		{"C018", "Foto d'instal·lació Col·lecció EspaiA", null, "PhotoInstallationCollectionSpaceA"},
		{"C019", "Servei educatiu taller", null, "EducationalServicesWorkshop"},
		{"C020", "Servei educatiu visites dinamitzades", null, "EducationalDynamicVisit"},
		{"C021", "Muntatge exposició", null, "Set-upProject"},
		{"C022", "Foto instal·lació fora FAT", null, "PhotoInstallationOutsideFat"},
		{"C023", "Performance", null, "Performance"},
		{"C024", "Portes obertes", null, "FreeAdmissionDays"},
		{"C025", "Clausura", null, "Close"},
		{"C026", "Curs", null, "Courses"}
	
	};

	
	private static void migrarCollections() {
		try {
			System.out.println("Uploading collections...");
			int idx = 0;
			while(idx<collectionList.length) {
				CustomMap collection = new CustomMap();
				collection.put("className", collectionList[idx][3]);
				collection.put("Title", collectionList[idx][1]);
				String uri = uploadObject(collection);
				collectionList[idx][2] = uri;
				idx++;
			}
			System.out.println("Uploaded.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static String getCollectionUri(String code) {
		for (String[] c : collectionList) {
			if (c[0].equals(code)) return c[2];
		}
		return null;
	}
	
	public static void collectAgents() throws Exception {
		CsvReader r = new CsvReader(new FileReader(new File("./fm/CF-fitxer-videos.csv")));
		r.readHeaders();
		
		Set<String> agents = new TreeSet<String>();
		while (r.readRecord()) {
			if (r.get("Realització")!=null && !"".equals(r.get("Realització"))) agents.add(r.get("Realització").trim());
			if (r.get("productor")!=null && !"".equals(r.get("productor"))) agents.add(r.get("productor").trim());
		}
		
		Iterator<String> it = agents.iterator();
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}
	
	public static void collectCities() throws Exception {
		CsvReader r = new CsvReader(new FileReader(new File("./fm/SF-MASER-COLLECIO.csv")));
		r.readHeaders();
		
		Set<String> cities = new TreeSet<String>();
		while (r.readRecord()) {
			if (r.get("Editor")!=null && !"".equals(r.get("Editor"))) {
				int idx = r.get("Editor").indexOf(",");
				if (idx>0) cities.add(r.get("Editor").substring(idx+1).trim());
				
				
			}
		}
		
		Iterator<String> it = cities.iterator();
		while (it.hasNext()) {
			System.out.println(it.next());
		}
	}
	
	public static void main(String[] args) throws Exception {
		DOWNLOAD_DATA = true;
		MIGRAR = true;	
		
		SimpleDateFormat sdf = new SimpleDateFormat("hh:mm");
		System.out.println("Starting migration at " + sdf.format(new GregorianCalendar().getTime()));
		
		// ----- Migració de SPIP
		//migrarPersons(); 							// DONE.
		//migrarEvents();							// DONE.
		//migrarPublications();						// DONE.
		//migrarRelations();						// DONE.
		
		// ----- Migració de File-Maker
		//migrarFileMaker1();
		//migrarFileMaker2(); 
		migrarFileMaker3();
		
		// ----- Migració de Media
		//migrarCollections(); 						// DONE.
		//migrarMedia();
		//migrarImages();
		
		//System.out.println(participantsNotFound);
		
		System.out.println("FINISHED migration at " + sdf.format(new GregorianCalendar().getTime()));
		// -- utils
		//collectAgents();
		//collectCities();
		
	}

	// TODO: recordar que faltaven camps per exportar del file maker
	
	// TODO: recordar determinar el doctype documentation en els events

}
