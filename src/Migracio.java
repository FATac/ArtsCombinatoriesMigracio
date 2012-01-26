import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.util.Calendar;
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

import org.apache.log4j.Logger;
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

class CustomMap extends HashMap<String, Object>{
	private static final long serialVersionUID = -1812690206134151827L;
	
	public String put(String key, String value) {
		if (key==null) return null;
		key = key.trim();
		Object prev = super.get(key);
		if (prev!=null && value!=null) {
			if (!"about".equals(key)) {
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
			}
		} else {
			super.put(key, value);
		}
		
		return value;
	}
	
	public String[] put(String key, String[] value) {
		if ("about".equals(key)) return null;
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

enum Migrar { NOMES_DADES, NOMES_MEDIA, TOT, RES }

@SuppressWarnings("unused")
public class Migracio {
	private static Logger log = Logger.getLogger(Migracio.class); 
	
	/* Objectes recurrents */
	static Map<String, String> backupAgents = new HashMap<String, String>();
	static Map<String, String> realIds = new HashMap<String, String>();
	
	static boolean DOWNLOAD_DATA = true;
	static Migrar migrar = Migrar.TOT;
	static String directori_medias = "";
		
	static CustomMap errors = new CustomMap();
	
	static String hostport = "67.202.24.185:8080";
	
	/* Statistics */
	private static int error_count = 0;
	
	private static List<String> find(String f, String v, String c) throws Exception {
		if (v.contains("\n")) v = v.split("\n")[0];
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
		if (data.get("type")==null) {
			System.exit(0);
		}
		//log.debug("Migrating data: " + data);
		if (migrar != Migrar.RES) {
			URL url = new URL("http://"+hostport+"/ArtsCombinatoriesRest/resource/upload");
		    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		    conn.setRequestProperty("Content-Type", "application/json");
		    conn.setRequestMethod("PUT");
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
		   // log.debug("SERVER RESPONSE: " +  res);
		    if ("error".equals(res)) System.out.print(data);
		    return res;
		}
		
		return null;
	}
	
	private static void reseteja(String time) throws Exception {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yy kk:mm");
		String confirm = URLEncoder.encode(sdf.format(Calendar.getInstance().getTime()), "UTF-8");
		if (time!=null) confirm = URLEncoder.encode(time, "UTF-8");
		URL url = new URL("http://"+hostport+"/ArtsCombinatoriesRest/reset?confirm="+confirm);
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
	    
	    if ("error".equals(sb)) throw new Exception("Server Error esborrant-ho tot!");
	}
	
	private static String updateObject(String id, CustomMap data) throws Exception {
		//log.debug("Migrating data: id=" + id + " " + data);
		if (data==null) return null;
		if (migrar != Migrar.RES) {
			URL url = new URL("http://"+hostport+"/ArtsCombinatoriesRest/resource/"+id+"/update");
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
		   // log.debug("SERVER RESPONSE: " +  res);
		    if ("error".equals(res)) System.out.print(data);
		    return res;
		}
		
		return null;
	}
	
	private static String uploadObjectFile(String fileName) throws Exception {
		if (migrar != Migrar.RES) {
			try {
				String[] parts = fileName.split("\\/");
				String fileNameOnly = parts[parts.length-1];
				URL url = new URL("http://"+hostport+"/ArtsCombinatoriesRest/media/upload?fn=" + URLEncoder.encode(fileNameOnly,"UTF-8"));
				
				HttpURLConnection conn = (HttpURLConnection)url.openConnection();
				conn.setRequestProperty("Content-Type", "application/json");
			    conn.setRequestMethod("POST");
			    conn.setDoOutput(true);
			    File file = new File(fileName);
			    FileInputStream fin = new FileInputStream(file);
			    conn.setFixedLengthStreamingMode((int)file.length());
			    
			    DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
			    
			    byte[] input = new byte[1024*4];
				int b = 0;
				
				while ((b = fin.read(input)) > 0) {
					wr.write(input, 0, b);
					wr.flush();
				}
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
			   // log.debug("SERVER RESPONSE: " +  res);
			    return res;
			} catch (OutOfMemoryError e) {
				log.debug("WARNING: Problema de falta de memoria pujant arxiu: " + fileName);
			} catch (FileNotFoundException e) {
				log.error("",e);
			}
		}
		
		return null;
	}
	
	private static CustomMap getObject(String id) throws Exception {
		URL url = new URL("http://"+hostport+"/ArtsCombinatoriesRest/resource/"+id);
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
	    	log.debug(sb);
	    	throw e;
	    }
	}
	
	static Map<String, String> addedCountries = new HashMap<String, String>();
	
	private static void migrarPersons() {
		log.debug(" ======================== MIGRACIO PERSONS ========================== ");
		
		try {
			// baixar info de persones 
			if (DOWNLOAD_DATA) {
				//log.debug("Downloading persons rdf data...");
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
						CustomMap person = new CustomMap();
						String fatacId = url.substring(url.indexOf("id_article=")+11);
						person.put("ac:FatacId", fatacId );
						person.put("type", "ac:Person");
						//log.debug("FatacId " + fatacId);
						
						//log.debug("Reading person...");
						String contryNotFound = null;
						
						while(it2.hasNext()) {
							Statement stmt1 = it2.next();
							
							Resource  subject1   = stmt1.getSubject();     	// get the subject
						    Property  predicate1 = stmt1.getPredicate();   	// get the predicate
						    RDFNode   object1   = stmt1.getObject();      	// get the object
						    
						    //log.debug(stmt1);
						    String content = predicate1.toString();
							
						    if (subject1.isURIResource()) {
						    	String currentUri = subject1.toString();
						    	
						    	if (currentUri.contains("participants")) {
						    		if (content.equals("http://xmlns.com/foaf/0.1/givenName")) {
										person.put("ac:givenName", object1.toString().trim());
										//log.debug("name " + object1.toString());
									} else if (content.equals("http://xmlns.com/foaf/0.1/familyName")) {
										person.put("ac:familyName", object1.toString().trim());
										//log.debug("surname " + object1.toString());
									} else if (content.equals("http://purl.org/vocab/bio/0.1/biography")) {
										String bio = object1.toString().replaceAll(" class=\"spip\"", "").replaceAll(" class=\"spip_out\"","");	
										person.put("ac:Bio", bio);
										//log.debug("CV " + bio);
									}
						    	}
						    } else if (content.equals("http://purl.org/vocab/bio/0.1/date")) {
								person.put("ac:BirthDate", object1.toString());
								//log.debug("BirthDate " + object1.toString());
							} else if (content.equals("http://purl.org/vocab/bio/0.1/place")) {
								String valEn = searchCityCountry(object1.asLiteral().getString(), "en")[0];
								String countryUri = addedCountries.get(valEn);
								if (countryUri == null) {
									String valEs = searchCityCountry(object1.asLiteral().getString(), "es")[0];
									String valCa = searchCityCountry(object1.asLiteral().getString(), "ca")[0];
									if (valCa != null) {
										CustomMap country = new CustomMap();
										country.put("type", "ac:Country");
										country.put("about", valCa);
										country.put("ac:Name", valCa+"@ca");
										country.put("ac:Name", valEs+"@es");
										country.put("ac:Name", valEn+"@en");
										countryUri = uploadObject(country);
										addedCountries.put(valEn, countryUri);
									} else {
										contryNotFound = object1.asLiteral().getString();
									}
								}
								person.put("ac:bornIn", countryUri);
								//log.debug("bornIn " + countryUri);
							}
						}
						
						if (contryNotFound != null) {
							log.debug("No s'ha pogut trobat cap pais de neixement pel nom : " + contryNotFound + ". Persona " + person.get("ac:givenName") + " " + person.get("ac:familyName"));
						}
						
						//log.debug("Uploading person...");
						String fullName = ((person.get("ac:givenName")!=null?person.get("ac:givenName")+" ":"") + (person.get("ac:familyName")!=null?person.get("ac:familyName"):"")).trim();
						person.put("about", fullName);
						person.put("ac:Name", fullName);
						String agentUri = uploadObject(person);
						backupAgents.put(fullName, agentUri);
						realIds.put((String)person.get("ac:FatacId"), agentUri);

						//log.debug("Uploaded. ");
			    	} catch (Exception e) {
			    		log.debug("ERROR with person " + object.toString());
			    		log.error("",e);
			    	}
			    }
			}
		} catch (Exception e) {
			log.debug("Error migrating persons " + e);
			log.error("",e);
		}

	}
	
	static Map<String, String> addedCities = new HashMap<String, String>();
	
	public static String[] searchCityCountry(String cityName, String lang) {
		String countryName = null;
		
		try {
		    // Send data
		    URL url = new URL("http://api.geonames.org/searchJSON?formatted=true&q="+URLEncoder.encode(cityName, "UTF-8")+"&maxRows=10&lang="+lang+"&username=johnsmith&style=full");
		    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
		    conn.setRequestProperty("Content-Type", "application/json");
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
		    
		    int idx1 = sb.indexOf("\"countryName\": \"") + 16;
		    if (idx1>15) {
		    	int idx2 = sb.indexOf("\"", idx1);
		    	countryName = sb.substring(idx1, idx2);
		    }
		    
		    idx1 = sb.indexOf("\"lang\": \""+lang+"\"") - 13;
		    if (idx1>-1) {
		    	int idx2 = sb.indexOf("\"name\": \"",idx1-45) + 9;
		    	cityName = sb.substring(idx2, idx1);
		    }
		} catch (Exception e) {
			log.error("",e);
			return null;
		}
		
		return new String[]{countryName, cityName};
	}
	
	
	
	/* TODO: CAL una taula completa de ciutats.csv i amb els noms alinieats per idoma perquè aquesta funció sigui fiable */
	public static List<String> seekAndGenerateLocations(String desc) throws Exception {
		String[] lang = {"ca", "es", "en"};
		
		desc = desc.replace('<', ' ').replace('>', ' ').replace(',', ' ')
					 .replace('.', ' ').replace('(', ' ').replace(')', ' ')
					 .replace('[', ' ').replace(']', ' ').replace('!', ' ');
		desc = " " + desc + " ";
		
		String tmp = null;
		
		CsvReader r = new CsvReader(new FileReader(new File("./masters/ciutats.csv")));
		r.readHeaders();
		
		List<String> res = new ArrayList<String>();
		
		while (r.readRecord()) {
			String uri = null;
			
			String nom = r.get("Català").trim();
			String nombre = r.get("Castellà").trim();
			String name = r.get("Anglès").trim();
			
			String foundName = null;
			if (desc.contains(" "+nom+" ")) {
				foundName = nom.trim();
			} else if (desc.contains(" "+nombre+" ")) {
				foundName = nombre.trim();
			} else if (desc.contains(" "+name+" ")) {
				foundName = name.trim();
			}
			
			if (foundName!=null && !"".equals(foundName.trim())) {
				desc = desc.replaceAll(foundName, "");
				uri = addedCities.get(foundName);
				
				if (uri==null) {
					CustomMap country = new CustomMap();
					country.put("type", "ac:Country");
					CustomMap city = new CustomMap();
					city.put("type", "ac:City");
					
					String countryUri = null;
					List<String> cn = new ArrayList<String>();
					
					for (String l : lang) {
						String[] names = new String[2];
						names = searchCityCountry(foundName, l);
						
						if (names!=null) {
							city.put("ac:Name", names[1]+"@"+l);
							if (l.equals("ca")) city.put("about", names[1]);
							cn.add(names[1]);
							tmp = names[0];
							country.put("ac:Name", names[0]+"@"+l);
							if (l.equals("ca")) country.put("about", names[0]);
						} else {
							city.put("ac:Name", foundName+"@"+l);
						}
					}
					
					countryUri = addedCountries.get(tmp);
					if (countryUri == null) {
						countryUri = uploadObject(country);
						addedCountries.put(tmp, countryUri);
					}
					
					city.put("ac:IsLocatedIn", countryUri);
					uri = uploadObject(city);
					for (String n : cn) addedCities.put(n, uri);
				}
				
				if (uri!=null) res.add(uri);	
			}
		}
		
		return res;
	}
	
	private static String clearHtmlObjects(String html, String tag) {
		int idx = 0;
		do {
			idx = html.indexOf("<"+tag);
			if (idx>-1)	html = html.substring(0, idx)+html.substring(html.indexOf("</"+tag+">")+9);
		} while(idx>-1);
		return html;
	}
	
	private static Map<String, String> objectExpedient = new HashMap<String, String>();
	private static Map<String, String> eventExpedient = new HashMap<String, String>();
	
	public static void migrarEvents() {
		log.debug(" ======================== MIGRACIO EVENTS ========================== ");
		
		try {
			// migrar events
			if (DOWNLOAD_DATA) {
				//log.debug("Downloading events rdf data...");
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
		    	//log.debug(stmt);
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
						event.put("type", "ac:FrameActivity");
						CustomMap caseFile = new CustomMap();
						caseFile.put("type", "ac:CulturalManagement");
						
						String fatacId = url.substring(url.indexOf("id_article=")+11);
						event.put("ac:FatacId", fatacId );
						
						Map<String, CustomMap> documents = new HashMap<String, CustomMap>();
						Map<String, CustomMap> specificEvents = new HashMap<String, CustomMap>();
						CustomMap currDoc = new CustomMap();
						List<String> touringsAdded = new ArrayList<String>();
						
						String lastDocument = null;
						String idExpedient = null;
						
						//log.debug("Reading event...");
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
										event.put("ac:Title", object1.toString());
										if ("ca".equals(object1.asLiteral().getLanguage())) {
											event.put("about", object1.asLiteral().getString());
											caseFile.put("about", "Expedient " + object1.asLiteral().getString());
										}
										//caseFile.put("Description", object1.toString());
										//log.debug("title" + object1.toString());
									} else if (property.equals("http://purl.org/dc/elements/1.1/date")) {
										event.put("ac:StartDate", object1.toString());
										//log.debug("date " + object1.toString());
									} else if (property.equals("http://www.fundaciotapies.org/terms/0.1/expediente")) {
										idExpedient = object1.toString();
									}
							    } else if (subjectURI.contains("documents")) {
							    	String[] uriparts = subjectURI.split("/");
							    	String currentDoc = uriparts[uriparts.length-1];
							    	
							    	if (!currentDoc.equals(lastDocument)) {
							    		if (lastDocument!=null) {
							    			if (((String)currDoc.get("doctype")).startsWith("Touring")) {
							    				if (!touringsAdded.contains(lastDocument)) { 
								    				if (currDoc.get("ac:Description")!=null) {
									    				Object d = currDoc.get("ac:Description");
									    				List<String> ll = new ArrayList<String>();
									    				if (d instanceof String) {
									    					ll = seekAndGenerateLocations((String)d);
									    				} else ll = seekAndGenerateLocations(((String[])d)[0]);
									    				if (ll.size()>0)
									    					for (String l : ll) event.put("ac:tookPlaceAt", l);
								    				}
								    				
								    				touringsAdded.add(lastDocument);
							    				}
							    			} else if (((String)currDoc.get("doctype")).startsWith("Introduction")) {
							    				event.put("ac:Description", currDoc.get("ac:Description"));
							    			} else if (((String)currDoc.get("doctype")).startsWith("List of works")) {
							    				currDoc.put("type", "ac:Text");	// TODO: check whether set class is correct
							    				documents.put(lastDocument, currDoc);
							    			} else if (((String)currDoc.get("doctype")).startsWith("Documentation")) {
							    				currDoc.put("type", "ac:Text");	// TODO: check whether set class is correct
							    				documents.put(lastDocument, currDoc);
							    			} else if (((String)currDoc.get("doctype")).startsWith("Collection")) {
							    				// no action
							    			} else if (((String)currDoc.get("doctype")).startsWith("Event") || ((String)currDoc.get("doctype")).startsWith("Activity")) {
							    				currDoc.put("ac:FatacId", lastDocument );
							    				currDoc.put("type", "ac:SpecificActivity");
							    				specificEvents.put(lastDocument, currDoc);
							    			}
							    		}
							    		currDoc = documents.get(currentDoc);
							    		currDoc = specificEvents.get(currentDoc);
							    		if (currDoc == null) currDoc = new CustomMap();
							    		
							    		lastDocument = currentDoc;
							    	}
							    	
							    	if (property.equals("http://purl.org/dc/elements/1.1/title")) {
							    		currDoc.put("ac:Title", object1.toString());
							    		if ("ca".equals(object1.asLiteral().getLanguage())) currDoc.put("about", object1.asLiteral().getString());
							    		//log.debug("doc-title " + object1.toString());
							    	} else if (property.equals("http://purl.org/dc/elements/1.1/description")) {
							    		String desc = object1.toString().replaceAll(" class=\"spip\"", "").replaceAll(" class=\"spip_out\"","");
							    		desc = clearHtmlObjects(desc, "iframe");
							    		currDoc.put("ac:Description", desc);
							    		//log.debug("doc-description " + desc);
							    	} else if (property.equals("http://www.fundaciotapies.org/terms/0.1/doctype")) {
							    		if (currDoc.get("doctype")==null) {
							    			currDoc.put("doctype", object1.asLiteral().getString());
							    		}
							    	}
							    }
						    }
						}
						
						if (lastDocument!=null) {
							if (((String)currDoc.get("doctype")).startsWith("Touring")) {
			    				if (currDoc.get("ac:Description")!=null) {
				    				Object d = currDoc.get("ac:Description");
				    				List<String> ll = new ArrayList<String>();
				    				if (d instanceof String) {
				    					ll = seekAndGenerateLocations((String)d);
				    				} else ll = seekAndGenerateLocations(((String[])d)[0]);
				    				if (ll.size()>0)
				    					for (String l : ll) event.put("ac:tookPlaceAt", l);
			    				}
			    			} else if (((String)currDoc.get("doctype")).startsWith("Introduction")) {
			    				event.put("ac:Description", currDoc.get("ac:Description"));
			    			} else if ("List of works".equals(currDoc.get("doctype"))) {
			    				currDoc.put("type", "ac:Text"); // TODO: check whether set class is correct
			    				documents.put(lastDocument, currDoc);
			    			} else if ("Documentation".equals(currDoc.get("doctype"))) {
			    				currDoc.put("type", "ac:Text"); // TODO: check whether set class is correct
			    				documents.put(lastDocument, currDoc);
			    			} else if ("Collection".equals(currDoc.get("doctype"))) {
			    				// no action
			    			} else if ("Event".equals(currDoc.get("doctype")) || ((String)currDoc.get("doctype")).startsWith("Activity")) {
			    				currDoc.put("ac:FatacId", lastDocument );
			    				currDoc.put("type", "ac:SpecificActivity");
			    				specificEvents.put(lastDocument, currDoc);
			    			}
						}
						
						//log.debug("Uploading event documents...");
						Iterator<Map.Entry<String, CustomMap>> it = documents.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String, CustomMap> entry = it.next();
							String uri = uploadObject(entry.getValue());
							if (entry.getValue().get("ac:FatacId")!=null) realIds.put(entry.getValue().get("ac:FatacId")+"", uri);
							caseFile.put("ac:isWorks", uri);
						}
						//log.debug("Uploaded.");
						
						//log.debug("Uploading specific events...");
						it = specificEvents.entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry<String, CustomMap> entry = it.next();
							String uri = uploadObject(entry.getValue());
							if (entry.getValue().get("ac:FatacId")!=null) realIds.put(entry.getValue().get("ac:FatacId")+"", uri);
							event.put("ac:hasSpecificActivity", uri);
						}
						//log.debug("Uploaded.");
						
						//log.debug("Uploading event...");
						String eventUri = uploadObject(event);
						caseFile.put("ac:references", eventUri);
						realIds.put((String)event.get("ac:FatacId"), eventUri);
						String caseFileUri = uploadObject(caseFile);
						//log.debug("Uploaded.");
						
						if (idExpedient!=null) {
							objectExpedient.put(caseFileUri, idExpedient);
						}
						if (event.get("ac:Title")!=null) {
							if (event.get("ac:Title") instanceof String)
								eventExpedient.put(caseFileUri, event.get("ac:Title")+"");
							else {
								String[] titles = (String[])event.get("ac:Title");
								for (String t : titles)	eventExpedient.put(caseFileUri, t);
							}
						}
			    	} catch (Exception e) {
			    		log.debug("ERROR with event " + object.toString() + "\n");
			    		log.error("",e);
			    	}
			    }
			}
		} catch (Exception e) {
			log.debug("Error " + e);
			log.error("",e);
		}
	}
	
	private static Map<String, String> addedOrganizations = new HashMap<String, String>();
	
	public static void migrarPublications() {
		log.debug(" ======================== MIGRACIO PUBLICATIONS ========================== ");
		
		try {
			// migrar publications
			if (DOWNLOAD_DATA) {
				//log.debug("Downloading publications rdf data...");
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
						publication.put("type", "ac:Publication");
						
						String fatacId = url.substring(url.indexOf("id=")+3);
						publication.put("ac:FatacId", fatacId );
						//log.debug("FatacId " + fatacId);
						
						//log.debug("Reading publication...");
						
						String editorLastId = null;
						Map<String, String> currEditor = null;
						CustomList editorsList = new CustomList();
						
						String distrLastId = null;
						Map<String, String> currDistr = null;
						CustomList distributorsList = new CustomList();

						while(it2.hasNext()) {
							Statement stmt1 = it2.next();
							//log.debug(stmt1);
							
							Resource subject1 = stmt1.getSubject();
						    Property predicate1 = stmt1.getPredicate();   	// get the predicate
						    RDFNode object1   = stmt1.getObject();      	// get the object
						    
						    String subjectURI = subject1.toString();
						    String predicateURI = predicate1.toString();
						    String value = object1.toString();
						    
						    if (subjectURI.contains("publications") || value.startsWith("ISBN")) {
						    	if (predicateURI.equals("http://purl.org/dc/elements/1.1/title")) {
									publication.put("ac:Title", value);
									publication.put("about", object1.asLiteral().getString());
								} else if (predicateURI.equals("http://purl.org/dc/elements/1.1/description")) {
									publication.put("ac:Description", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/cover")) {
									if ("soft".equals(value)) value = "paperback";
									publication.put("ac:Cover", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/pvp")) {
									publication.put("ac:pvp", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/pvp_friends")) {
									publication.put("ac:pvp_friends ", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/internal_comments")) {
									publication.put("ac:internal_comments", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/disponible")) {
									publication.put("ac:disponible", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/internal_ref")) {
									publication.put("ac:internal_ref", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/special_offer")) {
									publication.put("ac:special_offer", value);
								} else if (value.startsWith("ISBN")) {
									publication.put("ac:ISBN", value.substring(5));
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/measurements")) {
									publication.put("ac:measurement", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/color")) {
									publication.put("ac:ColorIllustrations", value);
								} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/pages_num")) {
									publication.put("ac:NumberofPages", value);
								} else if (predicateURI.equals("http://purl.org/dc/elements/1.1/date")) {
									publication.put("ac:Date", value);
								}

						    } else if (subjectURI.contains("editors")) {
						    	if (!subjectURI.equals(editorLastId)) {
						    		if (editorLastId!=null)	editorsList.add(currEditor);
						    		editorLastId = subjectURI;
						    		currEditor = new HashMap<String, String>();
						    		currEditor.put("type", "ac:Organisation");
						    	}
						    	
						    	if (predicateURI.equals("http://purl.org/dc/elements/1.1/date")) {
						    		currEditor.put("ac:date", value);
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
										errors.put("ac:Editor", value);
									}
									StmtIterator it3 = model2.listStatements();
									
									while(it3.hasNext()) {
										Statement stmt2 = it3.next();
										
										Property predicate11 = stmt2.getPredicate();   	// get the predicate
									    RDFNode object11   = stmt2.getObject();      	// get the object
									    
									    String predicateURI1 = predicate11.toString();
									    String value1 = object11.toString();
									    
									    if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/name")) {
									    	currEditor.put("ac:Name", value1.trim());
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/url")) {
									    	currEditor.put("ac:Homepage", value1);
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/location")) {
									    	List<String> l = seekAndGenerateLocations(value1);
									    	if (l.size()>0) {
									    		currEditor.put("ac:isLocatedAt", l.get(0));
									    		publication.put("ac:tookPlaceAt", l.get(0));
									    	}
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/id")) {
									    	currEditor.put("ac:FatacId", value1);
									    }
									}
						    	}
						    } else if (subjectURI.contains("separata")) {
						    	// TODO: migrar separata MANUALMENT ja que només n'hi ha 7
						    } else if (subjectURI.contains("distributors")) {
						    	if (!subjectURI.equals(distrLastId)) {
						    		if (distrLastId!=null)	distributorsList.add(currDistr);
						    		distrLastId = subjectURI;
						    		currDistr = new HashMap<String, String>();
						    		currDistr.put("type", "ac:Organisation");
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
									    	currDistr.put("ac:Name", value1);
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/url")) {
									    	currDistr.put("ac:Homepage", value1);
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/location")) {
									    	List<String> l = seekAndGenerateLocations(value1);
									    	if (l.size()>0) currEditor.put("ac:isLocatedAt", l.get(0));
									    } else if (predicateURI1.equals("http://www.fundaciotapies.org/terms/0.1/id")) {
									    	currDistr.put("ac:FatacId", value1);
									    }
									}
						    	}
						    }
						}
						
						if (editorLastId!=null)	editorsList.add(currEditor);
						if (distrLastId!=null)	distributorsList.add(currDistr);
					
						//log.debug("Uploading publication...");
						String puri = uploadObject(publication);
						realIds.put((String)publication.get("ac:FatacId"), puri);
						//log.debug("Uploaded. ");
						
						//log.debug("Uploading publication editors...");
						for(Map<String, String> e : editorsList) {
							CustomMap role = new CustomMap();
							role.put("type","ac:Editor");
							role.put("ac:appliesOn", puri);
							if (e.get("ac:date")!=null) role.put("ac:Date", e.get("ac:date"));
							String roleuri = uploadObject(role);
							
							String orguri = addedOrganizations.get(e.get("ac:Name"));
							if (orguri==null) {
								e.put("ac:performsRole", roleuri);
								e.put("about", e.get("ac:Name"));
								orguri = uploadObject(new CustomMap(e));
								addedOrganizations.put(e.get("ac:Name"), orguri);
							} else {
								CustomMap org = getObject(orguri);
								org.put("ac:performsRole", roleuri);
								updateObject(orguri, org);
							}
							
							CustomMap publik = getObject(puri);
							publik.put("ac:carriedOutBy", orguri);
							updateObject(puri, publik);
						}
						//log.debug("Uploaded.");
						
						//log.debug("Uploading publication distributors...");
						for(Map<String, String> d : distributorsList) {
							CustomMap role = new CustomMap();
							role.put("type","ac:Publisher");
							role.put("ac:appliesOn", puri);
							String roleuri = uploadObject(role);
							
							String orguri = addedOrganizations.get(d.get("ac:Name"));
							if (orguri==null) {
								d.put("ac:performsRole", roleuri);
								d.put("about", d.get("ac:Name"));
								orguri = uploadObject(new CustomMap(d));
								addedOrganizations.put(d.get("ac:Name"), orguri);
								if (d.get("ac:FatacId")!=null) realIds.put(d.get("ac:FatacId")+"", orguri);
							} else {
								CustomMap org = getObject(orguri);
								org.put("ac:performsRole", roleuri);
								updateObject(orguri, org);
							}
						}
						//log.debug("Uploaded.");
						
			    	} catch (Exception e) {
			    		log.debug("ERROR with publication " + object.toString());
			    		log.error("",e);
			    	}
			    }
			}
		} catch (Exception e) {
			log.debug("Error " + e);
			log.error("",e);
		}
	}
	
	public static String getRealId(String c, String fatacId) {
		try {
			String res = realIds.get(fatacId);
			if (res!=null) return res;
			// Send data
		    URL url = new URL("http://"+hostport+"/ArtsCombinatoriesRest/getRealId?c="+c+"&id="+fatacId);
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
		    
			res = sb.toString();
			if ("".equals(res.trim())) return null;
			return res;
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
			 log.error("",e);
		}
		
		return null;
	}
	
	private static Set<String> participantsNotFound = new TreeSet<String>();
	
	private static void migrarRelation(String type1, String id1, String type2, String id2, String roleId) throws Exception {
		//log.debug("Uploading relation/role...");
		if (type1.equals("ft:event_id_1") && type2.equals("ft:event_id_2")) {
			String sid1 = getRealId("ac:FrameActivity", id1);
			String sid2 = getRealId("ac:FrameActivity", id2);
			String roleName = getRoleName(roleId);
			
			if ("Serie".equals(roleName)) {
				CustomMap event1 = getObject(sid1);
				event1.put("ac:continuesTo", sid2);
				updateObject(sid1, event1);
			}
			
		} else if (type1.equals("ft:participant_id") && type2.equals("ft:event_id")) {
			String sid1 = getRealId("ac:Person", id1);
			String sid2 = getRealId("ac:FrameActivity", id2);
			if (sid2==null) sid2 = getRealId("ac:SpecificActivity", id2);
			String roleName = getRoleName(roleId);
			
			if (roleName.equals("Workshop tutor")) roleName = "Workshop_tutor";
			if (roleName.equals("chair")) roleName = "Chair";
			if (roleName.equals("Director of the project")) roleName = "Director";
			if (roleName.equals("Poet and Novelist")) roleName = "Poet";
			if (roleName.equals("Director of the project in Barcelona")) roleName = "Director";
			if (roleName.contains("Curator")) roleName = "Curator";
			if (roleName.contains("Cataloguer")) roleName = "Cataloguer";
			if (roleName.contains("Collection")) roleName = "Cataloguer";
			
			CustomMap role = new CustomMap();
			role.put("type", "ac:"+roleName);
			role.put("ac:appliesOn", sid2);
			String sid3 = uploadObject(role);
			
			CustomMap person = getObject(sid1);
			person.put("ac:performsRole", sid3);
			updateObject(sid1, person);
			
		} else if (type1.equals("ft:publication_id") && type2.equals("ft:event_id")) {
			String sid1 = getRealId("ac:Publication", id1);
			String sid2 = getRealId("ac:FrameActivity", id2);
			if (sid2==null) sid2 = getRealId("ac:SpecificActivity", id2);
			
			CustomMap pub = getObject(sid1);
			pub.put("ac:isPublicationoftheEvent", sid2);
			updateObject(sid1, pub);
		} else if (type1.equals("ft:publication_id") && type2.equals("ft:participant_id")) {
			String sid1 = getRealId("ac:Person", id2); // és correcte, no tocar
			String sid2 = getRealId("ac:Publication", id1);
			String roleName = getRoleName(roleId);
			
			CustomMap role = new CustomMap();
			role.put("type", "ac:"+roleName);
			role.put("ac:appliesOn", sid2);
			String sid3 = uploadObject(role);
			
			CustomMap person = getObject(sid1);
			person.put("ac:performsRole", sid3);
			updateObject(sid1, person);
		} else if (type1.equals("ft:participant_id_1") && type2.equals("ft:participant_id_2")) {
			String sid1 = getRealId("ac:Person", id1);
			String sid2 = getRealId("ac:Person", id2);
			String roleName = getRoleName(roleId);
			
			CustomMap role = new CustomMap();
			role.put("type", "ac:"+roleName);
			role.put("ac:appliesOn", sid2);
			String sid3 = uploadObject(role);
			
			CustomMap person = getObject(sid1);
			person.put("ac:performsRole", sid3);
			updateObject(sid1, person);
		} else if (type1.equals("ft:separata_id")) {
			// TODO: manualment
		}
		//log.debug("Uploaded...");
	}
	
	private static void migrarRelations() {
		log.debug(" ======================== MIGRACIO RELACIONS ========================== ");
		
		try {
			// migrar relations
			if (DOWNLOAD_DATA) {
				//log.debug("Downloading relations rdf data...");
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
					
					//log.debug(params);
					try {
						migrarRelation(params.get(0), params.get(1), params.get(2), params.get(3), params.get(5));
					} catch (Exception e) {
						log.debug("WARNING: No s'ha pogut migrar la relació: (" + params.get(0) + " " + params.get(1) + ") (" + params.get(2) + " " + params.get(3) + ") " + params.get(5));
						//log.error("",e);
					}
			    }
			}
		 } catch (Exception e) {
			 log.error("",e);
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
			if (idx>-1) pt.add((100000+idx+"").substring(1)+"____"+r.get(0)+"___"+r.get(1));
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
		log.debug(" ======================== MIGRACIO FILEMAKER 1 ========================== ");
		
		try {
			CsvReader r = new CsvReader(new FileReader(new File("./fm/CF-fitxer-videos-03.csv")));
			r.readHeaders();
			
			String lastTitle = null;
			String caseFileUri = null;
			
			Set<String> linkedExpos = null;
			Set<String> prodExpoPair = null;
			Set<String> realExpoPair = null;
			CustomMap media = null;
			String[] mfid = {null, null};
			boolean isExpoSet = false;
			boolean isProdSet = false;
			boolean isRealSet = false;
			List<String> agentProductor = new ArrayList<String>();
			List<String> agentRealitzador = new ArrayList<String>();
			
			while (r.readRecord()) {
				
				String eventUri = null;
				
				if (!r.get("títol").equals(lastTitle)) {
					if (media!=null) {
						String mediaUri = uploadObject(media);
						
						if (mfid[0]!=null) {
							mfid[1] = mediaUri;
							mediaFileId.add(mfid);
							mfid = new String[]{null, null};
						}
						
						if (caseFileUri!=null) {
							CustomMap caseFile = getObject(caseFileUri);
							caseFile.put("ac:hasMedia", mediaUri);
							updateObject(caseFileUri, caseFile);
							eventUri = caseFile.get("ac:references")+"";
						}
						
						if (eventUri!=null) {
							for(String id : agentProductor) {
								CustomMap role = new CustomMap();
								role.put("type", "ac:Producer");
								role.put("ac:appliesOn", eventUri);
								String roleUri = uploadObject(role);
								
								CustomMap prod = getObject(id);
								prod.put("ac:performsRole", roleUri);
								updateObject(id, prod);
							}
							
							for(String id : agentRealitzador) {
								CustomMap role = new CustomMap();
								role.put("type", "ac:Film-maker");
								role.put("ac:appliesOn", eventUri);
								String roleUri = uploadObject(role);
								
								CustomMap prod = getObject(id);
								prod.put("ac:performsRole", roleUri);
								updateObject(id, prod);
							}
						}
					}
					
					lastTitle = r.get("títol");
					linkedExpos = new TreeSet<String>();
					prodExpoPair = new TreeSet<String>();
					realExpoPair = new TreeSet<String>();
					caseFileUri = null;
					media = new CustomMap();
					media.put("type", "ac:Video");
					isExpoSet = false;
					isProdSet = false;
					isRealSet = false;
					agentProductor = new ArrayList<String>();
					agentRealitzador = new ArrayList<String>();
				}
				
				
				
				if (r.get("any")!=null && !"".equals(r.get("any"))) {
					if (media.get("ac:StartDate")==null) media.put("ac:StartDate", r.get("any"));
				}
				
				if (r.get("Codi arxiu digital")!=null && !"".equals(r.get("Codi arxiu digital"))) {
					mfid[0] = r.get("Codi arxiu digital").trim();
				}
				
				if (r.get("duració")!=null && !"".equals(r.get("duració"))) {
					media.put("ac:extent", r.get("duració").trim());
				}
				
				if (r.get("núm. inventari")!=null && !"".equals(r.get("núm. inventari"))) {
					media.put("ac:InventoryNumber", r.get("núm. inventari"));
				} 
				
				if (r.get("observacions")!=null && !"".equals(r.get("observacions"))) {
					media.put("ac:Description", r.get("observacions"));
				}
				
				if (r.get("exposició")!=null && !"".equals(r.get("exposició"))) {
					if (!isExpoSet) {
						String expo = r.get("exposició").trim().toLowerCase().replaceAll("[\\s\\t]+", "");
						int idx = r.get("exposició").indexOf("(");
						if (idx!=-1) expo = expo.substring(0, idx).trim();
						
						if (!linkedExpos.contains(expo)) {
							linkedExpos.add(expo);
	
							Set<Map.Entry<String, String>> roll = eventExpedient.entrySet();
							for(Map.Entry<String, String> p : roll) {
								String eventTitle = p.getValue().toLowerCase().replaceAll("[\\s\\t]+", "");
								//log.debug(eventTitle + " " + expo);
								if (eventTitle.contains(expo)){
									caseFileUri = p.getKey();
									//log.debug("HERE!!!!!!!!");
									break;
								}
							}
							
							if (caseFileUri==null) {
								log.debug("WARNING: No s'ha pogut trobar l'Event '"+r.get("exposició").trim()+"', caldrà relacionar-lo manualment");
							}
						}
						isExpoSet = true;
					}
				}
				
				if (r.get("productor")!=null && !"".equals(r.get("productor"))) {
					if (!isProdSet) {
						String currProductor = r.get("productor").trim();
						String[] pl = searchAgent(currProductor);
					
						if (pl!=null) {
							for(String panoramix : pl) {
								int idx = panoramix.indexOf("___");
								String p = panoramix.substring(0, idx);
								String t = panoramix.substring(idx+3);
								
								String uri = null;
								uri = backupAgents.get(p);
								if (uri==null) {
									List<String> l = new ArrayList<String>();
									if ("p".equals(t)) {
										l = find("ac:givenName", p, "ac:Person");
										if (l.size()==0) l = find("ac:givenName", p.split(" ")[0], "ac:Person");
									} else {
										l = find("ac:Name", p, "ac:Organisation");
									}
									
									if (l.size()==0) {
										CustomMap agent = new CustomMap();
										if ("p".equals(t)) {
											String[] fname = p.split(" ");
											agent.put("type", "ac:Person");
											agent.put("ac:givenName", fname[0]);
											if (fname.length>1)	agent.put("ac:familyName", p.replace(fname[0]+" ", ""));
											String fullName = agent.get("ac:givenName") + (agent.get("ac:familyName")!=null?" "+agent.get("ac:familyName"):"");
											agent.put("about", fullName);
											agent.put("ac:Name", fullName);
										} else {
											agent.put("type", "ac:Organisation");
											agent.put("ac:Name", p);
											agent.put("about", p);
										}
										
										uri = uploadObject(agent);
										backupAgents.put(p, uri);
									} else {
										uri = l.get(0); 
									}
								}
								
								if (eventUri != null && !prodExpoPair.contains(uri+eventUri)) {
									agentProductor.add(uri);
									prodExpoPair.add(uri+eventUri);
								}
							}
						}
						isProdSet = true;
					}
				} 
				
				if (r.get("Realització")!=null && !"".equals(r.get("Realització"))) {
					if (!isRealSet) {
						String currRealitzador = r.get("Realització").trim();
						String[] pl = searchAgent(currRealitzador); 
						
						if (pl!=null) {
							for(String panoramix : pl) {
								int idx = panoramix.indexOf("___");
								String p = panoramix.substring(0, idx);
								String t = panoramix.substring(idx+3);
								
								String uri = null;
								uri = backupAgents.get(p);
								
								if (uri==null) {
									List<String> l = new ArrayList<String>();
									if ("p".equals(t)) {
										l = find("ac:givenName", p, "ac:Person");
										if (l.size()==0) l = find("ac:givenName", p.split(" ")[0], "ac:Person");
									} else {
										l = find("ac:Name", p, "ac:Organisation");
									}
	
									if (l.size()==0) {
										CustomMap agent = new CustomMap();
										if ("p".equals(t)) {
											String[] fname = p.split(" ");
											agent.put("type", "ac:Person");
											agent.put("ac:givenName", fname[0]);
											if (fname.length>1)	agent.put("ac:familyName", p.replace(fname[0]+" ", ""));
											String fullName = agent.get("ac:givenName") + (agent.get("ac:familyName")!=null?" "+agent.get("ac:familyName"):"");
											agent.put("about", fullName);
											agent.put("ac:Name", fullName);
										} else {
											agent.put("type", "ac:Organisation");
											agent.put("ac:Name", p);
											agent.put("about", p);
										}	 
										
										uri = uploadObject(agent);
										 backupAgents.put(p, uri);
									} else {
										uri = l.get(0); 
									}
								}
								
								if (eventUri != null && !realExpoPair.contains(uri+eventUri)) {
									agentRealitzador.add(uri);
									realExpoPair.add(uri+eventUri);
								}
							}
						}
						isRealSet = true;
					}
				}
				
				if (r.get("so")!=null && !"".equals(r.get("so"))) {
					String v = r.get("so");
					if ("sense so".equals(v) || "sin sonido".equals(v)) {
						media.put("ac:Mute", "true");
					}
					if (v.contains("català") || v.contains("catalán") ) {
						media.put("ac:hasLanguage", "Catala"); 
					}
					if (v.contains("español") || v.contains("castellano") || v.contains("castellà")) {
						media.put("ac:hasLanguage", "Espanyol");
					}
					if (v.contains("english") || v.contains("anglès") || v.contains("inglés")) {
						media.put("ac:hasLanguage", "Angles"); 
					}
				} 
				
				if (r.get("suport")!=null && !"".equals(r.get("suport"))) {
					media.put("ac:OriginalSource", r.get("suport"));
				} 
				
				if (r.get("tipus")!=null && !"".equals(r.get("tipus"))) {
					String v = r.get("tipus");
					if (v.contains("color") || v.contains("Color")) media.put("ac:ColorMode","Color");
					if (v.contains("Blanc i negre") || v.contains("b/n") || v.contains("blanco-negro")) media.put("ac:ColorMode", "B/N");
				} 
				
				if (r.get("títol")!=null && !"".equals(r.get("títol"))) {
					//if (caseFile.get("Description")==null)
					//	caseFile.put("Description", r.get("títol"));
					if (media.get("ac:Title")==null) {
						media.put("ac:Title", r.get("títol"));
						media.put("about", r.get("títol"));
					}
				}
				
				if (eventUri!=null) {
					for(String id : agentProductor) {
						CustomMap role = new CustomMap();
						role.put("type", "ac:Producer");
						role.put("ac:appliesOn", eventUri);
						String roleUri = uploadObject(role);
						
						CustomMap prod = getObject(id);
						prod.put("ac:performsRole", roleUri);
						updateObject(id, prod);
					}
					
					for(String id : agentRealitzador) {
						CustomMap role = new CustomMap();
						role.put("type", "ac:Film-maker");
						role.put("ac:appliesOn", eventUri);
						String roleUri = uploadObject(role);
						
						CustomMap prod = getObject(id);
						prod.put("ac:performsRole", roleUri);
						updateObject(id, prod);
					}
				}
			}
			
			if (media!=null) {
				String mediaUri = uploadObject(media);
				
				if (mfid[0]!=null) {
					mfid[1] = mediaUri;
					mediaFileId.add(mfid);
				}
				
				if (caseFileUri!=null) {
					CustomMap caseFile = getObject(caseFileUri);
					caseFile.put("ac:hasMedia", mediaUri);
					updateObject(caseFileUri, caseFile);
				}
			}
			
			r.close();
		} catch (Exception e) {
			log.error("",e);
		}
	}
	
	private static void migrarFileMaker2() {
		log.debug(" ======================== MIGRACIO FILEMAKER 2 ========================== ");
		
		try {
			CsvReader r = new CsvReader(new FileReader(new File("./fm/CF-fitxers-conferencies.csv")));
			r.readHeaders();

			String lastTitle = null;
			
			Set<String> linkedExpos = null;
			Set<String> prodExpoPair = null;
			Set<String> autorExpoPair = null;
			String caseFileUri = null;
			CustomMap media = null;
			String[] mfid = {null, null};
			List<String> agentAutor = null;
			List<String> agentProductor = null;
			
			boolean isAutorSet = false;
			boolean isExpoSet = false;
			
			while (r.readRecord()) {
				String eventUri = null;
				
				if (!r.get("títol").equals(lastTitle)) {
					if (media!=null) {
						String mediaUri = uploadObject(media);
						if (mfid[0]!=null) {					
							mfid[1] = mediaUri;
							mediaFileId.add(mfid);
							mfid = new String[]{null, null};
						}
						if (caseFileUri!=null) {
							CustomMap caseFile = getObject(caseFileUri);
							caseFile.put("ac:hasMedia", mediaUri);
							updateObject(caseFileUri, caseFile);
							eventUri = caseFile.get("ac:references")+"";
						}
						
						if (eventUri!=null) {
							for(String id : agentProductor) {
								CustomMap role = new CustomMap();
								role.put("type", "ac:Producer");
								role.put("ac:appliesOn", eventUri);
								String roleUri = uploadObject(role);
								
								CustomMap prod = getObject(id);
								prod.put("ac:performsRole", roleUri);
								updateObject(id, prod);
							}
							
							for(String id : agentAutor) {
								CustomMap role = new CustomMap();
								role.put("type", "ac:Lecturer");
								role.put("ac:appliesOn", eventUri);
								String roleUri = uploadObject(role);
								
								CustomMap autor = getObject(id);					
								autor.put("ac:performsRole", roleUri);
								updateObject(id, autor);
							}
						}
					}
					
					lastTitle = r.get("títol");
					
					linkedExpos = new TreeSet<String>();
					prodExpoPair = new TreeSet<String>();
					autorExpoPair = new TreeSet<String>();
					caseFileUri = null;
					media = new CustomMap();
					media.put("type", "ac:Audio");
					isAutorSet = false;
					isExpoSet = false;
					agentAutor = new ArrayList<String>();
					agentProductor = new ArrayList<String>();
				}
				
				
				if (r.get("exposició")!=null && !"".equals(r.get("exposició"))) {
					if (!isExpoSet) {
						String expo = r.get("exposició").trim().toLowerCase().replaceAll("[\\s\\t]+", "");
						int idx = r.get("exposició").indexOf("(");
						if (idx!=-1) expo = expo.substring(0, idx).trim();
						
						if (!linkedExpos.contains(expo)) {
							linkedExpos.add(expo);
	
							Set<Map.Entry<String, String>> roll = eventExpedient.entrySet();
							for(Map.Entry<String, String> p : roll) {
								String eventTitle = p.getValue().trim().toLowerCase().replaceAll("[\\s\\t]+", "");
								if (eventTitle.contains(expo)){
									caseFileUri = p.getKey();
									break;
								}
							}
							
							if (caseFileUri==null) {
								log.debug("WARNING: No s'ha pogut trobar l'Event '"+r.get("exposició").trim()+"', caldrà relacionar-lo manualment");
							}
						}
						
						isExpoSet = true;
					}
				} 
				
				if (r.get("autor")!=null && !"".equals(r.get("autor"))) {
					if (!isAutorSet) {
						String prodNames = r.get("autor").trim();
						String[] pl = prodNames.split(",");
						String prodTypes = r.get("persona/organització");
						String[] ptl = prodTypes.split(",");
						
						int i = 0;
						for(String p : pl) {
							p = p.trim();
							String pt = "ac:Person";
							if (ptl.length==1 && "o".equals(ptl[0]) 
								|| ptl.length>i && "o".equals(ptl[i].trim())) pt = "ac:Organisation";
							
							String uri = null;
							uri = backupAgents.get(p);
							if (uri==null) {
								List<String> l = find("ac:Person".equals(pt)?"ac:givenName":"ac:Name", p, pt);
								
								if (l.size()==0) {
									CustomMap agent = new CustomMap();
									
									agent.put("type", pt);
									if ("ac:Person".equals(pt)) agent.put("ac:givenName", p);
									agent.put("about", p);
									agent.put("ac:Name", p);
									uri = uploadObject(agent);
									backupAgents.put(p, uri);
								} else {
									uri = l.get(0); 
								}
							}
							
							if (eventUri != null && !autorExpoPair.contains(uri+eventUri)) {
								agentAutor.add(uri);
								autorExpoPair.add(uri+eventUri);
							}
	
							i++;
						}
						
						isAutorSet = true;
					}
				}
				
				if (r.get("durada")!=null && !"".equals(r.get("durada"))) {
					media.put("ac:extent", r.get("durada"));
				}
				
				if (r.get("any")!=null && !"".equals(r.get("any"))) {
					String any = r.get("any");
					media.put("ac:created", any);
				}
				
				if (r.get("contingut")!=null && !"".equals(r.get("contingut"))) {
					if (media.get("ac:Description") == null) media.put("ac:Description", r.get("contingut"));
				} 
				
				if (r.get("Codi arxiu digital")!=null && !"".equals(r.get("Codi arxiu digital"))) {
					if (mfid[0]==null)
						mfid[0] = r.get("Codi arxiu digital").trim();
					else
						mfid[0] += " " + r.get("Codi arxiu digital").trim();
				}
				
				if (r.get("format")!=null && !"".equals(r.get("format"))) {
					media.put("ac:OriginalSource", r.get("format").trim());
				} 
				
				if (r.get("Idioma")!=null && !"".equals(r.get("Idioma"))) {
					String v = r.get("Idioma");
					if (v.contains("català") || v.contains("catalán") ) {
						media.put("ac:hasLanguage", "Catala"); 
					} 
					if (v.contains("español") || v.contains("castellano") || v.contains("castellà")) {
						media.put("ac:hasLanguage", "Espanyol");
					} 
					if (v.contains("english") || v.contains("anglès") || v.contains("inglés")) {
						media.put("ac:hasLanguage", "Angles"); 
					}
					if (v.contains("àrab") || v.contains("árabe") || v.contains("arabic")) {
						media.put("ac:hasLanguage", "Arab");
					}
					if (v.contains("francès") || v.contains("francés") || v.contains("french")) {
						media.put("ac:hasLanguage", "Frances"); 
					}
					if (v.contains("alemany") || v.contains("german") || v.contains("alemán")) {
						media.put("ac:hasLanguage", "Alemany"); 
					}
					if (v.contains("italià") || v.contains("italian") || v.contains("italiano")) {
						media.put("ac:hasLanguage", "Italia"); 
					}
				} 
				
				if (r.get("núm. inv.")!=null && !"".equals(r.get("núm. inv."))) {
					media.put("ac:InventoryNumber", r.get("núm. inv."));
				}

				if (r.get("títol")!=null && !"".equals(r.get("títol"))) {
					if (media.get("ac:Title")==null) {
						media.put("ac:Title", r.get("títol"));
						media.put("about", r.get("títol"));
					}
				}
			}
			
			String eventUri = null;
			
			if (media!=null) {
				String mediaUri = uploadObject(media);
				if (mfid[0]!=null) {					
					mfid[1] = mediaUri;
					mediaFileId.add(mfid);
					mfid = new String[]{null, null};
				}
				if (caseFileUri!=null) {
					CustomMap caseFile = getObject(caseFileUri);
					caseFile.put("ac:hasMedia", mediaUri);
					updateObject(caseFileUri, caseFile);
					eventUri = caseFile.get("ac:references")+"";
				}
				
				if (eventUri!=null) {
					for(String id : agentProductor) {
						CustomMap role = new CustomMap();
						role.put("type", "ac:Producer");
						role.put("ac:appliesOn", eventUri);
						String roleUri = uploadObject(role);
						
						CustomMap prod = getObject(id);
						prod.put("ac:performsRole", roleUri);
						updateObject(id, prod);
					}
					
					for(String id : agentAutor) {
						CustomMap role = new CustomMap();
						role.put("type", "ac:Lecturer");
						role.put("ac:appliesOn", eventUri);
						String roleUri = uploadObject(role);
						
						CustomMap autor = getObject(id);					
						autor.put("ac:performsRole", roleUri);
						updateObject(id, autor);
					}
				}
			}
			
			r.close();
		} catch (Exception e) {
			log.error("",e);
		}
	}
	
	private static CustomMap imagesExpedient = new CustomMap();
	private static Map<String, String> workExpedient = new HashMap<String, String>();
	
	private static void migrarFileMaker3() {
		log.debug(" ======================== MIGRACIO FILEMAKER 3 ========================== ");
		
		try {
			CsvReader r = new CsvReader(new FileReader(new File("./fm/SF-MASER-COLLECIO.csv")));
			r.readHeaders();
			
			String lastWork = null;
			String currentWork = null;
			CustomMap work = null;
			List<String> editorList = null;
			String[] nt = null;
			
			while (r.readRecord()) {
				if (r.get("Títol")!=null && !"".equals(r.get("Títol"))) {
					currentWork = r.get("Títol").trim();
					if (!currentWork.equals(lastWork)) {
						if (lastWork!=null) {
							work.put("ac:Title", lastWork+"@ca");
							work.put("about", lastWork);
							work.put("type", "ac:AT_Work_FAT_Collection");
							
							String wri = uploadObject(work);
							
							if (nt!=null) {
								CustomMap exp = new CustomMap();
								exp.put("type", "ac:ProtectionPromotionAT");
								exp.put("about", "Expedient " + lastWork);
								exp.put("ac:references", wri);
								String expUri = uploadObject(exp);
								
								for (String n : nt) imagesExpedient.put(expUri, n);
								workExpedient.put(expUri, wri);
							}
						}
						
						work = new CustomMap();
						editorList = new ArrayList<String>();
						nt  = null;
						lastWork = currentWork;
					}
				}
				
				if (r.get("Núm. fotog.")!=null && !"".equals(r.get("Núm. fotog."))) {
					nt = r.get("Núm. fotog.").split("[\\s\\n\\/]+");
					for (String n : nt)	work.put("ac:NumberT", nt);
				}
				if (r.get("Nombre d'exemplars")!=null && !"".equals(r.get("Nombre d'exemplars"))) {
					work.put("ac:NumberofCopies", r.get("Nombre d'exemplars"));
				}
				if (r.get("Any")!=null && !"".equals(r.get("Any"))) {
					work.put("ac:Date", r.get("Any"));
				}
				if (r.get("Bibliografia")!=null && !"".equals(r.get("Bibliografia"))) {
					String[] bib = r.get("Bibliografia").split("\n");
					for (String b : bib) work.put("ac:Bibliography", b);
				}
				if (r.get("Descripció de l'objecte")!=null && !"".equals(r.get("Descripció de l'objecte"))) {
					work.put("ac:Description", r.get("Descripció de l'objecte"));
				}
				if (r.get("Estat de conservació")!=null && !"".equals(r.get("Estat de conservació"))) {
					work.put("ac:Conservation", r.get("Estat de conservació"));
				}
				if (r.get("Exposicions")!=null && !"".equals(r.get("Exposicions"))) {
					// TODO: pocs registres i poc automatitzable: MANUALMENT
				}
				if (r.get("Localització")!=null && !"".equals(r.get("Localització"))) {
					String c = r.get("Localització");
					if (r.get("updated loc.")!=null && !"".equals(r.get("updated loc."))) {
						c = c + " " + r.get("updated loc.");
					}
					work.put("ac:CurrentSituation", c);
				}
				if (r.get("Mides")!=null && !"".equals(r.get("Mides"))) {
					work.put("ac:measurement", r.get("Mides").trim());
				}
				if (r.get("obra gràfica =")!=null && !"".equals(r.get("obra gràfica ="))) {
					work.put("ac:hasType", "Grafica");
				}
				if (r.get("obra original =")!=null && !"".equals(r.get("obra original ="))) {
					work.put("ac:hasType", "Original");
				}
				if (r.get("Núm. Accés")!=null && !"".equals(r.get("Núm. Accés"))) {
					work.put("ac:RegisterNumber", r.get("Núm. Accés").trim());
				}
				if (r.get("préstec a :")!=null && !"".equals(r.get("préstec a :"))) {
					// TODO: pocs registres i poc automatitzable: MANUALMENT
				}
				if (r.get("Tècnica")!=null && !"".equals(r.get("Tècnica"))) {
					work.put("ac:Technique", r.get("Tècnica")+"@ca");
				}
				if (r.get("técnica")!=null && !"".equals(r.get("técnica"))) {
					work.put("ac:Technique", r.get("técnica")+"@es");
				} else if (r.get("trad.castellano técnica:")!=null && !"".equals(r.get("trad.castellano técnica:"))) {
					work.put("ac:Technique", r.get("trad.castellano técnica:").trim()+"@es");
				}
				if (r.get("trad.english technique:")!=null && !"".equals(r.get("trad.english technique:"))) {
					work.put("ac:Technique", r.get("trad.english technique:").trim()+"@en");
				}
				if (r.get("trad.français technique:")!=null && !"".equals(r.get("trad.français technique:"))) {
					work.put("ac:Technique", r.get("trad.français technique:").trim()+"@fr");
				}
				if (r.get("trad. castellano")!=null && !"".equals(r.get("trad. castellano"))) {
					work.put("ac:Title", r.get("trad. castellano").trim()+"@es");
				}
				if (r.get("trad. english")!=null && !"".equals(r.get("trad. english"))) {
					work.put("ac:Title", r.get("trad. english").trim()+"@en");
				}
				if (r.get("trad. français")!=null && !"".equals(r.get("trad. français"))) {
					work.put("ac:Title", r.get("trad. français").trim()+"@fr");
				}
				if (r.get("Valoració Econòmica en €")!=null && !"".equals(r.get("Valoració Econòmica en €"))) {
					Double d = new Double(r.get("Valoració Econòmica en €").replace(',', '.'));
					work.put("ac:EstimatedValue", Math.round(Math.abs(d)));
				}
				if (r.get("Edició")!=null && !"".equals(r.get("Edició"))) {
					CustomMap pub = new CustomMap();
					pub.put("type", "ac:Publication");
					String editorUri = null;
					
					if (r.get("Impressor")!=null && !"".equals(r.get("Impressor"))) {
						if (r.get("Mides planxa")!=null && !"".equals(r.get("Mides planxa"))) {
							pub.put("ac:SheetSize", r.get("Mides planxa").trim());
						}
						if (r.get("Tiratge")!=null && !"".equals(r.get("Tiratge"))) {
							pub.put("ac:Circulation", r.get("Tiratge").trim());
						}
						if (r.get("Núm. planxes")!=null && !"".equals(r.get("Núm. planxes"))) {
							pub.put("ac:SheetNumber", r.get("Núm. planxes").trim());
						}
						if (r.get("Filigrana")!=null && !"".equals(r.get("Filigrana"))) {
							pub.put("ac:Watermark", r.get("Filigrana").trim());
						}
					}
					
					if (r.get("Editor")!=null && !"".equals(r.get("Editor"))) {
						String[] editors = searchAgent(r.get("Editor"));
						
						if (editors!=null) {
							for(String panoramix : editors) {
								int idx = panoramix.indexOf("___");
								String p = panoramix.substring(0, idx);
								
								editorUri = backupAgents.get(p);
								
								if (editorUri==null) {
									List<String> l = find("ac:Name", p, "ac:Organisation");
									
									if (l.size()==0) {
										CustomMap agent = new CustomMap();
										agent.put("type", "ac:Organisation");
										agent.put("ac:Name", p);
										agent.put("about", p);
										editorUri = uploadObject(agent);
										backupAgents.put(p, editorUri);
									} else {
										editorUri = l.get(0); 
									}	
								}
							}
						}
					}
					
					String puburi = uploadObject(pub);
					
					CustomMap role = new CustomMap();
					role.put("type", "ac:Editor");
					role.put("ac:appliesOn", puburi);
					String roleUri = uploadObject(role);
					
					CustomMap editor = getObject(editorUri);
					editor.put("ac:performsRole", roleUri);
					updateObject(editorUri, editor);
				}
			}
			
			if (lastWork!=null) {
				work.put("ac:Title", currentWork+"@ca");
				work.put("about", currentWork);
				work.put("type", "ac:AT_Work_FAT_Collection");
				
				String wri = uploadObject(work);
				
				if (nt!=null) {
					CustomMap exp = new CustomMap();
					exp.put("type", "ac:ProtectionPromotionAT");
					exp.put("ac:references", wri);
					String expUri = uploadObject(exp);
					
					for (String n : nt) imagesExpedient.put(expUri, n);
					workExpedient.put(expUri, wri);
				}
			}
			
			r.close();
		} catch (Exception e) {
			log.error("",e);
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
	
	private static void afegirACollection(String objectId, String codi, String uri) throws Exception {
		int idx = codi.indexOf("C0");
		String codiCol = codi.substring(idx,idx+4);
		for (String[] c : collectionList) {
			if (c[0].equals(codiCol)) {
				CustomMap media = getObject(objectId);
				media.put("ac:isCollectedIn", c[2]);
				media.put("ac:Uri", uri);
				updateObject(objectId, media);
				break;
			}
		}
	}
	
	
	private static void migrarMedia() {
		log.debug(" ======================== MIGRACIO AUDIO/VIDEO ========================== ");
		
		List<String> llistaFitxersMedia = getMediaList(directori_medias);
		
		try {
			for (String[] dup : mediaFileId) {
				String fileId = dup[0];
				String objectId = dup[1];
				
				String[] fileIds = fileId.split(" ");
				for (String fid : fileIds) {
					for (String fn : llistaFitxersMedia) {
						if (fn.contains(fid.substring(0, 4)) && fn.contains(fid.substring(12))) {
							String uri = uploadObjectFile(fn); 
							afegirACollection(objectId, fn, uri);
							break;
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("",e);
		}
	}
	
	private static void putImageData(CustomMap image, String fn, String fn2) {
		if (fn2.startsWith("FF1")) {
			image.put("ac:OriginalSource", "Paper");
		} else if (fn2.startsWith("FF2B")) {
			image.put("ac:OriginalSource", "Positiu 135 mm b/n");
		} else if (fn2.startsWith("FF2C")) {
			image.put("ac:OriginalSource", "Positiu 135 mm col.");
		} else if (fn2.startsWith("FF3B")) {
			image.put("ac:OriginalSource", "Negatiu 135 mm b/n");
		} else if (fn2.startsWith("FF3C")) {
			image.put("ac:OriginalSource", "Negatiu 135 mm col.");
		} else if (fn2.startsWith("FF4B")) {
			image.put("ac:OriginalSource", "Positiu 120 mm b/n");
		} else if (fn2.startsWith("FF4C")) {
			image.put("ac:OriginalSource", "Positiu 120 mm col.");
		} else if (fn2.startsWith("FF5B")) {
			image.put("ac:OriginalSource", "Negatiu 120 mm b/n");
		} else if (fn2.startsWith("FF5C")) {
			image.put("ac:OriginalSource", "Negatiu 120 mm col.");
		} else if (fn2.startsWith("FF6B")) {
			image.put("ac:OriginalSource", "Positiu Placa b/n");
		} else if (fn2.startsWith("FF6C")) {
			image.put("ac:OriginalSource", "Positiu Placa col.");
		} else if (fn2.startsWith("FF7A")) {
			image.put("ac:OriginalSource", "Negatiu Placa b/n");
		} else if (fn2.startsWith("FF7B")) {
			image.put("ac:OriginalSource", "Negatiu Placa col·");
		} else if (fn2.startsWith("FF8")) {
			image.put("ac:OriginalSource", "Digital");
		}
		
		image.put("ac:format",fn2.substring(fn2.length()-3).toLowerCase());
		
		int idx = fn2.indexOf("C0");
		if (idx!=-1) {
			String codiCol = fn2.substring(idx,idx+4);
			for (String[] c : collectionList) {
				if (c[0].equals(codiCol)) {
					image.put("ac:isCollectedIn", c[2]);
					break;
				}
			}
		}
	}
	
	private static void migrarImages() {
		log.debug(" ======================== MIGRACIO IMAGES ========================== ");
		
		List<String> llistaFitxersMedia = getMediaList(directori_medias);
		
		try {
			for (String fileName : llistaFitxersMedia) {
				Set<Map.Entry<String, String>> ent = objectExpedient.entrySet();
				int idx = -1;
				int idx2 = -1;
				
				for (Map.Entry<String, String> e : ent) {
					String uriExp = e.getKey();
					String codiExp = e.getValue();
					
					idx = fileName.indexOf("/"+codiExp+"_FF");
					idx2 = fileName.indexOf("/"+codiExp+"_");
					if (idx!=-1) {
						CustomMap image = new CustomMap();
						image.put("type", "ac:Image");
						String fn2 = fileName.substring(idx);
						
						putImageData(image, fileName, fn2);
						
						String fileuri = uploadObjectFile(fileName);
						
						image.put("ac:Uri", fileuri);
						String imageuri = uploadObject(image);
						
						CustomMap exp = getObject(uriExp);
						exp.put("ac:hasMedia", imageuri);
						updateObject(uriExp, exp);
						
						break;
					} else if (idx2!=-1) {
						CustomMap media = new CustomMap();
						String fn2 = fileName.substring(idx2);
						
						if (fileName.endsWith(".tif") || fileName.endsWith(".jpg")) {
							media.put("type", "ac:Image");
							putImageData(media, fileName, fn2);
						} else if (fileName.endsWith(".pdf") || fileName.endsWith(".doc") || fileName.endsWith(".rtf") || fileName.endsWith(".odt")) {
							media.put("type", "ac:Text");
						} else continue;
						
						String fileuri = uploadObjectFile(fileName);
						media.put("ac:Uri", fileuri);
						
						String imageuri = uploadObject(media);
						
						CustomMap exp = getObject(uriExp);
						exp.put("ac:hasMedia", imageuri);
						updateObject(uriExp, exp);
						
						break;
					}
				}
				
				if ((idx == -1) && (idx2 == -1)) {
					Set<Map.Entry<String, Object>> ent2 = imagesExpedient.entrySet();
					for (Map.Entry<String, Object> e : ent2) {
						String uriExp = e.getKey();
						Object codiImg = e.getValue();
						
						String[] codis = null;
						
						if (codiImg != null) {
							if (codiImg instanceof String)
								codis = new String[]{ (String)codiImg };
							else 
								codis = (String[])codiImg;
							
							for (String c : codis) {
								if (c.length()<3) continue;
								else if (c.contains("T")) c = c.substring(c.indexOf("T"));
								else if (c.contains("G")) c = c.substring(c.indexOf("G"));
								else continue;
								
								idx = fileName.indexOf("/"+c);
								if (idx!=-1 && !"".equals(c.trim())) {
									CustomMap media = new CustomMap();
									
									if (fileName.endsWith(".tif") || fileName.endsWith(".jpg")) {
										media.put("type", "ac:Image");
									} else if (fileName.endsWith(".pdf") || fileName.endsWith(".doc") || fileName.endsWith(".rtf") || fileName.endsWith(".odt")) {
										media.put("type", "ac:Text");
									} else break;
									
									media.put("ac:format",fileName.substring(fileName.length()-3).toLowerCase());
									
									String fileuri = uploadObjectFile(fileName);
									
									String workUri = workExpedient.get(uriExp);
									if (workUri!=null) media.put("ac:represents", workUri);
									media.put("ac:Uri", fileuri);
									String imageuri = uploadObject(media);
									
									CustomMap exp = getObject(uriExp);
									exp.put("ac:hasMedia", imageuri);
									updateObject(uriExp, exp);
								}
							}
						}
					}
				}
			}
		} catch (Exception e) {
			log.error("",e);
		}
	}
		
	
	private static String[][] collectionList = {
	
		{"C001", "Inauguracions", null, "ac:Opening", "Opening", "Inauguraciones"},
		{"C002", "Conferències i seminaris", null, "ac:LecturesandSeminars", "Lectures and seminars", "Lecturas y seminarios"},
		{"C003", "Roda de premsa", null, "ac:PressReleases", "Press releases", "Rueda de prensa"},
		{"C004", "Sopars", null, "ac:Dinners", "Dinners", "Cenas"},
		{"C005", "Acords institucionals", null, "ac:InstitutionalAgreement", "Institutional agreements", "Acuerdos Institucionales"},
		{"C006", "Emissions i gravacions: TV, ràdio, cinema", null, "ac:TvRadioCinema", "TV radio and cinema recordings and broadcast", "Emisiones y grabaciones: TV, radio, cine"},
		{"C007", "Visites protocol·làries", null, "ac:ProtocolVisits", "Protocol visits", "Visitas protocolarias"},
		{"C008", "Espectacles", null, "ac:Show", "Shows", "Espectáculos"},
		{"C009", "Activitats paral·les", null, "ac:OtherActivities", "Other activities", "Actividades paralelas"},
		{"C010", "Itineràncies", null, "ac:ExhibitionTourings", "Exhibition tourings", "Itinerancias"},
		{"C011", "Fotografies d'instal·lació", null, "ac:PhotoInstallation", "Installation pictures", "Fotografías de instalación"},
		{"C012", "Llibre d'instal·lació", null, "ac:InstallationGuide", "Installation guide", "Guía de instalación"},
		{"C013", "Entrevistes", null, "ac:Interviews", "Interviews", "Entrevistas"},
		{"C014", "Presentacions", null, "ac:PresentationsorLaunchs", "Presentation launchs", "Presentaciones"},
		{"C015", "Tallers i trobades", null, "ac:WorkshopandMeeting", "Workshops and meetings", "Talleres y presentaciones"},
		{"C016", "Obra", null, "ac:Work", "Work", "Obra"},
		{"C017", "Material per premsa", null, "ac:PressKits", "Press kits", "Material de prensa"},
		{"C018", "Foto d'instal·lació Col·lecció EspaiA", null, "ac:PhotoInstallationCollectionSpaceA", "Photo installation collection spaceA", "Foto de instalación Colección espacioA"},
		{"C019", "Servei educatiu taller", null, "ac:EducationalServicesWorkshop", "Educational services workshop", "Servicio educativo taller"},
		{"C020", "Servei educatiu visites dinamitzades", null, "ac:EducationalDynamicVisit", "Educational dynamic visit", "Servicio educativo visitas dinamizadas"},
		{"C021", "Muntatge exposició", null, "ac:Set-upProject", "Set-up project", "Montaje de exposición"},
		{"C022", "Foto instal·lació fora de FAT", null, "ac:PhotoInstallationOutsideFat", "Installation picture outside FAT", "Foto instalacion fuera de FAT"},
		{"C023", "Actuació", null, "ac:Performance", "Performance", "Actuació"},
		{"C024", "Portes obertes", null, "ac:FreeAdmissionDays", "Free admission days", "Puertas abiertas"},
		{"C025", "Clausura", null, "ac:Close", "Close", "Clausura"},
		{"C026", "Curs", null, "ac:Courses", "Courses", "Curso"}
	
	};

	
	private static void migrarCollections() {
		log.debug(" ======================== MIGRACIO COLLECTIONS ========================== ");
		
		try {
			//log.debug("Uploading collections...");
			int idx = 0;
			while(idx<collectionList.length) {
				CustomMap collection = new CustomMap();
				collection.put("type", collectionList[idx][3]);
				collection.put("ac:Title", collectionList[idx][1]+"@ca");
				collection.put("ac:Title", collectionList[idx][4]+"@en");
				collection.put("ac:Title", collectionList[idx][5]+"@es");
				collection.put("about", collectionList[idx][1]);
				String uri = uploadObject(collection);
				collectionList[idx][2] = uri;
				idx++;
			}
			//log.debug("Uploaded.");
		} catch (Exception e) {
			log.error("",e);
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
			log.debug(it.next());
		}
	}
	
	public static void collectCities() throws Exception {
		CsvReader r = new CsvReader(new FileReader(new File("./fm/SF-MASER-COLLECIO.csv")));
		r.readHeaders();
		
		Set<String> cities = new TreeSet<String>();
		while (r.readRecord()) {
			if (r.get("Editor")!=null && !"".equals(r.get("Editor"))) {
				int idx = r.get("Editor").indexOf(",");
				if (idx>-1) cities.add(r.get("Editor").substring(idx+1).trim());
			}
		}
		
		Iterator<String> it = cities.iterator();
		while (it.hasNext()) {
			log.debug(it.next());
		}
	}
	
	private static void migrarDadesFixes() {
		log.debug(" ======================== MIGRACIO GENERICS ========================== ");
		
		try {
		
			String[] languages = {
					"Catala", "Català@ca", "Catalán@es", "Catalan@en",
					"Angles", "Anglès@ca", "Inglés@es", "English@en",
					"Italia", "Italià@ca", "Italiano@es", "Italian@en",
					"Espanyol", "Espanyol@ca", "Español@es", "Spanish@en",
					"Frances", "Francès@ca", "Francés@es", "French@en",
					"Arab", "Àrab@ca", "Árabe@es", "Arabic@en",
					"Alemany", "Alemany@ca", "Alemán@es", "German@en"
			};
			
			String[] kindArtWork = {
					"Dança", "Dança@ca", "Danza@es", "Dance@en",
					"Installacio", "Instal·lació@ca", "Instalación@es", "Installation@en",
					"Actuacio", "Actuació@ca", "Actuación@es", "Performance@en",
					"Fotografia", "Fotografia@ca", "Fotografía@es", "Photo@en",
					"Pintura", "Pintura@ca", "Pintura@es", "Picture@en",
					"Requiem", "Requiem@ca", "Requiem@es", "Requiem@en",
					"Escultura", "Escultura@ca", "Escultura@es", "Sculpture@en",
					"Sonata", "Sonata@ca", "Sonata@es", "Sonata@en",
					"Videoart", "Videoart@ca", "Videoart@es", "Videoart@en",
					"Grafica", "Gràfica@ca", "Gráfica@es", "Graphic@en",
					"Original", "Original@ca", "Original@es", "Original@en"
			};
			
			/*String[] materials = {
					"Gold", "Or@ca", "Oro@es", "Gold@en",
					"Iron", "Acer@ca", "Acero@es", "Iron@en",
					"Marble", "Marbre@ca", "Mármol@es", "Marble@en",
					"Pastle", "Pastel@ca", "Pastel@es", "Pastle@en",
					"Stone", "Pedra@ca", "Piedra@es", "Stone@en",
					"Tempera", "Tempera@ca", "Tempera@es", "Tempera@en",
					"Titanium", "Titani@ca", "Titanio@es", "Titanium@en",
					"Wood", "Fusta@ca", "Madera@es", "Wood@en"
			};*/
			
			int i=0;
			while(i<languages.length) {
				CustomMap lang = new CustomMap();
				lang.put("type", "ac:Language");
				lang.put("about", languages[i++]);
				lang.put("ac:Label", languages[i++]);
				lang.put("ac:Label", languages[i++]);
				lang.put("ac:Label", languages[i++]);
				uploadObject(lang);
			}
			
			i=0;
			while(i<kindArtWork.length) {
				CustomMap kind = new CustomMap();
				kind.put("type", "ac:KindArtWork");
				kind.put("about", kindArtWork[i++]);
				kind.put("ac:Name", kindArtWork[i++]);				
				kind.put("ac:Name", kindArtWork[i++]);
				kind.put("ac:Name", kindArtWork[i++]);
				uploadObject(kind);
			}
			
			/*i=0;
			while(i<materials.length) {
				CustomMap m = new CustomMap();
				m.put("type", "ac:Material");
				m.put("about", materials[i++]);
				m.put("Label", materials[i++]);
				m.put("Label", materials[i++]);
				m.put("Label", materials[i++]);
				uploadObject(m);
			}*/
		
		} catch (Exception e) {
			log.error("",e);
		}
		
	}
	
	private static void migrarTipusDocumental() {
		log.debug(" ======================== MIGRACIO DOCUMENTARY TYPE ========================== ");
		
		try {
			CsvReader r = new CsvReader(new FileReader(new File("./fm/VocavbularisControlats.csv")));
			r.readHeaders();
			
			String className = "ac:PaperDocumentaryType";
			
			while(r.readRecord()) {
				if (r.get("TIPUS")!=null && !"".equals(r.get("TIPUS"))) {
					if ("AUDIOVISUAL".equals(r.get("TIPUS").trim())) className = "ac:AudiovisualDocumentaryType";
					else if ("PAPER".equals(r.get("TIPUS").trim())) className = "ac:PaperDocumentaryType";
				}
				
				CustomMap obj = new CustomMap();
				obj.put("type", className);
				
				if (r.get("TÍTOL-cat")!=null && !"".equals(r.get("TÍTOL-cat"))) {
					obj.put("about", r.get("TÍTOL-cat").trim());
					obj.put("ac:Name", r.get("TÍTOL-cat").trim()+"@ca");
				}
				if (r.get("TÍTOL-cast")!=null && !"".equals(r.get("TÍTOL-cast"))) {
					obj.put("ac:Name", r.get("TÍTOL-cast").trim()+"@es");
				}
				if (r.get("TÍTOL-ang")!=null && !"".equals(r.get("TÍTOL-ang"))) {
					obj.put("ac:Name", r.get("TÍTOL-ang").trim()+"@en");
				}
				if (r.get("DEFINICIÓ")!=null && !"".equals(r.get("DEFINICIÓ"))) {
					obj.put("ac:definition", r.get("DEFINICIÓ").trim()+"@ca");
				}
				
				uploadObject(obj);
			}
		} catch (Exception e) {
			log.error("",e);
		}
	}

	public static void main(String[] args) throws Exception {
		migrar = Migrar.TOT;
		hostport = "localhost:8080";
		String resetTime = null; // "30/11/11 16:37"
		directori_medias = ".";
		
		// arguments: <Què migrar> <Url servidor> <Data-Hora servidor> <DirectoriMedias> 
		if (args!=null) {
			if (args.length>0) {
				if ("tot".equals(args[0])) migrar = Migrar.TOT;
				else if ("nomes media".equals(args[0])) migrar = Migrar.NOMES_MEDIA;
				else if ("nomes dades".equals(args[0])) migrar = Migrar.NOMES_DADES;
			}
			if (args.length>1 && !"".equals(args[1]) && args[1]!=null) hostport = args[1];
			if (args.length>2 && !"".equals(args[2]) && args[2]!=null) resetTime = args[2];
			if (args.length>3) directori_medias = args[3];
		}
		
		SimpleDateFormat sdf = new SimpleDateFormat("kk:mm");
		log.debug("Starting migration at " + sdf.format(new GregorianCalendar().getTime()));
		log.debug("Configuració : ");
		log.debug("\t Migrar: " + migrar);
		log.debug("\t URL servidor: " + hostport);
		log.debug("\t Data i hora reset: " + resetTime);
		log.debug("\t Directori medias: " + directori_medias);
		
		if (migrar == Migrar.TOT || migrar == Migrar.NOMES_DADES) {
			reseteja(resetTime);
			
			// ----- Migrar dades fixes				DONE
			migrarDadesFixes();
			migrarCollections();
			migrarTipusDocumental();
			
			// ----- Migració de SPIP				DONE
			migrarPersons(); 				
			migrarEvents();							
			migrarPublications();
			migrarRelations();
			
			// ----- Migració de File-Maker			DONE
			migrarFileMaker1();
			migrarFileMaker2();
			migrarFileMaker3();
			
		}
		
		backupDadesTemporalsMigracio();
		if (migrar == Migrar.TOT || migrar == Migrar.NOMES_MEDIA) {
			// ----- Migració de Media				DONE
			migrarMedia();
			migrarImages();
		}
		
		log.debug("FINISHED migration at " + sdf.format(new GregorianCalendar().getTime()));
		// -- utils no migracio

		//collectAgents();
		//collectCities();
	}

	private static void backupDadesTemporalsMigracio() throws Exception {
		if (migrar == Migrar.NOMES_DADES || migrar == Migrar.TOT) {
			log.debug(" ======================== BACKUP DADES TEMPORALS ========================== ");
			
			String collectionListJson = new Gson().toJson(collectionList);
			File f = new File("collectionList.json");
			FileWriter fw = new FileWriter(f);
			fw.write(collectionListJson);
			fw.close();
			
			String mediaFileIdJson = new Gson().toJson(mediaFileId);
			f = new File("mediaFileId.json");
			fw = new FileWriter(f);
			fw.write(mediaFileIdJson);
			fw.close();
			
			String objectExpedientJson = new Gson().toJson(objectExpedient);
			f = new File("objectExpedient.json");
			fw = new FileWriter(f);
			fw.write(objectExpedientJson);
			fw.close();
			
			String imagesExpedientJson = new Gson().toJson(imagesExpedient);
			f = new File("imagesExpedient.json");
			fw = new FileWriter(f);
			fw.write(imagesExpedientJson);
			fw.close();
			
			String workExpedientJson = new Gson().toJson(workExpedient);
			f = new File("workExpedient.json");
			fw = new FileWriter(f);
			fw.write(workExpedientJson);
			fw.close();
		} else if (migrar == Migrar.NOMES_MEDIA) {
			log.debug(" ======================== RECUPERACIÓ DADES TEMPORALS ========================== ");
			
			File f = new File("collectionList.json");
			collectionList = new Gson().fromJson(new FileReader(f), String[][].class);
			
			f = new File("mediaFileId.json");
			Type listOfArraysType = new TypeToken<List<String[]>>(){}.getType();
			mediaFileId = new Gson().fromJson(new FileReader(f), listOfArraysType);
			
			f = new File("objectExpedient.json");
			Type stringMapType = new TypeToken<Map<String,String>>(){}.getType();
			objectExpedient = new Gson().fromJson(new FileReader(f), stringMapType);	
			
			GsonBuilder gson = new GsonBuilder();
	    	gson.registerTypeAdapter(CustomMap.class, new CustomMapDeserializer());
	    	f = new File("imagesExpedient.json");
	    	imagesExpedient = gson.create().fromJson(new FileReader(f), CustomMap.class);
			
			f = new File("workExpedient.json");
			workExpedient = new Gson().fromJson(new FileReader(f), stringMapType);
		}
	}
}