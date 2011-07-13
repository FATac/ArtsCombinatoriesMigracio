import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class Migracio {
	private static Logger log = Logger.getLogger(Migracio.class.getName());
	
	static final boolean MIGRAR = false;
	
	private static void uploadObject(Map<String, String> data) throws Exception {
		URL url = new URL("http://localhost:8080/ArtsCombinatoriesRest/uploadObject");
	    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
	    conn.setRequestProperty("Content-Type", "application/json");
	    conn.setRequestMethod("POST");
	    conn.setDoOutput(true);
	    OutputStreamWriter wr = new OutputStreamWriter(conn.getOutputStream());
	    wr.write(new Gson().toJson(data));
	    wr.flush();
	    wr.close();
	}
	
	private static void migrarPersons() {
		try {
			// baixar info de persones 
			log.info("Downloading persons rdf data...");
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
			    	URL personUrl = new URL(object.toString());
			    	//URL personUrl = new URL("http://www.fundaciotapies.org/site/spip.php?page=xml-participant&id_article=4081");
			    	
			    	in = new BufferedReader(new InputStreamReader(personUrl.openStream()));
					
					File personFile = new File("currentPerson.rdf");
					if (personFile.exists()) {
						personFile.delete();
						personFile = new File("currentPerson.rdf");
					}
					FileWriter personFileWriter = new FileWriter(personFile);
			
					input = new char[255];
					b = 0;
					while ((b = in.read(input)) > 0) personFileWriter.write(input, 0, b);
					in.close();
					
					personFileWriter.flush();
					personFileWriter.close();
					
					Model model1 = ModelFactory.createDefaultModel();
					model1.read("file:currentPerson.rdf");
					
					StmtIterator it2 = model1.listStatements();
					Map<String, String> data = new HashMap<String,String>();
					
					log.info("Reading person...");
					
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
					    		data.put("className", "Person");
					    		if (content.equals("http://xmlns.com/foaf/0.1/givenName")) {
									 data.put("name", object1.toString());
									System.out.println("name " + object1.toString());
								} else if (content.equals("http://xmlns.com/foaf/0.1/familyName")) {
									data.put("surname", object1.toString());
									System.out.println("surname " + object1.toString());
								} else if (content.equals("http://purl.org/vocab/bio/0.1/biography")) {
									String bio = object1.toString().replaceAll(" class=\"spip\"", "").replaceAll(" class=\"spip_out\"","");
									data.put("CV", bio);
									System.out.println("CV " + bio);
								} else if (content.equals("http://purl.org/vocab/bio/0.1/date")) {
									data.put("BirthDate", object1.toString());
									System.out.println("BirthDate " + object1.toString());
								}
					    	}
					    }
					}
					
					if (MIGRAR) {
						log.info("Uploading person...");
						uploadObject(data);
					    log.info("Uploaded. ");
					}
			    }
			}
		} catch (Exception e) {
			log.severe("Error migrating persons " + e);
			e.printStackTrace();
		}
	}
	
	public static void migrarEvents() {
		try {
			// migrar events
			log.info("Downloading events rdf data...");
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
			
			Model model = ModelFactory.createDefaultModel();
			model.read("file:events.rdf");
			
			String lastDocument = null;
			
			//	per a cada event...
			StmtIterator it1 = model.listStatements();
			while(it1.hasNext()) {
				Statement stmt = it1.next();
			    //Resource  subject   = stmt.getSubject();     			// get the subject
			    Property  predicate = stmt.getPredicate();   			// get the predicate
			    RDFNode   object    = stmt.getObject();      			// get the object
			   
			    // obtenir les seves dades completes...
			    if (predicate.toString().equals("http://www.fundaciotapies.org/terms/0.1/rdfuri")) {
			    	URL eventUrl = new URL(object.toString());
			    	
			    	in = new BufferedReader(new InputStreamReader(eventUrl.openStream()));
					
					File eventFile = new File("currentEvent.rdf");
					if (eventFile.exists()) {
						eventFile.delete();
						eventFile = new File("currentEvent.rdf");
					}
					FileWriter eventFileWriter = new FileWriter(eventFile);
			
					input = new char[255];
					b = 0;
					while ((b = in.read(input)) > 0) eventFileWriter.write(input, 0, b);
					in.close();
					
					eventFileWriter.flush();
					eventFileWriter.close();
					
					Model model1 = ModelFactory.createDefaultModel();
					model1.read("file:currentEvent.rdf");
					
					StmtIterator it2 = model1.listStatements();
					Map<String, String> data = new HashMap<String, String>();
					data.put("className", "SuperActivity");
					
					Map<String, Map<String, String>> documents = new HashMap<String, Map<String,String>>();
					Map<String, String> currDoc = new HashMap<String, String>();
					
					log.info("Reading event...");
					while(it2.hasNext()) {
						Statement stmt1 = it2.next();
						
						Resource  subject1   = stmt1.getSubject();     	// get the subject
					    Property  predicate1 = stmt1.getPredicate();   	// get the predicate
					    RDFNode   object1   = stmt1.getObject();      	// get the object
					    
					    //System.out.println(stmt1);
					    
					    String objectURI = subject1.toString();
					    
					    if (subject1.isURIResource()) {
					    	
					    	String content = predicate1.toString();
					    	
						    if (objectURI.contains("events")) {
						    	if (content.equals("http://purl.org/dc/elements/1.1/title")) {
									data.put("title", object1.toString());
									System.out.println("title" + object1.toString());
								} else if (content.equals("http://purl.org/dc/elements/1.1/date")) {
									data.put("date", object1.toString());
									System.out.println("date " + object1.toString());
								}
						    } else if (objectURI.contains("documents")) {
						    	String[] uriparts = objectURI.split("/");
						    	String currentDoc = uriparts[uriparts.length-1];
						    	
						    	if (!currentDoc.equals(lastDocument)) {
						    		if (lastDocument!=null) {
						    			documents.put(lastDocument, currDoc);
						    		}
						    		currDoc = new HashMap<String, String>();
						    		currDoc.put("className", "Document");
						    		lastDocument = currentDoc;
						    	}
						    	
						    	if (content.equals("http://purl.org/dc/elements/1.1/title")) {
						    		currDoc.put("title", object1.toString());
						    		System.out.println("doc-title " + object1.toString());
						    	} else if (content.equals("http://purl.org/dc/elements/1.1/description")) {
						    		String desc = object1.toString().replaceAll(" class=\"spip\"", "").replaceAll(" class=\"spip_out\"","");
						    		currDoc.put("description", desc);
						    		System.out.println("doc-description " + desc);
						    	} 
						    }
					    }
					}
					
					if (lastDocument!=null) {
						documents.put(lastDocument, currDoc);
					}
					
					if (MIGRAR) {
						log.info("Uploading event...");
						uploadObject(data);
					    log.info("Uploaded. ");
					    
					    log.info("Uploading event documents...");
					    Iterator<Map.Entry<String, Map<String, String>>> it = documents.entrySet().iterator();
					    while (it.hasNext()) {
					    	Map.Entry<String, Map<String, String>> entry = it.next();
					    	uploadObject(entry.getValue());
					    }
					    log.info("Uploaded.");
					    
					    // TODO: migrate relations between event and documents !!!
					}
			    }
			}
		} catch (Exception e) {
			log.severe("Error " + e);
			e.printStackTrace();
		}
	}
	
	public static void migrarPublications() {
		try {
			// migrar publications
			log.info("Downloading publications rdf data...");
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
			    	URL publicationUrl = new URL(object.toString());
			    	
			    	in = new BufferedReader(new InputStreamReader(publicationUrl.openStream()));
					
					File publicationFile = new File("currentPublication.rdf");
					if (publicationFile.exists()) {
						publicationFile.delete();
						publicationFile = new File("currentPublication.rdf");
					}
					FileWriter publicationFileWriter = new FileWriter(publicationFile);
			
					input = new char[255];
					b = 0;
					while ((b = in.read(input)) > 0) publicationFileWriter.write(input, 0, b);
					in.close();
					
					publicationFileWriter.flush();
					publicationFileWriter.close();
					
					Model model1 = ModelFactory.createDefaultModel();
					model1.read("file:currentPublication.rdf");
					
					//model1.write(System.out);
					
					StmtIterator it2 = model1.listStatements();
					Map<String, String> data = new HashMap<String, String>();
					data.put("className", "Publication");
					
					log.info("Reading publication...");
					
					
					while(it2.hasNext()) {
						Statement stmt1 = it2.next();
						
						Resource  subject1   = stmt1.getSubject();     	// get the subject
					    Property  predicate1 = stmt1.getPredicate();   	// get the predicate
					    RDFNode   object1   = stmt1.getObject();      	// get the object
					    
					    //if (!subject1.isURIResource() || !subject1.toString().contains("publications")) continue;
					    
					    String predicateURI = predicate1.toString();
					    String value = object1.toString();
						
						if (predicateURI.equals("http://purl.org/dc/elements/1.1/title")) {
							data.put("title", value);
							System.out.println("title " + value);
						} else if (predicateURI.equals("http://purl.org/dc/elements/1.1/description")) {
							data.put("description", value);
							System.out.println("description " + value);
						} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/cover")) {
							data.put("cover", value);
							System.out.println("cover " + value);
						} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/pvp")) {
							data.put("pvp", value);
							System.out.println("pvp " + value);
						} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/pvp_friends")) {
							data.put("pvp_friends ", value);
							System.out.println("cover " + value);
						} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/internal_comments")) {
							data.put("internal_comments", value);
							System.out.println("internal_comments " + value);
						} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/disponible")) {
							data.put("disponible", value);
							System.out.println("disponible " + value);
						} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/internal_ref")) {
							data.put("internal_ref", value);
							System.out.println("internal_ref " + value);
						} else if (predicateURI.equals("http://www.fundaciotapies.org/terms/0.1/special_offer")) {
							data.put("special_offer", value);
							System.out.println("special_offer " + value);
						} else if (value.startsWith("ISBN")) {
							data.put("ISBN", value.substring(5));
							System.out.println(value);
						}
					}
				
					if (MIGRAR) {
						log.info("Uploading publication...");
						uploadObject(data);
					    log.info("Uploaded. ");
					}
			    }
			}
		} catch (Exception e) {
			log.severe("Error " + e);
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		//migrarPersons();
		migrarEvents();
		//migrarPublications();
		
		// TODO: migrar case-files
		// TODO: enllaçar events i case-files
		// TODO: migrar publicacions de SPIP
		// TODO: enllaçar persones-events, events-publicacions (SPIP)
		// TODO: migrar file-maker
		// TODO: migrar arxius digitalitzats
	}
}
