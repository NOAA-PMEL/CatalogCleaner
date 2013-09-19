package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.crawler.Clean;
import gov.noaa.pmel.tmap.cleaner.crawler.CleanableCatalog;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogXML;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.jdo.Rubric;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.jdo.PersistenceManager;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.jdom2.JDOMException;
import org.joda.time.DateTime;

public class Cleaner extends Crawler {

    private static List<CleanableCatalog> cleanables = new ArrayList<CleanableCatalog>();

    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            
            Option s = crawlerOptions.getOption("s");
            s.setRequired(true);
            crawlerOptions.addOption(s);
            
            init(false, args);
            threddsServer = cl.getOptionValue("s");
            if ( !threddsServer.startsWith("http://")) {
                System.err.println("The server must be a full url, eg. http://ferret.pmel.noaa.gov:8080/");
                System.exit(-1);
            }
            if ( threddsServer.startsWith("http://")) threddsServerName = threddsServer.substring(7);
            if ( threddsServer.startsWith("https://")) threddsServerName = threddsServer.substring(8);
            
            System.out.println("Starting clean work at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));

            Catalog catalog;
            CatalogXML catalogXML;
            CleanableCatalog cleanableCatalog;
            if ( url != null ) {
                cleanableCatalog = new CleanableCatalog(root, url);
            } else {
                cleanableCatalog  = new CleanableCatalog(root, root);
            }
            cleanables.add(cleanableCatalog);
            catalog = helper.getCatalog(cleanableCatalog.getParent(), cleanableCatalog.getUrl());
            System.out.println("Doing a catalog clean for root "+root);
            List<CatalogReference> refs = catalog.getCatalogRefs();
            // We want to exclude the catalogs in the skip at the root level.
            
            processChildren(cleanableCatalog.getUrl(), refs);
            int total = 0;
            Collections.shuffle(cleanables);
            for ( Iterator cleanableIt = cleanables.iterator(); cleanableIt.hasNext(); ) {
                CleanableCatalog cleanable = (CleanableCatalog) cleanableIt.next();             
                Clean clean = new Clean(pmf, cleanable, threddsServer, threddsServerName, threddsContext, exclude, excludeCatalog, excludeDataset);
                completionPool.submit(clean);
                total++;
            }
            for ( int i = 0; i < total; i++) {
                Future<String> f = completionPool.take();
                String leaf = f.get();
                System.out.println("Finished with "+leaf);
            }
           
      
            for (Iterator refIt = refs.iterator(); refIt.hasNext();) {
                CatalogReference catalogReference = (CatalogReference) refIt.next();
                if ( !skip(catalogReference.getUrl())) { 
                    File rfile = new File(Clean.getRubricFilePath(catalogReference.getUrl()));
                    Rubric rubric = null;
                    if ( rfile.exists() ) {
                        rubric = Rubric.read(rfile);
                    } else {
                        rubric = new Rubric();
                        rubric.setParentJson(Clean.getRubricFilePath(cleanableCatalog.getParent()));
                        rubric.setParent(cleanableCatalog.getParent());
                        rubric.setParentJson(Clean.getRubricFilePath(cleanableCatalog.getParent()));
                        rubric.setUrl(catalogReference.getUrl());
                    }

                    if ( rubric != null ) {
                        sum(cleanableCatalog.getUrl(), catalogReference.getUrl(), rubric);
                        rubric.write();
                    }
                }
            }
            Rubric rootRub = new Rubric();
            rootRub.setParent(root);
            rootRub.setUrl(root);
            if ( url == null ) {
                // Sum up the top level catalogs for the root.
                for (Iterator refIt = refs.iterator(); refIt.hasNext();) {
                    CatalogReference catalogReference = (CatalogReference) refIt.next();
                    if ( !skip(catalogReference.getUrl())) { 
                        File rfile = new File(Clean.getRubricFilePath(catalogReference.getUrl()));
                        Rubric childRub = Rubric.read(rfile);
                        rootRub.addAggregated(childRub.getAggregated());
                        rootRub.addBadLeaves(childRub.getBadLeaves());
                        rootRub.addFast(childRub.getFast());
                        rootRub.addLeaves(childRub.getLeaves());
                        rootRub.addMedium(childRub.getMedium());
                        rootRub.addSlow(childRub.getSlow());
                        rootRub.addChild(Clean.getRubricFilePath(childRub.getUrl()));
                    }
                }
            }
            rootRub.write();
            pmf.close();
            pool.shutdown();
            System.out.println("All work complete.  Shutting down at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
            System.exit(0);
        } catch ( FileNotFoundException e ) {
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
        } catch ( IOException e ) {
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
        } catch ( JDOMException e ) {
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
        } catch ( URISyntaxException e ) {
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
        } catch ( InterruptedException e ) {
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
        } catch ( ExecutionException e ) {
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
        } catch ( InstantiationException e ) {
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
        } catch ( IllegalAccessException e ) {
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
        } catch ( ClassNotFoundException e ) {
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
        } catch ( SQLException e ) {
            System.err.println(e.getLocalizedMessage());
            System.exit(-1);
        }
    }
    private static void sum(String father, String son, Rubric rubric) throws IOException {        
        Catalog c = helper.getCatalog(father, son);     
        if ( c != null ) {
            List<CatalogReference> refs = c.getCatalogRefs();
            for (Iterator catRefIt = refs.iterator(); catRefIt.hasNext();) {
                CatalogReference catalogReference = (CatalogReference) catRefIt.next();
                if ( !skip(catalogReference.getUrl()) ) {
                    File rfile = new File(Clean.getRubricFilePath(catalogReference.getUrl()));
                    Rubric catrub;
                    if ( rfile.exists() ) {
                        catrub = Rubric.read(rfile);
                    } else {
                        catrub = new Rubric();
                        catrub.setParent(son);
                        catrub.setParentJson(Clean.getRubricFilePath(son));
                        catrub.setUrl(catalogReference.getUrl());
                    }
                    sum(son, catalogReference.getUrl(), catrub);
                    rubric.addAggregated(catrub.getAggregated());
                    rubric.addBadLeaves(catrub.getBadLeaves());
                    rubric.addFast(catrub.getFast());
                    rubric.addLeaves(catrub.getLeaves());
//                    System.out.println("Adding "+catrub.getLeaves()+" leaves from "+catrub.getUrl()+" to "+rubric.getUrl()+" for a total of "+rubric.getLeaves());
                    rubric.addMedium(catrub.getMedium());
                    rubric.addSlow(catrub.getSlow());
                    // Keep track of the direct children of this json file.
                    if ( son.equals(catrub.getParent()) ) {
                        String child = rfile.getAbsolutePath();
                        child = child.substring(child.indexOf("CleanCatalogs"));
                        rubric.addChild(child);
                    }  
                   
                }
            }  
            rubric.write();
        }
    }
    private static void processChildren(String url, List<CatalogReference> refs) throws IOException, JDOMException, URISyntaxException {
        for ( Iterator<CatalogReference> refsIt = refs.iterator(); refsIt.hasNext(); ) {
            CatalogReference catalogReference = refsIt.next();
            Catalog catalog = helper.getCatalog(url, catalogReference.getUrl());
            if ( !skip(catalogReference.getUrl()) ) {
                CleanableCatalog cleanable = new CleanableCatalog(url, catalogReference.getUrl());
                cleanables.add(cleanable);
                if ( catalog != null && catalog.getCatalogRefs() != null && catalog.getCatalogRefs().size() > 0 ) {
                    processChildren(catalog.getUrl(), catalog.getCatalogRefs());
                } else {
                    if ( catalog == null ) {
                        System.out.println("Nothing to clean in "+url);
                    }
                }
            }
        }
    }
}
