package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.crawler.Clean;
import gov.noaa.pmel.tmap.cleaner.crawler.CleanableCatalog;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogXML;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;

import java.io.FileNotFoundException;
import java.io.IOException;
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
            processChildren(root, refs);
            int total = 0;
            Collections.shuffle(cleanables);
            for ( Iterator cleanableIt = cleanables.iterator(); cleanableIt.hasNext(); ) {
                CleanableCatalog cleanable = (CleanableCatalog) cleanableIt.next();              
                Clean clean = new Clean(pmf, cleanable, threddsServer, threddsServerName, threddsContext, exclude, excludeDataset);
                completionPool.submit(clean);
                total++;
            }
            for ( int i = 0; i < total; i++) {
                Future<String> f = completionPool.take();
                String leaf = f.get();
                System.out.println("Finished with "+leaf);
            }
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
    private static void processChildren(String url, List<CatalogReference> refs) throws IOException, JDOMException, URISyntaxException {
        for ( Iterator<CatalogReference> refsIt = refs.iterator(); refsIt.hasNext(); ) {
            CatalogReference catalogReference = refsIt.next();
            Catalog catalog = helper.getCatalog(url, catalogReference.getUrl());
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
