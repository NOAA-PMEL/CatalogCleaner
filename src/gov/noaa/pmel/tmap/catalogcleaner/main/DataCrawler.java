package gov.noaa.pmel.tmap.catalogcleaner.main;

import gov.noaa.pmel.tmap.catalogcleaner.crawler.DataCrawl;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.joda.time.DateTime;

public class DataCrawler {
    private static String[] exclude;
    private static ExecutorService pool;
    private static PersistenceHelper helper; 
    private static String root;
    private static List<Future<String>> futures;
    private static Properties properties;
    /**
     * @param args
     */
    public static void main(String[] args) {
        CrawlerOptions crawlerOptions = new CrawlerOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine cl = null;
        int width = 80;
        try {
            cl = parser.parse(crawlerOptions, args);
            root = cl.getOptionValue("r");
            exclude = cl.getOptionValues("x");
            String t = cl.getOptionValue("t");
            String database = cl.getOptionValue("d");
            int threads = 1;
            try {
                if ( t != null )
                    threads = Integer.valueOf(t);
            } catch ( NumberFormatException e ) {
                // S'ok.  Use the default.
            }
            pool = Executors.newFixedThreadPool(threads);
            properties = new Properties();
            URL propertiesURL =  ClassLoader.getSystemResource("datanucleus.properties");
            try {
                properties.load(new FileInputStream(new File(propertiesURL.getFile())));
                String connectionURL = (String) properties.get("datanucleus.ConnectionURL");
                if ( connectionURL.contains("database") ) {
                    connectionURL = connectionURL.replace("database", "");
                } else {
                    System.err.println("The conenctionURL string should use the name \"databast\" which will be substituted for each catalog" );
                    System.exit(-1);
                }
                String tag = DateTime.now().toString("yyyyMMdd");
                if ( database == null ) {
                    database = "cc_"+tag;
                }                
                connectionURL = connectionURL + database;
                properties.setProperty("datanucleus.ConnectionURL", connectionURL);
                JDOPersistenceManagerFactory pmf = (JDOPersistenceManagerFactory) JDOHelper.getPersistenceManagerFactory(properties);
                PersistenceManager persistenceManager = pmf.getPersistenceManager();
                System.out.println("We are using mulitple threads. "+persistenceManager.getMultithreaded());
                helper = new PersistenceHelper(persistenceManager);
                Transaction tx = helper.getTransaction();
                tx.begin();
                Catalog rootCatalog = helper.getCatalog(root, root);
                DataCrawl dataCrawl = new DataCrawl(properties, root, root);
                futures = new ArrayList<Future<String>>();
                Future<String> future = pool.submit(dataCrawl);
                futures.add(future);
                processReferences(root, rootCatalog.getCatalogRefs());
                tx.commit();
                for ( Iterator futuresIt = futures.iterator(); futuresIt.hasNext(); ) {
                    Future<String> f = (Future<String>) futuresIt.next();
                    String cat = f.get();
                    System.out.println("Finished with "+cat);
                }
                helper.close();
                shutdown(0);
            } catch ( IOException e ) {
                shutdown(-1);
                e.printStackTrace();
            } catch ( InterruptedException e ) {
                shutdown(-1);
                e.printStackTrace();
            } catch ( ExecutionException e ) {
                shutdown(-1);
                e.printStackTrace();
            } catch ( Exception e ) {
                shutdown(-1);
                e.printStackTrace();
            }
        } catch ( ParseException e ) {
            System.err.println( e.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(width);
            formatter.printHelp("TreeCrawler", crawlerOptions, true);
            System.exit(-1);
        }
    }
    public static void processReferences(String parent, List<CatalogReference> refs) {
        try {
        for ( Iterator refsIt = refs.iterator(); refsIt.hasNext(); ) {
            CatalogReference catalogReference = (CatalogReference) refsIt.next();
            Catalog sub = helper.getCatalog(parent, catalogReference.getUrl());
            if ( sub != null ) {
                DataCrawl dataCrawl = new DataCrawl(properties, sub.getParent(), sub.getUrl());
                Future<String> future = pool.submit(dataCrawl);
                futures.add(future);
                processReferences(sub.getUrl(), sub.getCatalogRefs());
            } else {
                System.err.println("CatalogRefernce db reference was null for "+catalogReference.getUrl());
            }
        }
        } catch (Exception e) {
            e.printStackTrace();
            shutdown(-1);
        }
    }
    public static void shutdown(int code) {
        pool.shutdown();
        System.exit(code);
    }
}
