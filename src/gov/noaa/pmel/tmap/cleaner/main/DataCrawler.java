package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;
import gov.noaa.pmel.tmap.cleaner.crawler.CrawlableLeafNode;
import gov.noaa.pmel.tmap.cleaner.crawler.DataCrawlOne;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

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
    private static boolean force;
    private static Properties properties;
    private static JDOPersistenceManagerFactory pmf;
    private static List<CrawlableLeafNode> dataSources = new ArrayList<CrawlableLeafNode>();
    private static List<Future<String>> futures = new ArrayList<Future<String>>();
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
            String url = cl.getOptionValue("u");
            force = cl.hasOption("f");
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
                pmf = (JDOPersistenceManagerFactory) JDOHelper.getPersistenceManagerFactory(properties);
                PersistenceManager persistenceManager = pmf.getPersistenceManager();
                System.out.println("Starting data crawl work at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss")+" with "+threads+" threads.");
                helper = new PersistenceHelper(persistenceManager);
                if ( url == null ) {
                    url = root;
                }
                Catalog rootCatalog = helper.getCatalog(root, url);
                List<LeafNodeReference> rootLeafNodes = rootCatalog.getLeafNodes();
                if ( rootLeafNodes != null ) {
                    for ( Iterator leafIt = rootLeafNodes.iterator(); leafIt.hasNext(); ) {
                        LeafNodeReference leafNodeReference = (LeafNodeReference) leafIt.next();
                        dataSources.add(new CrawlableLeafNode(root, url, leafNodeReference));
                    }
                }            
                gatherReferences(root, rootCatalog.getCatalogRefs());
                int total = 0;
                Collections.shuffle(dataSources);
                for ( Iterator<CrawlableLeafNode> dsIt = dataSources.iterator(); dsIt.hasNext(); ) {
                    CrawlableLeafNode cln = dsIt.next();
                    DataCrawlOne one = new DataCrawlOne(pmf, cln.getRoot(), cln.getParent(), cln.getLeafNodeReference(), force);
                    Future<String> f = pool.submit(one);
                    futures.add(f);   
                    total++;
                }
                System.out.println(total+" data sources queued for processing.");
                boolean done = false;
                List<String> completed = new ArrayList<String>();
                while ( !done ) {
                    done = true;
                    for ( Iterator<Future<String>> futuresIt = futures.iterator(); futuresIt.hasNext(); ) {
                        Future<String> f = (Future<String>) futuresIt.next();
                        if ( f.isDone() ) {
                            String leaf = f.get();
                            if ( !completed.contains(leaf) ) {
                                completed.add(leaf);
                                System.out.println("Finished with "+leaf);
                            }
                        }
                        done = done && f.isDone();
                    }
                }
                helper.close();
                shutdown(0);
            } catch ( Exception e ) {
                System.out.println("Error with "+e.getLocalizedMessage());
                e.printStackTrace();
            } finally {
                shutdown(0);
            }
        } catch ( ParseException e ) {
            System.err.println( e.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(width);
            formatter.printHelp("DataCrawler", crawlerOptions, true);
            System.exit(-1);
        }
    }
    public static void gatherReferences(String parent, List<CatalogReference> refs) {
        Map<String, List<LeafNodeReference>> subReferences = new HashMap<String, List<LeafNodeReference>>();
        try {
            for ( Iterator refsIt = refs.iterator(); refsIt.hasNext(); ) {
                CatalogReference catalogReference = (CatalogReference) refsIt.next();
                Catalog sub = helper.getCatalog(parent, catalogReference.getUrl());
                if ( sub != null ) {
                    List<LeafNodeReference> l = sub.getLeafNodes();
                    if ( l != null ) {
                        for ( Iterator lIt = l.iterator(); lIt.hasNext(); ) {
                            LeafNodeReference leafNodeReference = (LeafNodeReference) lIt.next();
                            dataSources.add(new CrawlableLeafNode(parent, sub.getUrl(), leafNodeReference));
                        }
                    }
                    gatherReferences(sub.getUrl(), sub.getCatalogRefs());
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
        System.out.println("All work complete.  Shutting down at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        pool.shutdown();
        System.exit(code);
    }
}
