package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;
import gov.noaa.pmel.tmap.cleaner.crawler.TreeCrawl;
import gov.noaa.pmel.tmap.cleaner.crawler.TreeCrawlResult;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogXML;
import gov.noaa.pmel.tmap.cleaner.jdo.ClassList;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.datanucleus.NucleusContext;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.datanucleus.store.schema.SchemaAwareStoreManager;
import org.joda.time.DateTime;

public class TreeCrawler {
    private static String[] exclude;
    private static ExecutorService pool;
    private static PersistenceHelper helper; 
    private static String root;
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
                String connectionUser = properties.getProperty("datanucleus.ConnectionUserName");
                String connectionPW = properties.getProperty("datanucleus.ConnectionPassword");

                Class.forName("com.mysql.jdbc.Driver").newInstance();
                Connection conn = null;
                Statement s = null;
                conn = DriverManager.getConnection(connectionURL+"?user="+connectionUser+"&password="+connectionPW); 
                s=conn.createStatement();
                String tag = DateTime.now().toString("yyyyMMdd");
                if ( database == null ) {
                    database = "cc_"+tag;
                }
                
                s.executeUpdate("CREATE DATABASE IF NOT EXISTS "+database);
                connectionURL = connectionURL + database;
                properties.setProperty("datanucleus.ConnectionURL", connectionURL);
                JDOPersistenceManagerFactory pmf = (JDOPersistenceManagerFactory) JDOHelper.getPersistenceManagerFactory(properties);
                NucleusContext ctx = pmf.getNucleusContext();
                ClassList classNames = new ClassList();
                ((SchemaAwareStoreManager)ctx.getStoreManager()).createSchema(classNames, properties);
                PersistenceManager persistenceManager = pmf.getPersistenceManager();
                helper = new PersistenceHelper(persistenceManager);
                System.out.println("Starting tree crawl work at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss")+" with "+threads+" threads.");
                TreeCrawl crawl = new TreeCrawl(properties, root, root);
                List<Future> futures = new ArrayList<Future>();
                Future future = pool.submit(crawl);
                futures.add(future);
                saveAndProcessChildren(futures);
                helper.close();
                shutdown(0);
            } catch ( FileNotFoundException e ) {
                e.printStackTrace();
            } catch ( IOException e ) {
                e.printStackTrace();
            } catch ( SQLException e ) {
                e.printStackTrace();
            } catch ( InstantiationException e ) {
                e.printStackTrace();
            } catch ( IllegalAccessException e ) {
                e.printStackTrace();
            } catch ( ClassNotFoundException e ) {
                e.printStackTrace();
            } catch ( Exception e ) {
                e.printStackTrace();
            } finally {
                helper.close();
                shutdown(0);
            }
        } catch ( ParseException e ) {
            System.err.println( e.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(width);
            formatter.printHelp("TreeCrawler", crawlerOptions, true);
            System.exit(-1);
        }
    }
    private static void shutdown(int c) {
        System.out.println("All work complete.  Shutting down at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        pool.shutdown();
        System.exit(c);
    }
    private static void saveAndProcessChildren(List<Future> futures) throws Exception {
        List<Future> futureChildren = new ArrayList<Future>();
        for ( Iterator futureIt = futures.iterator(); futureIt.hasNext(); ) {
            Future<TreeCrawlResult> future = (Future<TreeCrawlResult>) futureIt.next();
            TreeCrawlResult result = future.get();
            Catalog catalog = helper.getCatalog(result.getParent(), result.getUrl());
            if ( catalog != null && !catalog.hasBestTimeSeries() ) {
                List<CatalogReference> refs = catalog.getCatalogRefs();
                List<CatalogReference> remove = new ArrayList();
                for ( Iterator refsIt = refs.iterator(); refsIt.hasNext(); ) {
                    CatalogReference ref = (CatalogReference) refsIt.next();
                    for ( int i = 0; i < exclude.length; i++ ) {
                        if ( Pattern.matches(exclude[i], ref.getUrl())) {
                            remove.add(ref);
                        }
                    }
                }
                refs.removeAll(remove);
                for ( Iterator refsIt = refs.iterator(); refsIt.hasNext(); ) {
                    CatalogReference catalogReference = (CatalogReference) refsIt.next();
                    String childURL = catalogReference.getUrl();
                    TreeCrawl crawl = new TreeCrawl(properties, catalog.getUrl(), childURL);
                    Future<TreeCrawlResult> futureChild = pool.submit(crawl);
                    futureChildren.add(futureChild);
                }
                saveAndProcessChildren(futureChildren);
            }
        }
       
        
    }
    
}
