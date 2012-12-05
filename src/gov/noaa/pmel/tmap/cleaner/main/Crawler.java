package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.xml.JDOMUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.joda.time.DateTime;

public class Crawler {

    protected static String root;
    protected static String threddsContext;
    protected static String threddsServer;
    protected static String threddsServerName;
    protected static List<String> exclude = new ArrayList<String>();
    protected static List<String> excludeCatalog = new ArrayList<String>();
    protected static Map<String, List<String>> excludeDataset = new HashMap<String, List<String>>();
    protected static ExecutorService pool;
    protected static CompletionService<String> completionPool;
    protected static PersistenceHelper helper;
    protected static String database;
    protected static String url;
    protected static boolean varcheck;
    protected static boolean force;
    protected static boolean full;
    protected static JDOPersistenceManagerFactory pmf;
    protected static Properties properties;
    protected static CrawlerOptions crawlerOptions = new CrawlerOptions();
    protected static CommandLine cl;
    public Crawler() {
        super();
    }  
    public static void init(boolean create, String[] args) throws FileNotFoundException, IOException, InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException, JDOMException{
        CommandLineParser parser = new GnuParser();
       
        int width = 80;
        try {

            URL skipFile = ClassLoader.getSystemResource("skip.xml");
            Document skipdoc = new Document();
            JDOMUtils.XML2JDOM(new File(skipFile.getFile()), skipdoc);
            
            Element skip = skipdoc.getRootElement();
            List<Element> regexEls = skip.getChildren("regex");
            for ( Iterator rIt = regexEls.iterator(); rIt.hasNext(); ) {
                Element regex = (Element) rIt.next();
                String value = regex.getAttributeValue("value");
                if ( value != null ) {
                    exclude.add(value);
                }
            }
            List<Element> catalogEls = skip.getChildren("catalog");
            for ( Iterator catIt = catalogEls.iterator(); catIt.hasNext(); ) {
                Element catE = (Element) catIt.next();
                String url = catE.getAttributeValue("url");
                if ( url != null ) {
                    if ( catE.getChildren("dataset").size() == 0 ) {
                        excludeCatalog.add(url);
                    } else {
                        List<Element> dsE = catE.getChildren("dataset");
                        List<String> exds = new ArrayList<String>();
                        excludeDataset.put(url, exds);
                        for ( Iterator dsIt = dsE.iterator(); dsIt.hasNext(); ) {
                            Element dataset = (Element) dsIt.next();
                            String name = dataset.getAttributeValue("name");
                            if ( name != null ) {
                                exds.add(name);
                            }
                        }
                    }
                }
            }
            
            cl = parser.parse(crawlerOptions, args);
            full = cl.hasOption("a");
            root = cl.getOptionValue("r");
            url = cl.getOptionValue("u");
            threddsContext = cl.getOptionValue("c");
            String[] x = cl.getOptionValues("x");
            if ( x != null ) {
                for ( int i = 0; i < x.length; i++ ) {
                    exclude.add(x[i]);
                }
            }
            if ( threddsContext == null ) {
                threddsContext = "thredds";
            }
            
            String t = cl.getOptionValue("t");
            int threads = 1;
            try {
                if ( t != null )
                    threads = Integer.valueOf(t);
            } catch ( NumberFormatException e ) {
                // S'ok.  Use the default.
            }
            varcheck = cl.hasOption("v");
            force = cl.hasOption("f");
            pool = Executors.newFixedThreadPool(threads);
            completionPool = new ExecutorCompletionService<String>(pool);
           
            database = cl.getOptionValue("d");
            String tag = DateTime.now().toString("yyyyMMdd");

            if ( database == null ) {
                database = "cc_"+tag;
            }
            properties = new Properties() ;
            URL propertiesURL =  ClassLoader.getSystemResource("datanucleus.properties");
            properties.load(new FileInputStream(new File(propertiesURL.getFile())));
            String connectionURL = (String) properties.get("datanucleus.ConnectionURL");
            
            if ( create ) {
                String createURL = null;
                if ( connectionURL.contains("database") ) {
                    createURL = connectionURL.replace("database", "");
                } else {
                    System.err.println("The conenctionURL string should use the name \"database\" which will be substituted for from the command line arguments." );
                    System.exit(-1);
                }
                String connectionUser = properties.getProperty("datanucleus.ConnectionUserName");
                String connectionPW = properties.getProperty("datanucleus.ConnectionPassword");
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                Connection conn = null;
                Statement statement = null;
                conn = DriverManager.getConnection(createURL+"?user="+connectionUser+"&password="+connectionPW); 
                statement=conn.createStatement();
                statement.executeUpdate("CREATE DATABASE IF NOT EXISTS "+database);
            }
            
            if ( connectionURL.contains("database") ) {
                connectionURL = connectionURL.replace("database", database);
            } else {
                System.err.println("The conenctionURL string should use the name \"database\" which will be substituted for from the command line arguments." );
                System.exit(-1);
            }
            
            properties.setProperty("datanucleus.ConnectionURL", connectionURL);
            pmf =  (JDOPersistenceManagerFactory) JDOHelper.getPersistenceManagerFactory(properties);
            PersistenceManager persistenceManager = pmf.getPersistenceManager();
            helper = new PersistenceHelper(persistenceManager);
        } catch ( ParseException e ) {
            System.err.println( e.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(width);
            formatter.printHelp("Cleaner", crawlerOptions, true);
            System.exit(-1);

        } 
    }
}