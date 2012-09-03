package gov.noaa.pmel.tmap.catalogcleaner.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import gov.noaa.pmel.tmap.catalogcleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;

import sun.security.action.GetLongAction;

public class TreeCrawlReport {
     private static String root;
    private static PersistenceHelper helper;
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
            String database = cl.getOptionValue("d");
            Properties properties = new Properties() ;
            URL propertiesURL =  ClassLoader.getSystemResource("datanucleus.properties");
            properties.load(new FileInputStream(new File(propertiesURL.getFile())));
            String connectionURL = (String) properties.get("datanucleus.ConnectionURL");
            if ( connectionURL.contains("database") ) {
                connectionURL = connectionURL.replace("database", database);
            } else {
                System.err.println("The conenctionURL string should use the name \"databast\" which will be substituted for each catalog" );
                System.exit(-1);
            }
            properties.setProperty("datanucleus.ConnectionURL", connectionURL);
            JDOPersistenceManagerFactory pmf = (JDOPersistenceManagerFactory) JDOHelper.getPersistenceManagerFactory(properties);
            PersistenceManager persistenceManager = pmf.getPersistenceManager();
            helper = new PersistenceHelper(persistenceManager);
            Catalog catalog = helper.getCatalog(root, root);
            System.out.println("Report for "+root);
            if ( catalog == null ) {
                System.out.println("No catalog report for "+root+" in "+database);
                System.exit(0);
            }
            List<LeafNodeReference> leaves = catalog.getLeafNodes();
            int count = 0;
            int level = 1;
            if ( leaves != null ) {
                System.out.println("Root has "+leaves.size()+" OPeNDAP datasets.");
                count = count + leaves.size();
            }
            count = count + report(count, level, catalog.getCatalogRefs());
            System.out.println("Total leaf data sets = "+count);
        } catch ( ParseException e ) {
            System.err.println( e.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(width);
            formatter.printHelp("TreeCrawler", crawlerOptions, true);
            System.exit(-1);
        } catch ( FileNotFoundException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch ( IOException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    private static int report(int total, int level, List<CatalogReference> refs) {
        for ( Iterator iterator = refs.iterator(); iterator.hasNext(); ) {
            CatalogReference catalogReference = (CatalogReference) iterator.next();
            Catalog sub = helper.getCatalog(root, catalogReference.getUrl());
            List<LeafNodeReference> leaves = sub.getLeafNodes();
            if ( leaves != null && leaves.size() > 0 ) {
                String blanks = "";
                for ( int i = 0; i < level;  i++ ) {
                    blanks = blanks + "  ";
                }
                System.out.println(blanks+sub.getUrl()+" has "+leaves.size()+" OPeNDAP datasets");
                total = total + leaves.size();
            }
            total = report(total, level, sub.getCatalogRefs());
        }
        level++;
        return total;
    }
}
