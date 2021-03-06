package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;
import gov.noaa.pmel.tmap.cleaner.crawler.DataCrawlOne;
import gov.noaa.pmel.tmap.cleaner.jdo.GeoAxis;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafDataset;
import gov.noaa.pmel.tmap.cleaner.jdo.NetCDFVariable;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.jdo.TimeAxis;
import gov.noaa.pmel.tmap.cleaner.jdo.VerticalAxis;

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
import javax.jdo.Transaction;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.joda.time.DateTime;

public class ScanOne {
    private static String parent;
    private static String url;
    private static String dataurl;
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
            parent = cl.getOptionValue("r");
            url = cl.getOptionValue("u");
            dataurl = cl.getOptionValue("l");
            String database = cl.getOptionValue("d");
            Properties properties = new Properties() ;
            URL propertiesURL =  ClassLoader.getSystemResource("datanucleus.properties");
            properties.load(new FileInputStream(new File(propertiesURL.getFile())));
            String connectionURL = (String) properties.get("datanucleus.ConnectionURL");
            if ( connectionURL.contains("database") ) {
                connectionURL = connectionURL.replace("database", database);
            } else {
                System.err.println("The conenctionURL string should use the name \"database\" which will be substituted for each catalog" );
                System.exit(-1);
            }
            properties.setProperty("datanucleus.ConnectionURL", connectionURL);
            JDOPersistenceManagerFactory pmf = (JDOPersistenceManagerFactory) JDOHelper.getPersistenceManagerFactory(properties);
            PersistenceManager persistenceManager = pmf.getPersistenceManager();
            System.out.println("Starting scan at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
            helper = new PersistenceHelper(persistenceManager);
            Transaction tx = helper.getTransaction();
            tx.begin();
            System.out.println("Scan for: "+url);
            DataCrawlOne dataCrawl = new DataCrawlOne(pmf, parent, url, dataurl, true);
            String result = dataCrawl.call();
            tx.commit();
            System.out.println("Ending scan at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
            // Check
            LeafDataset leaf = helper.getLeafDataset(dataurl);
            List<NetCDFVariable> variables = leaf.getVariables();
            if ( variables != null ) {
                for ( Iterator varIt = variables.iterator(); varIt.hasNext(); ) {
                    NetCDFVariable netCDFVariable = (NetCDFVariable) varIt.next();
                    System.out.println("\tNetCDFVariable: "+netCDFVariable.getDescription());
                    GeoAxis x = netCDFVariable.getxAxis();
                    System.out.println("\t\tX-Axis: "+x.getName()+" "+x.getMinValue()+" "+x.getMaxValue());
                    GeoAxis y = netCDFVariable.getyAxis();
                    System.out.println("\t\tY-Axis: "+y.getName()+" "+y.getMinValue()+" "+y.getMaxValue());
                    VerticalAxis z = netCDFVariable.getVerticalAxis();
                    if ( z != null ) System.out.println("\t\tZ-Axis: "+z.getName()+" "+z.getMinValue()+" "+z.getMaxValue());
                    TimeAxis t = netCDFVariable.getTimeAxis();
                    if ( t != null ) System.out.println("\t\tT-Axis: "+t.getTimeCoverageStart()+" "+t.getTimeCoverageEnd());
                }
            }
            helper.close();
        } catch ( ParseException e ) {
            System.err.println( e.getMessage() );
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(width);
            formatter.printHelp("Dumper", crawlerOptions, true);
            System.exit(-1);
        } catch ( FileNotFoundException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch ( IOException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch ( Exception e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
