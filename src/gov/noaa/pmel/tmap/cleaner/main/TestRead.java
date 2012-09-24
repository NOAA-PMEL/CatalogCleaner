package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogXML;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.xml.JDOMUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;

import thredds.catalog.InvCatalog;
import thredds.catalog.InvCatalogFactory;
import thredds.catalog.InvDataset;
import thredds.catalog.InvMetadata;

public class TestRead {
    private static PersistenceHelper helper;
    /**
     * @param args
     */
    public static void main(String[] args) {
        String url = "http://colossus.dl.stevens-tech.edu:8080/thredds/catalog/fmrc/NYBight/catalog.xml";
        CrawlerOptions crawlerOptions = new CrawlerOptions();
        CommandLineParser parser = new GnuParser();
        CommandLine cl = null;
        int width = 80;
        try {
            cl = parser.parse(crawlerOptions, args);

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
            Transaction tx = helper.getTransaction();
            tx.begin();
            Catalog catalog = helper.getCatalog(url, url);
            CatalogXML catalogXML = helper.getCatalogXML(url);
            String xml = catalogXML.getXml();
            InvCatalogFactory catfactory = new InvCatalogFactory("default", false);
            InvCatalog thredds = (InvCatalog) catfactory.readXML(xml, new URI(catalog.getUrl()));
            List<InvDataset> rootInvDatasets = thredds.getDatasets();       
            findMetadata(rootInvDatasets);
        } catch ( FileNotFoundException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch ( IOException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch ( ParseException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch ( URISyntaxException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    private static void findMetadata(List<InvDataset> datasets) {
        for ( Iterator dsIt = datasets.iterator(); dsIt.hasNext(); ) {
            InvDataset invDataset = (InvDataset) dsIt.next();
            List<InvMetadata> metaData = invDataset.getMetadata();
            if ( metaData != null && metaData.size() > 0 ) {
                System.out.println(invDataset.getName() + "has metadata.");
            }
            if ( invDataset.hasNestedDatasets() ) {
                findMetadata(invDataset.getDatasets());
            }
        }
    }
}
