package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;
import gov.noaa.pmel.tmap.cleaner.crawler.CrawlableLeafNode;
import gov.noaa.pmel.tmap.cleaner.crawler.DataCrawlOne;
import gov.noaa.pmel.tmap.cleaner.crawler.TimeCrawlLeafNode;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafDataset;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference.DataCrawlStatus;
import gov.noaa.pmel.tmap.cleaner.jdo.NetCDFVariable;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.jdo.TimeAxis;
import gov.noaa.pmel.tmap.cleaner.util.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
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
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.GJChronology;
import org.joda.time.chrono.GregorianChronology;
import org.joda.time.chrono.JulianChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import uk.ac.rdg.resc.edal.time.AllLeapChronology;
import uk.ac.rdg.resc.edal.time.NoLeapChronology;
import uk.ac.rdg.resc.edal.time.ThreeSixtyDayChronology;

public class TimeUpdateCrawler extends Crawler {
    /*
     
     This query should give a reasonable list of datasets that need to have their time checked.
     
select url,timecoverageend from leafdataset, netcdfvariable, timeaxis where netcdfvariable.variables_leafdataset_id_oid=leafdataset.leafdataset_id AND netcdfvariable.timeaxis_timeaxis_id_oid=timeaxis_id AND timecoverageend like "%2012%";
     
     */
    private static final String patterns[] = {
        "yyyy-MM-dd", "yyyy-MM-dd", "yyyy-MM-dd", "yyyy-MM-dd",
        "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss'Z'",
        "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss"};
    private static DateTime now = DateTime.now().withZone(DateTimeZone.UTC);
    private static int yr = now.getYear();
    private static String startOfYear = yr+"-01-01 00:00:00";
    private static DateTimeFormatter fmtwh = DateTimeFormat.forPattern(patterns[4]).withZoneUTC();
    private static DateTime then = fmtwh.parseDateTime(startOfYear).withZone(DateTimeZone.UTC);
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            List<String> updates = new ArrayList<String>();
            init(false, args);      
            System.out.println("Starting time update data crawl work at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
            // Get the data sets that need updating, then work backward to the references to update their status and update time.
            int total = 0;
            Transaction tx = helper.getTransaction();
            tx.begin();
            List<LeafDataset> datasets = helper.getDatasetsEndingInYear(String.valueOf(yr));
            System.out.println(datasets.size()+" dataset found ending in "+yr);
            for ( Iterator leafIt = datasets.iterator(); leafIt.hasNext(); ) {
                LeafDataset leafDataset = (LeafDataset) leafIt.next();
                updates.add(leafDataset.getUrl());
            }
            tx.commit();
            for ( Iterator updateIt = updates.iterator(); updateIt.hasNext(); ) {
                String url = (String) updateIt.next();
                TimeCrawlLeafNode timeCrawlLeafNode = new TimeCrawlLeafNode(pmf, url);
                System.out.println("Queuing "+ url);
                completionPool.submit(timeCrawlLeafNode);
                total++;
            }
            System.out.println(total+" data sources queued for processing.");
            for ( int i = 0; i < total; i++) {
                Future<String> f = completionPool.take();
                String leaf = f.get();
                System.out.println("Finished with "+leaf);
            }
            System.out.println("All work complete.  Shutting down at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
            helper.close();
            shutdown(0);
        } catch ( Exception e ) {
            System.out.println("Error with "+e.getLocalizedMessage());
            e.printStackTrace();
        } finally {
            shutdown(0);
        }
    }
    /**
     * 
     * @deprecated -- I'm not sure we need to use this, but it provides a more detailed time range check...
     * @param dataset
     * @return
     */
    public static boolean checkTimeRange(LeafDataset dataset) {
        List<NetCDFVariable> vars = dataset.getVariables();
        for ( Iterator varIt = vars.iterator(); varIt.hasNext(); ) {
            NetCDFVariable var = (NetCDFVariable) varIt.next();
            TimeAxis t = var.getTimeAxis();
            String calendar = t.getCalendar();
            String timeRangeStart = t.getTimeCoverageStart();
            String timeRangeEnd = t.getTimeCoverageEnd();
            Chronology chrono = GJChronology.getInstance(DateTimeZone.UTC);
            if ( calendar != null ) {
                // If calendar attribute is set, use appropriate Chronology.
                if (calendar.equals("proleptic_gregorian") ) {
                    chrono = GregorianChronology.getInstance(DateTimeZone.UTC);
                } else if (calendar.equals("noleap") || calendar.equals("365_day") ) {
                    chrono = NoLeapChronology.getInstanceUTC();
                } else if (calendar.equals("julian") ) {
                    chrono = JulianChronology.getInstance(DateTimeZone.UTC);
                } else if ( calendar.equals("all_leap") || calendar.equals("366_day") ) {
                    chrono = AllLeapChronology.getInstanceUTC();
                } else if ( calendar.equals("360_day") ) {  /* aggiunto da lele */
                    chrono = ThreeSixtyDayChronology.getInstanceUTC();
                }
            }
            
            for ( int i = 0; i < patterns.length; i++ ) {
                try {
                    DateTimeFormatter fmt = DateTimeFormat.forPattern(patterns[i]);
                    DateTime end = fmt.parseDateTime(timeRangeEnd).withChronology(chrono).withZone(DateTimeZone.UTC);
                    
                    if ( end.isAfter(then) ) {
                        return true;
                    }
                } catch ( Exception e ) {
                    // Try a differnt pattern...
                }
            }
           
        }
        return false;
    }
    
    public static void shutdown(int code) {
        System.out.println("All work complete.  Shutting down at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        pool.shutdown();
        System.exit(code);
    }
}
