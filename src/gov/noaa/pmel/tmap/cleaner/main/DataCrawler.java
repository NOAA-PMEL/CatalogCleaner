package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.crawler.CrawlableLeafNode;
import gov.noaa.pmel.tmap.cleaner.crawler.DataCrawlOne;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafDataset;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference.DataCrawlStatus;
import gov.noaa.pmel.tmap.cleaner.jdo.NetCDFVariable;
import gov.noaa.pmel.tmap.cleaner.jdo.TimeAxis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

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

public class DataCrawler extends Crawler {
   
    private static List<CrawlableLeafNode> dataSources = new ArrayList<CrawlableLeafNode>();
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
            init(false, args);
           
            System.out.println("Starting data crawl work at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
            
            if ( url == null ) {
                url = root;
            }
            
            Catalog rootCatalog = helper.getCatalog(root, url);
            List<LeafNodeReference> rootLeafNodes = rootCatalog.getLeafNodes();
            if ( rootLeafNodes != null ) {
                System.out.println("Looking at leaf data sets in "+rootCatalog.getUrl());
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
                LeafNodeReference ref = cln.getLeafNodeReference();
                if ( force || varcheck || ref.getDataCrawlStatus() == DataCrawlStatus.NO_VARIABLES_FOUND || ref.getDataCrawlStatus() == DataCrawlStatus.NOT_STARTED || ref.getDataCrawlStatus() == DataCrawlStatus.FAILED ) {
                    DataCrawlOne one = new DataCrawlOne(pmf, cln.getRoot(), cln.getParent(), cln.getLeafNodeReference(), force);
                    completionPool.submit(one);
                    total++;
                }
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
    public static void gatherReferences(String parent, List<CatalogReference> refs) {
        Map<String, List<LeafNodeReference>> subReferences = new HashMap<String, List<LeafNodeReference>>();
        try {
            for ( Iterator refsIt = refs.iterator(); refsIt.hasNext(); ) {
                CatalogReference catalogReference = (CatalogReference) refsIt.next();
                Catalog sub = helper.getCatalog(parent, catalogReference.getUrl());
                boolean xclud = false;
                for ( int i = 0; i < exclude.size(); i++ ) {
                    if ( Pattern.matches(exclude.get(i), catalogReference.getUrl())) {
                        xclud = true;
                    }
                }
                if (!xclud &&  sub != null && !excludeCatalog.contains(catalogReference.getUrl())) {
                    List<LeafNodeReference> l = sub.getLeafNodes();
                    if ( l != null ) {
                        System.out.println("Looking at leaf data sets in "+sub.getUrl());
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
