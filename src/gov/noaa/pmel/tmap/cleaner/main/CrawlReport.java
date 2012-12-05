package gov.noaa.pmel.tmap.cleaner.main;

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

import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogXML;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafDataset;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.cleaner.jdo.NetCDFVariable;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference.DataCrawlStatus;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.joda.time.DateTime;

import sun.security.action.GetLongAction;

public class CrawlReport extends Crawler {
   
    private static int totalfailed = 0;
    private static int totalscanned = 0;
    private static int totalnotscanned = 0;
    private static int totalnovariables = 0;
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            init(false, args);
            Catalog catalog = helper.getCatalog(root, root);
            CatalogXML catalogXML = helper.getCatalogXML(root);
            if ( catalogXML == null ) {
                System.out.println("XML null for "+root);
            }
            System.out.println("Report for "+root);
            if ( catalog == null ) {
                System.out.println("No catalog report for "+root+" in "+database);
                System.exit(0);
            }
            List<LeafNodeReference> leaves = catalog.getLeafNodes();
            List<CatalogReference> refs = catalog.getCatalogRefs();
            int count = 0;
            
            int level = 1;
            if ( leaves != null ) {
                int scanned = 0;
                int notscanned = 0;
                int failed = 0;
                int novariables = 0;
                for ( Iterator leafIt = leaves.iterator(); leafIt.hasNext(); ) {
                    LeafNodeReference leafNodeReference = (LeafNodeReference) leafIt.next();
                    if ( full ) System.out.println("\t\t"+leafNodeReference.getUrl());
                    if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.FAILED) {
                        failed++;
                        totalfailed++;
                    } else if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.NOT_STARTED ) {
                        notscanned++;
                        totalnotscanned++;
                    } else if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.FINISHED) {
                        scanned++;
                        totalscanned++;
                    } else if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.NO_VARIABLES_FOUND) {
                        novariables++;
                        totalnovariables++;
                    }
//                    if ( varcheck ) {
//                        LeafDataset leaf = helper.getLeafDataset(root, leafNodeReference.getUrl());
//                        List<NetCDFVariable> vars = leaf.getVariables();
//                        if ( vars == null || vars.size() == 0 ) {
//                            System.out.println("-r "+root+" -u "+root+" -l "+leafNodeReference.getUrl());
//                        }
//                    }
                }
                System.out.println("Report generated at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss")+" for "+database+".");
                if ( varcheck ) {
                    if ( novariables > 0 ) {
                        System.out.println("Root has: \n\t"+leaves.size()+" OPeNDAP datasets with "+scanned+" finished "+notscanned+" not started "+failed+" failed and "+novariables+" had no variables.");
                    }
                } else {
                    System.out.println("Root has: \n\t"+leaves.size()+" OPeNDAP datasets with "+scanned+" finished "+notscanned+" not started "+failed+" failed and "+novariables+" had no variables.");
                    if ( refs != null && refs.size() > 0 ) {
                        System.out.println("\t"+refs.size()+" sub-catalogs.");
                    } else {
                        System.out.println("\t0 sub-catalogs.");
                    }
                }
                count = count + leaves.size();
                if ( full ) {
                    for ( Iterator leafIt = leaves.iterator(); leafIt.hasNext(); ) {
                        LeafNodeReference leafNodeReference = (LeafNodeReference) leafIt.next();
                        System.out.println("\t\tLeaf Data Set: "+leafNodeReference.getUrl());
                    }
                    for ( Iterator refIt = refs.iterator(); refIt.hasNext(); ) {
                        CatalogReference catalogReference = (CatalogReference) refIt.next();
                        System.out.println("\t\tCatalog Reference: "+catalogReference.getUrl());
                    }
                }
            }
            count = count + report(count, level, catalog.getUrl(), catalog.getCatalogRefs());
            helper.close();
            System.out.println("Total leaf data sets = "+count+" with "+totalscanned+" scanned "+totalnotscanned+" not scanned "+" and "+totalfailed+" failed and "+totalnovariables+" had no variables.");
            System.exit(0);
        } catch (Exception e) {
            System.err.println("Unable to initialize.  "+e.getMessage());
        }
    }
    private static int report(int total, int level, String parent, List<CatalogReference> refs) {
        for ( Iterator iterator = refs.iterator(); iterator.hasNext(); ) {
            CatalogReference catalogReference = (CatalogReference) iterator.next();
            Catalog sub = helper.getCatalog(parent, catalogReference.getUrl());
            CatalogXML subXML = helper.getCatalogXML(catalogReference.getUrl());
            if ( subXML == null ) {
                System.out.println("XML null for "+ catalogReference.getUrl());
            }
            if ( sub !=  null ) {
                List<LeafNodeReference> leaves = sub.getLeafNodes();
                int failed = 0;
                int notscanned = 0;
                int scanned = 0;
                int novariables = 0;
                for ( Iterator leafIt = leaves.iterator(); leafIt.hasNext(); ) {
                    LeafNodeReference leafNodeReference = (LeafNodeReference) leafIt.next();
                    if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.FAILED) {
                        failed++;
                        totalfailed++;
                    } else if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.NOT_STARTED ) {
                        notscanned++;
                        totalnotscanned++;
                    } else if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.FINISHED) {
                        scanned++;
                        totalscanned++;
                    } else if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.NO_VARIABLES_FOUND ) {
                        novariables++;
                        totalnovariables++;
                    }
//                    if ( varcheck ) {
//                        LeafDataset leaf = helper.getLeafDataset(sub.getUrl(), leafNodeReference.getUrl());
//                        List<NetCDFVariable> vars = leaf.getVariables();
//                        if ( vars == null || vars.size() == 0 ) {
//                            System.out.println("-r "+parent+" -u "+sub.getUrl()+" -l "+leafNodeReference.getUrl());
//                        }
//                    }
                }
                if ( leaves != null && leaves.size() > 0 ) {
                    String blanks = "";
                    for ( int i = 0; i < level;  i++ ) {
                        blanks = blanks + "  ";
                    }
                    if ( varcheck ) {
                        if ( novariables > 0 ) {
                            System.out.println(blanks+sub.getUrl()+" has:\n\t"+leaves.size()+" OPeNDAP datasets with "+scanned+" finished "+notscanned+" not started "+failed+" failed and "+novariables+" had no variables.");
                        }
                    } else { 
                        System.out.println(blanks+sub.getUrl()+" has:\n\t"+leaves.size()+" OPeNDAP datasets with "+scanned+" finished "+notscanned+" not started "+failed+" failed and "+novariables+" had no variables.");
                        List<CatalogReference> subrefs = sub.getCatalogRefs();
                        if ( subrefs != null && subrefs.size() > 0 ) {
                            System.out.println(blanks+"\t"+subrefs.size()+" sub-catalogs.");
                        } else {
                            System.out.println("\t0 sub-catalogs.");
                        }
                    }
                    total = total + leaves.size();
                    if ( full ) {
                        for ( Iterator leafIt = leaves.iterator(); leafIt.hasNext(); ) {
                            LeafNodeReference leafNodeReference = (LeafNodeReference) leafIt.next();
                            System.out.println("\t\tLeaf Data Set: "+leafNodeReference.getUrl());
                        }
                        List<CatalogReference> subrefs = sub.getCatalogRefs();
                        for ( Iterator refIt = subrefs.iterator(); refIt.hasNext(); ) {
                            CatalogReference cr = (CatalogReference) refIt.next();
                            System.out.println("\t\tCatalog Reference: "+cr.getUrl());
                        }
                    }
                }
                total = report(total, level, sub.getUrl(), sub.getCatalogRefs());
            } else {
                String blanks = "";
                for ( int i = 0; i < level;  i++ ) {
                    blanks = blanks + "  ";
                }
                System.out.println(blanks+catalogReference.getUrl()+" is not in the database.");
            }
        }
        level++;
        return total;
    }
}
