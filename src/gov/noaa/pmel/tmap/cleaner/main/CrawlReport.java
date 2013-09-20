package gov.noaa.pmel.tmap.cleaner.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;

import gov.noaa.pmel.tmap.cleaner.cli.CrawlerOptions;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogComment;
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
import org.datanucleus.NullCallbackHandler;
import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.joda.time.DateTime;

import sun.security.action.GetLongAction;

public class CrawlReport extends Crawler {
   
    private static int totalfailed = 0;
    private static int totalscanned = 0;
    private static int totalnotscanned = 0;
    private static int totalnovariables = 0;
    
    private static List<String> missing = new ArrayList<String>();
    private static List<String> catnull = new ArrayList<String>();
    /**
     * @param args
     */
    public static void main(String[] args) {
        try {
            init(false, args);
            Catalog catalog;
            CatalogXML catalogXML;
            if ( url != null ) {
                catalog = helper.getCatalog(root, url);
                if ( catalog == null ) {
                    System.out.println("No catalog report for "+url+" in "+database);
                    System.exit(0);
                }
                catalogXML = helper.getCatalogXML(url);
                if ( catalogXML == null ) {
                    if ( !brief ) System.out.println("XML null for "+url);
                }
                System.out.println("Report for "+url);

            } else {
                catalog = helper.getCatalog(root, root);
                catalogXML = helper.getCatalogXML(root);
                if ( catalogXML == null ) {
                    System.out.println("XML null for "+root);
                }
                System.out.println("Report for "+root);
                if ( catalog == null ) {
                    System.out.println("No catalog report for "+root+" in "+database);
                    System.exit(0);
                }
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
                String vcheck = null;
                List<String> vchecks = new ArrayList<String>();
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
                    if ( varcheck && !brief) {
                        vcheck = varcheck(leafNodeReference);
                        vchecks.add(vcheck);
                        System.out.println("Total leaf data sets = "+count+" with "+totalscanned+" scanned "+totalnotscanned+" not scanned "+" and "+totalfailed+" failed and "+totalnovariables+" had no variables.");
                    }
                }
                System.out.println("Report generated at "+DateTime.now().toString("yyyy-MM-dd HH:mm:ss")+" for "+database+".");

                System.out.println("Root has "+leaves.size()+" OPeNDAP datasets with:\n\t"+scanned+" finished.\n\t"+notscanned+" not started.\n\t"+failed+" failed.\n\t"+novariables+" had no variables.");
                if ( refs != null && refs.size() > 0 ) {
                    System.out.println("\t"+refs.size()+" sub-catalogs.");
                } else {
                    System.out.println("\t0 sub-catalogs.");
                }
                for(int v = 0; v < vchecks.size(); v++) {
                    if ( !vchecks.get(v).equals("") ) {
                        System.out.println("\t"+vchecks.get(v));
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
            int bts = 0;
            int btns = 0;
            int btf = 0;
            int btnv = 0;
            int btot = 0;
            if ( brief ) {
                if ( url != null ) {
                    System.err.println("Cannot use the --brief option with a sub catalog.");
                    System.exit(0);
                }
                for (Iterator refIt = refs.iterator(); refIt.hasNext();) {
                    CatalogReference catalogReference = (CatalogReference) refIt.next();
                    bts = bts + totalscanned;
                    btns = btns + totalnotscanned;
                    btf = btf + totalfailed;
                    btnv = btnv + totalnovariables;
                    totalscanned = 0;
                    totalnotscanned = 0;
                    totalfailed = 0;
                    totalnovariables = 0;
                    Catalog child = helper.getCatalog(root, catalogReference.getUrl());
                    List<LeafNodeReference> childleaves = child.getLeafNodes();
                    int subtotal = childleaves.size();
                    for ( Iterator leafIt = childleaves.iterator(); leafIt.hasNext(); ) {
                        LeafNodeReference leafNodeReference = (LeafNodeReference) leafIt.next();
                        if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.FAILED) {
                            totalfailed++;
                        } else if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.NOT_STARTED ) {
                            totalnotscanned++;
                        } else if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.FINISHED) {
                            totalscanned++;
                        } else if ( leafNodeReference.getDataCrawlStatus() == DataCrawlStatus.NO_VARIABLES_FOUND) {
                            totalnovariables++;
                        }
                    }
                    count = report(subtotal, level, catalogReference.getUrl(), child.getCatalogRefs());
                    btot = btot+count;

                    System.out.println("\t\t"+child.getUrl()+" has:");
                    System.out.println("\t\t\tleaf data sets = "+count+" with\n\t\t\t\t"+totalscanned+" scanned.\n\t\t\t\t"+totalnotscanned+" not scanned.\n\t\t\t\t"+totalfailed+" failed.\n\t\t\t\t"+totalnovariables+" had no variables.");
                }
            } else {
                count = count + report(0, level, catalog.getUrl(), refs);
            }
            helper.close();
            if ( brief ) {
                System.out.println("Total leaf data sets = "+btot+" with:\n\t"+bts+" scanned.\n\t"+btns+" not scanned.\n\t"+btf+" failed.\n\t"+btnv+" had no variables.");
            } else {
                System.out.println("Total leaf data sets = "+count+" with "+totalscanned+" scanned "+totalnotscanned+" not scanned "+" and "+totalfailed+" failed and "+totalnovariables+" had no variables.");
            }
            if ( missing.size() > 0 ) {
                System.out.println("\nThe following catalogs are listed in the master catalog, but are not in the data store.");
                for (Iterator missIt = missing.iterator(); missIt.hasNext();) {
                    String m = (String) missIt.next();
                    System.out.println("\t\t"+m);
                }
            }
            if ( catnull.size() > 0 ) {
                System.out.println("\nThe following catalogs are listed in the master catalog, but their XML contents is null in the data store.");
                for (Iterator catIt = catnull.iterator(); catIt.hasNext();) {
                    String c = (String) catIt.next();
                    System.out.println("\t\t"+c);
                }
            }
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Unable to initialize.  "+e.getMessage());
        }
    }
    private static int report(int total, int level, String parent, List<CatalogReference> refs) {
        for ( Iterator iterator = refs.iterator(); iterator.hasNext(); ) {
            CatalogReference catalogReference = (CatalogReference) iterator.next();
            if ( !skip(catalogReference) ) {
                Catalog sub = null;
                CatalogXML subXML = null;
                try {
                    sub = helper.getCatalog(parent, catalogReference.getUrl());
                    subXML = helper.getCatalogXML(catalogReference.getUrl());
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                if ( subXML == null ) {
                    catnull.add(catalogReference.getUrl());
                }
                if ( sub !=  null ) {
                    List<LeafNodeReference> leaves = sub.getLeafNodes();
                    int failed = 0;
                    int notscanned = 0;
                    int scanned = 0;
                    int novariables = 0;
                    String vcheck = null;
                    List<String> vchecks = new ArrayList();
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
                        if ( varcheck && !brief ) {
                            vcheck = varcheck(leafNodeReference);
                            vchecks.add(vcheck);
                        }
                    }
                    if ( leaves != null && leaves.size() > 0 ) {
                        String blanks = "";
                        for ( int i = 0; i < level;  i++ ) {
                            blanks = blanks + "  ";
                        }
                        if ( !brief ) {
                            System.out.println(blanks+sub.getUrl()+" has:\n\t"+leaves.size()+" OPeNDAP datasets with "+scanned+" finished "+notscanned+" not started "+failed+" failed and "+novariables+" had no variables.");
                        }
                        
                        List<CatalogReference> subrefs = sub.getCatalogRefs();
                        if ( !brief ) {
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
                            for ( Iterator refIt = refs.iterator(); refIt.hasNext(); ) {
                                CatalogReference aSubRef = (CatalogReference) refIt.next();
                                System.out.println("\t\tCatalog Reference: "+aSubRef.getUrl());
                            }
                        }
                        for(int v = 0; v < vchecks.size(); v++) {
                            if ( !vchecks.get(v).equals("") ) {
                                System.out.println("\t"+blanks+vchecks.get(v));
                            }
                        }
                    }
                    total = report(total, level, sub.getUrl(), sub.getCatalogRefs());

                } else {
                    String blanks = "";
                    for ( int i = 0; i < level;  i++ ) {
                        blanks = blanks + "  ";
                    }
                    missing.add(catalogReference.getUrl());
                }
            }
        }
        level++;
        return total;
    }
    private static String varcheck(LeafNodeReference leafNodeReference) {

        LeafDataset leaf = helper.getLeafDataset(leafNodeReference.getUrl());
        if ( leaf != null ) {
            List<NetCDFVariable> vars = leaf.getBadVariables();
            StringBuilder failvars = new StringBuilder();
            if (vars.size() > 0 ) {
                failvars.append("Dataset "+leaf.getUrl()+" failed with the following errors.");
            }
            for (Iterator badIt = vars.iterator(); badIt.hasNext();) {
                NetCDFVariable netCDFVariable = (NetCDFVariable) badIt.next();
                failvars.append("\n\t Variable "+netCDFVariable.getDescription()+" failed. "+netCDFVariable.getError());
            }
            List<NetCDFVariable> goodVars = leaf.getVariables();
            if ( goodVars.size() == 0 && vars.size() == 0 ) {
                failvars.append("Dataset "+leaf.getUrl()+" was scanned, but no CF variables where found.");
            }
            CatalogComment comment = leaf.getComment();
            if ( comment != null ) {
                String c = comment.getComment();
                if ( c != null && !c.equals("") ) {
                    failvars.append(" "+c);
                }
            }
            return failvars.toString();
           
        } else {
            return "Leaf node was null in the data store.";
        }
    }
}
