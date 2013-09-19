package gov.noaa.pmel.tmap.cleaner.crawler;

import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import gov.noaa.pmel.tmap.cleaner.jdo.LeafDataset;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.cleaner.jdo.NetCDFVariable;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference.DataCrawlStatus;

import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.joda.time.DateTime;

public class TimeCrawlLeafNode extends DataCrawl {
    public TimeCrawlLeafNode(JDOPersistenceManagerFactory pmf, String leafurl) {
        super(pmf, null, null, leafurl, true);
    }

    @Override
    public String call() throws Exception {
        try {
            PersistenceManager persistenceManager = pmf.getPersistenceManager();
            helper = new PersistenceHelper(persistenceManager);
            Transaction tx = helper.getTransaction();
            tx.begin();
            LeafDataset leafDataset = helper.getLeafDataset(leafurl);
            LeafNodeReference leafNodeReference = helper.getLeafNodeReference(leafurl);
            System.out.println("Crawling "+leafNodeReference.getUrl()+" in thread "+Thread.currentThread().getId());
            updateLeafNodeTime(leafDataset);
            List<NetCDFVariable> vars = leafDataset.getVariables();
            if ( vars != null && vars.size() > 0 ) {
                leafNodeReference.setDataCrawlStatus(DataCrawlStatus.FINISHED);
            } else {
                leafNodeReference.setDataCrawlStatus(DataCrawlStatus.NO_VARIABLES_FOUND);
            }
            leafNodeReference.setCrawlDate(DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
            tx.commit();
        } catch ( Exception e ) {
            System.err.println("Failed to update "+leafurl+" with "+e.getLocalizedMessage());
        }
        helper.close();
        System.out.println("Returning from "+leafurl);
        return leafurl;
    }

}
