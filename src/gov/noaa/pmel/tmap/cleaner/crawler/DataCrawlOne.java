package gov.noaa.pmel.tmap.cleaner.crawler;

import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference.DataCrawlStatus;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;

import java.util.Properties;
import java.util.concurrent.Callable;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.joda.time.DateTime;

public class DataCrawlOne extends DataCrawl implements Callable<String> {
    private LeafNodeReference leafNodeReference;
    public DataCrawlOne(JDOPersistenceManagerFactory pmf, String root, String parent, LeafNodeReference leafNodeReference, boolean force) {
        
        super(pmf, root, parent, leafNodeReference.getUrl(), force);
        this.leafNodeReference = leafNodeReference;
        
        
    }
    public DataCrawlOne(JDOPersistenceManagerFactory pmf, String parent, String url, String dataurl, boolean force) {
        super(pmf, parent, url, dataurl, force);
        
    }
    @Override
    public String call() throws Exception {

        PersistenceManager persistenceManager = pmf.getPersistenceManager();
        helper = new PersistenceHelper(persistenceManager);
        Transaction tx = helper.getTransaction();
        if ( leafNodeReference == null ) {
            leafNodeReference = helper.getLeafNodeReference(parent, leafurl);
        }
        tx.begin();
        System.out.println("Crawling "+leafNodeReference.getUrl()+" in thread "+Thread.currentThread().getId());
        try {
            crawlLeafNode(url, leafurl);
            leafNodeReference.setDataCrawlStatus(DataCrawlStatus.FINISHED);
            leafNodeReference.setCrawlDate(DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        } catch ( Exception e ) {
            System.err.println("Failed to save "+parent+"\n\t"+leafNodeReference.getUrl()+"\n\t for "+e.getLocalizedMessage());
            leafNodeReference.setDataCrawlStatus(DataCrawlStatus.FAILED);
        }
        tx.commit();
        helper.close();
        return leafurl;
    }
}
