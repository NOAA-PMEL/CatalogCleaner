package gov.noaa.pmel.tmap.cleaner.crawler;

import java.util.List;

import gov.noaa.pmel.tmap.cleaner.jdo.LeafDataset;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.cleaner.jdo.NetCDFVariable;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference.DataCrawlStatus;

import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.joda.time.DateTime;

public class TimeCrawlLeafNode extends DataCrawl {
    LeafNodeReference leafNodeReference;
    LeafDataset leafDataset;
    public TimeCrawlLeafNode(JDOPersistenceManagerFactory pmf, LeafNodeReference leafNodeReference, LeafDataset leafDataset) {
        super(pmf, null, null, leafNodeReference.getUrl(), true);
        this.leafNodeReference = leafNodeReference;
        this.leafDataset = leafDataset;
    }

    @Override
    public String call() throws Exception {
        crawlLeafNode(leafDataset, null, null);
        List<NetCDFVariable> vars = leafDataset.getVariables();
        if ( vars != null && vars.size() > 0 ) {
            leafNodeReference.setDataCrawlStatus(DataCrawlStatus.FINISHED);
        } else {
            leafNodeReference.setDataCrawlStatus(DataCrawlStatus.NO_VARIABLES_FOUND);
        }
        leafNodeReference.setCrawlDate(DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
        return leafNodeReference.getUrl();
    }

}
