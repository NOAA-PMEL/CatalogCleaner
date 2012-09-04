package gov.noaa.pmel.tmap.catalogcleaner.jdo;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class LeafNodeReference {

    @Persistent
    @Column(length=500)
    private String url;
    
    @Persistent
    DataCrawlStatus dataCrawlStatus;
    
    @Persistent
    String crawlDate;
    
    public enum DataCrawlStatus {
        NOT_STARTED,
        FINISHED,
        FAILED
    }

    public LeafNodeReference(String url) {
        super();
        this.url = url;
        this.dataCrawlStatus = DataCrawlStatus.NOT_STARTED;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    
    public String getCrawlDate() {
        return crawlDate;
    }

    public void setCrawlDate(String crawlDate) {
        this.crawlDate = crawlDate;
    }
    

    public DataCrawlStatus getDataCrawlStatus() {
        return dataCrawlStatus;
    }

    public void setDataCrawlStatus(DataCrawlStatus dataCrawlStatus) {
        this.dataCrawlStatus = dataCrawlStatus;
    }

    @Override
    public String toString() {
        return url;
    }

}
