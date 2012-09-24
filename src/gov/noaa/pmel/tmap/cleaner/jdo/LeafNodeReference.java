package gov.noaa.pmel.tmap.cleaner.jdo;

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
    @Column(length=500)
    private String urlPath;
    
    @Persistent
    DataCrawlStatus dataCrawlStatus;
    
    @Persistent
    String crawlDate;
    
    public enum DataCrawlStatus {
        NOT_STARTED,
        DO_NOT_CRAWL,
        FINISHED,
        FAILED
    }

    public LeafNodeReference(String url, String urlPath, DataCrawlStatus status) {
        super();
        this.url = url;
        this.urlPath = urlPath;
        this.dataCrawlStatus = status;
    }

    public String getUrlPath() {
        return urlPath;
    }

    public void setUrlPath(String urlPath) {
        this.urlPath = urlPath;
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
