package gov.noaa.pmel.tmap.cleaner.jdo;

import java.util.List;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.IdGeneratorStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

@PersistenceCapable
public class LeafDataset {
    
    @Persistent
    @Column(length=500)
    private String url;
    
//    @Persistent
//    @Column(length=500)
//    private String parent;
    
    @Persistent
    long crawlStartTime = 0l;
    
    @Persistent
    long crawlEndTime = 0l;
    
    @Persistent
    CatalogComment comment;
    
    @Persistent
    List<NetCDFVariable> variables;
    
    @Persistent
    List<NetCDFVariable> badVariables;
    

    public LeafDataset(String url) {
        this.url = url;
    }

    public CatalogComment getComment() {
        return comment;
    }

    public void setComment(CatalogComment comment) {
        this.comment = comment;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<NetCDFVariable> getVariables() {
        return variables;
    }

    public void setVariables(List<NetCDFVariable> variables) {
        this.variables = variables;
    }

    public long getCrawlStartTime() {
        return crawlStartTime;
    }

    public void setCrawlStartTime(long crawlStartTime) {
        this.crawlStartTime = crawlStartTime;
    }

    public long getCrawlEndTime() {
        return crawlEndTime;
    }

    public void setCrawlEndTime(long crawlEndTime) {
        this.crawlEndTime = crawlEndTime;
    }

    public List<NetCDFVariable> getBadVariables() {
        return badVariables;
    }

    public void setBadVariables(List<NetCDFVariable> badVariables) {
        this.badVariables = badVariables;
    }

    public String getRepresentativeTime() {
        if ( variables != null && variables.size() > 0 ) {
            NetCDFVariable one = variables.get(0);
            TimeAxis ta = one.getTimeAxis();
            if ( ta != null ) {
                return ta.getTimeCoverageStart();
            } else {
                return null;
            }
        } else {
            return null;
        }

    }
    

}
