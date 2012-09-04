package gov.noaa.pmel.tmap.catalogcleaner.jdo;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

@PersistenceCapable
public class CatalogComment {
    @Persistent
    @Column(length=500)
    private String url;
    @Persistent
    @Column(length=500)
    private String comment;
    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }
    public String getComment() {
        return comment;
    }
    public void setComment(String comment) {
        this.comment = comment;
    }
}
