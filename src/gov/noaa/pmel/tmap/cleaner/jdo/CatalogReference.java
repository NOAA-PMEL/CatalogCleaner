package gov.noaa.pmel.tmap.cleaner.jdo;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class CatalogReference {
    
    @Persistent
    @Column(length=500)
    private String url;
    @Persistent
    @Column(length=500)
    private String originalUrl;

    public CatalogReference(String originalUrl, String url) {
        super();
        this.url = url;
        this.originalUrl = originalUrl;
    }

    public String getOriginalUrl() {
        return originalUrl;
    }

    public void setOriginalUrl(String originalUrl) {
        this.originalUrl = originalUrl;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
    
    @Override
    public String toString() {
        return url;
    }
}
