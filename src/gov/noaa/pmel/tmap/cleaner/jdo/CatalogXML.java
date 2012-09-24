package gov.noaa.pmel.tmap.cleaner.jdo;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class CatalogXML {
    @PrimaryKey
    @Persistent
    @Column(length=500)
    private String url;
    @Persistent
    @Column(jdbcType="LONGVARCHAR")
    private String xml;
    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }
    public String getXml() {
        return xml;
    }
    public void setXml(String xml) {
        this.xml = xml;
    }
    @Override
    public String toString() {
        return url;
    }
}
