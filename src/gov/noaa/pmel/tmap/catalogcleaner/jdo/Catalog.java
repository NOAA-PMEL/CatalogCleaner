package gov.noaa.pmel.tmap.catalogcleaner.jdo;

import java.util.List;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
import javax.jdo.annotations.PrimaryKey;

@PersistenceCapable
public class Catalog {
    
    @Persistent
    @Column(length=500)
    private String url;
    
    @Persistent
    @Column(length=500)
    private String parent;

    @Persistent
    private List<CatalogReference> catalogRefs;
    
    @Persistent
    private List<LeafNodeReference> leafNodes;

    public String getParent() {
        return parent;
    }

    public void setParent(String parent) {
        this.parent = parent;
    }
    @Persistent
    private String version;
    
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public List<CatalogReference> getCatalogRefs() {
        return catalogRefs;
    }

    public void setCatalogRefs(List<CatalogReference> catalogRefs) {
        this.catalogRefs = catalogRefs;
    }

    public List<LeafNodeReference> getLeafNodes() {
        return leafNodes;
    }

    public void setLeafNodes(List<LeafNodeReference> leafNodes) {
        this.leafNodes = leafNodes;
    }
    @Override
    public String toString() {
        return url;
    }
}
