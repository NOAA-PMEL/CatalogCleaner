package gov.noaa.pmel.tmap.cleaner.crawler;

import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;

public class CrawlableLeafNode {
    private String root;
    private String parent;
    private LeafNodeReference leafNodeReference;
    
    
    public CrawlableLeafNode(String root, String parent, LeafNodeReference leafNodeReference) {
        super();
        this.root = root;
        this.parent = parent;
        this.leafNodeReference = leafNodeReference;
    }
    public String getRoot() {
        return root;
    }
    public void setRoot(String root) {
        this.root = root;
    }
    public String getParent() {
        return parent;
    }
    public void setParent(String parent) {
        this.parent = parent;
    }
    public LeafNodeReference getLeafNodeReference() {
        return leafNodeReference;
    }
    public void setLeafNodeReference(LeafNodeReference leafNodeReference) {
        this.leafNodeReference = leafNodeReference;
    }

}
