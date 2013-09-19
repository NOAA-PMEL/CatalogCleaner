package gov.noaa.pmel.tmap.cleaner.crawler;

import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;

public class CrawlableLeafNode {
    private String root;
    private LeafNodeReference leafNodeReference;
    
    
    public CrawlableLeafNode(String root, LeafNodeReference leafNodeReference) {
        super();
        this.root = root;
        this.leafNodeReference = leafNodeReference;
    }
    public String getRoot() {
        return root;
    }
    public void setRoot(String root) {
        this.root = root;
    }
    public LeafNodeReference getLeafNodeReference() {
        return leafNodeReference;
    }
    public void setLeafNodeReference(LeafNodeReference leafNodeReference) {
        this.leafNodeReference = leafNodeReference;
    }

}
