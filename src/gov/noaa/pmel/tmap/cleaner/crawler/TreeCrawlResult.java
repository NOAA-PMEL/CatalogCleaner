package gov.noaa.pmel.tmap.cleaner.crawler;


public class TreeCrawlResult {
    private String parent;
    private String url;
    
    public TreeCrawlResult(String parent, String url) {
        super();
        this.parent = parent;
        this.url = url;
    }
    public String getParent() {
        return parent;
    }
    public void setParent(String parent) {
        this.parent = parent;
    }
    public String getUrl() {
        return url;
    }
    public void setUrl(String url) {
        this.url = url;
    }
   
}
