package gov.noaa.pmel.tmap.catalogcleaner.crawler;

import gov.noaa.pmel.tmap.catalogcleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafNodeReference;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;

public class DataCrawl implements Callable<String> {
    Catalog catalog;
    
    public DataCrawl(Catalog catalog) {
        super();
        this.catalog = catalog;
    }
    public Catalog getCatalog() {
        return catalog;
    }
    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }
    @Override
    public String call() throws Exception {
        List<LeafNodeReference> leaves = catalog.getLeafNodes();
        for ( Iterator iterator = leaves.iterator(); iterator.hasNext(); ) {
            LeafNodeReference leafNodeReference = (LeafNodeReference) iterator.next();
            System.out.println("Crawling "+leafNodeReference.getUrl()+" in thread "+Thread.currentThread().getId());
        }
        return catalog.getUrl();
    }

}
