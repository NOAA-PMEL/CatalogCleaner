package gov.noaa.pmel.tmap.catalogcleaner.crawler;

import gov.noaa.pmel.tmap.catalogcleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogXML;

public class TreeCrawlResult {
    private Catalog catalog;
    private CatalogXML catalogXML;
    public TreeCrawlResult(Catalog catalog, CatalogXML catalogXML) {
        this.catalog = catalog;
        this.catalogXML = catalogXML;
    }
    public Catalog getCatalog() {
        return catalog;
    }
    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }
    public CatalogXML getCatalogXML() {
        return catalogXML;
    }
    public void setCatalogXML(CatalogXML catalogXML) {
        this.catalogXML = catalogXML;
    }

}
