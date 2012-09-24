package gov.noaa.pmel.tmap.cleaner.jdo;

import java.util.HashSet;

public class ClassList extends HashSet<String> {
    
    /**
     * 
     */
    private static final long serialVersionUID = -3312189266575464918L;

    public ClassList() {
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.Catalog");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogComment");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogReference");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogXML");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.DoubleAttribute");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.FloatAttribute");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.GeoAxis");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.IntAttribute");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafDataset");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafNodeReference");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.LongAttribute");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.NetCDFVariable");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.ShortAttribute");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.StringAttribute");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.TimeAxis");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.VerticalAxis");
    }

}
