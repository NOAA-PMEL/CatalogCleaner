package gov.noaa.pmel.tmap.cleaner.jdo;

import java.util.HashSet;

public class ClassList extends HashSet<String> {
    
    /**
     * 
     */
    private static final long serialVersionUID = -3312189266575464918L;

    public ClassList() {
        add("gov.noaa.pmel.tmap.cleaner.jdo.Catalog");
        add("gov.noaa.pmel.tmap.cleaner.jdo.CatalogComment");
        add("gov.noaa.pmel.tmap.cleaner.jdo.CatalogReference");
        add("gov.noaa.pmel.tmap.cleaner.jdo.CatalogXML");
        add("gov.noaa.pmel.tmap.cleaner.jdo.DoubleAttribute");
        add("gov.noaa.pmel.tmap.cleaner.jdo.FloatAttribute");
        add("gov.noaa.pmel.tmap.cleaner.jdo.GeoAxis");
        add("gov.noaa.pmel.tmap.cleaner.jdo.IntAttribute");
        add("gov.noaa.pmel.tmap.cleaner.jdo.LeafDataset");
        add("gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference");
        add("gov.noaa.pmel.tmap.cleaner.jdo.LongAttribute");
        add("gov.noaa.pmel.tmap.cleaner.jdo.NetCDFVariable");
        add("gov.noaa.pmel.tmap.cleaner.jdo.ShortAttribute");
        add("gov.noaa.pmel.tmap.cleaner.jdo.StringAttribute");
        add("gov.noaa.pmel.tmap.cleaner.jdo.TimeAxis");
        add("gov.noaa.pmel.tmap.cleaner.jdo.VerticalAxis");
    }

}
