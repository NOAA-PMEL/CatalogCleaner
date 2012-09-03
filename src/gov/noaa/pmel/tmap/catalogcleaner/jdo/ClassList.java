package gov.noaa.pmel.tmap.catalogcleaner.jdo;

import java.util.HashSet;

public class ClassList extends HashSet<String> {
    
    /**
     * 
     */
    private static final long serialVersionUID = -3312189266575464918L;

    public ClassList() {
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.Catalog");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogReference");
        add("gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogXML");

    }

}
