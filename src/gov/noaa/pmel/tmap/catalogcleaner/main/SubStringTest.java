package gov.noaa.pmel.tmap.catalogcleaner.main;

import java.util.List;

import gov.noaa.pmel.tmap.catalogcleaner.util.Util;

public class SubStringTest {

    /**
     * @param args
     */
    public static void main(String[] args) {
        String s1 = "http://www.esrl.noaa.gov/psd/thredds/dodsC/Datasets/ncep.reanalysis.dailyavgs/surface_gauss/catalog.html?dataset=PSDgriddedData/ncep.reanalysis.dailyavgs/surface_gauss/soilw.0-10cm.gauss.2011.nc";
        String s2 = "http://www.esrl.noaa.gov/psd/thredds/dodsC/Datasets/ncep.reanalysis.dailyavgs/surface_gauss/catalog.html?dataset=PSDgriddedData/ncep.reanalysis.dailyavgs/surface_gauss/soilw.10-200cm.gauss.2011.nc";
        
        List<String> parts = Util.uniqueParts(s1, s2);

        System.out.println("S1 reduces to: "+ parts.get(0));
        System.out.println("S2 reduces to: "+ parts.get(1));
    }
    
}
