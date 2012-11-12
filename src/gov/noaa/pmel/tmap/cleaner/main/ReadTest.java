package gov.noaa.pmel.tmap.cleaner.main;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import ucar.nc2.constants.FeatureType;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import ucar.nc2.ft.FeatureDatasetFactoryManager;

public class ReadTest {

    /**
     * @param args
     */
    public static void main(String[] args) {

        try {
            String soill_url = "http://www.esrl.noaa.gov/psd/thredds/dodsC/Datasets/20thC_ReanV2/Dailies/gaussian_sprd/subsurface/soill.2003.nc";

            GridDataset soill = (GridDataset) FeatureDatasetFactoryManager.open(FeatureType.GRID, soill_url, null, null);
            if ( soill != null ) {
                List<GridDatatype> grids = soill.getGrids();
                for ( Iterator gridIt = grids.iterator(); gridIt.hasNext(); ) {
                    GridDatatype gridDatatype = (GridDatatype) gridIt.next();
                    System.out.println(gridDatatype.getDescription());
                }
            } else {
                System.out.println("Soill is null.");
            }
            
            String soilw_url = "http://www.esrl.noaa.gov/psd/thredds/dodsC/Datasets/20thC_ReanV2/Dailies/gaussian_sprd/subsurface/soilw.2003.nc";

            GridDataset soilw = (GridDataset) FeatureDatasetFactoryManager.open(FeatureType.GRID, soilw_url, null, null);
            if ( soilw != null ) {
                List<GridDatatype> grids = soilw.getGrids();
                for ( Iterator gridIt = grids.iterator(); gridIt.hasNext(); ) {
                    GridDatatype gridDatatype = (GridDatatype) gridIt.next();
                    System.out.println(gridDatatype.getDescription());
                }
            } else {
                System.out.println("Soilw is null.");
            }
        } catch ( IOException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }

}
