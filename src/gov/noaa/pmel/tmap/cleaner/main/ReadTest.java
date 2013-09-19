package gov.noaa.pmel.tmap.cleaner.main;

import gov.noaa.pmel.tmap.cleaner.crawler.DataCrawl;
import gov.noaa.pmel.tmap.cleaner.jdo.GeoAxis;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import ucar.nc2.constants.FeatureType;
import ucar.nc2.dataset.CoordinateAxis;
import ucar.nc2.dataset.CoordinateAxis1D;
import ucar.nc2.dataset.CoordinateAxis2D;
import ucar.nc2.dt.GridCoordSystem;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridDataset;
import ucar.nc2.ft.FeatureDatasetFactoryManager;
import ucar.unidata.geoloc.LatLonRect;
import ucar.unidata.geoloc.ProjectionPoint;
import ucar.unidata.geoloc.ProjectionRect;

public class ReadTest {

    /**
     * @param args
     */
    public static void main(String[] args) {

        readAndPrintXY("http://www.esrl.noaa.gov/psd/thredds/dodsC/Datasets/NARR/pressure/air.198012.nc");
        readAndPrintXY("http://www.esrl.noaa.gov/psd/thredds/dodsC/Datasets/NARR/pressure/air.198101.nc");



    }
    public static void readAndPrintXY(String url) {
        try {
            GridDataset gridDataset = (GridDataset) FeatureDatasetFactoryManager.open(FeatureType.GRID, url, null, null);
            System.out.println(url);

            if ( gridDataset != null ) {
                List<GridDatatype> grids = gridDataset.getGrids();
                for ( Iterator gridIt = grids.iterator(); gridIt.hasNext(); ) {
                    GridDatatype gridDatatype = (GridDatatype) gridIt.next();
                    GeoAxis geoAxisX = new GeoAxis();
                    GeoAxis geoAxisY = new GeoAxis();
                    System.out.println(gridDatatype.getDescription());
                    GridCoordSystem gcs = gridDatatype.getCoordinateSystem();  
                    if ( gcs.isLatLon() ) {
                        CoordinateAxis xCoordAxis = gcs.getXHorizAxis();
                        if (xCoordAxis instanceof CoordinateAxis1D) {
                            CoordinateAxis1D x1d = (CoordinateAxis1D) xCoordAxis;
                            System.out.println("X is 1D "+x1d);
                        } else if ( xCoordAxis instanceof CoordinateAxis2D ) {
                            CoordinateAxis2D x2d = (CoordinateAxis2D) xCoordAxis;
                            System.out.println("X is 2D "+x2d);
                            geoAxisX = DataCrawl.makeGeoAxis("x", x2d );
                        }
                        CoordinateAxis yCoordAxis = gcs.getYHorizAxis();
                        if (yCoordAxis instanceof CoordinateAxis1D) {
                            CoordinateAxis1D y1d = (CoordinateAxis1D) yCoordAxis;
                            System.out.println("Y is 1D "+y1d);
                        } else if ( yCoordAxis instanceof CoordinateAxis2D ) {
                            CoordinateAxis2D y2d = (CoordinateAxis2D) yCoordAxis;
                            System.out.println("Y is 2D "+y2d);
                            geoAxisY = DataCrawl.makeGeoAxis("y", y2d );

                        }
                    } else {
                        LatLonRect bb = gcs.getLatLonBoundingBox();
                       
                        geoAxisY.setName("lat");
                        geoAxisY.setMaxValue(bb.getLatMax());
                        geoAxisY.setMaxValue(bb.getLatMin());
                        geoAxisX.setName("lon");
                        geoAxisX.setMinValue(bb.getLonMin());
                        geoAxisX.setMaxValue(bb.getLonMax());
                    }
                    System.out.println(geoAxisX.summary());
                    System.out.println(geoAxisY.summary());
                }                
            } else {
                System.out.println(url+"returned null.");
            }

        } catch ( IOException e ) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
