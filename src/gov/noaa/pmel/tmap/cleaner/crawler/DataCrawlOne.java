package gov.noaa.pmel.tmap.cleaner.crawler;

import gov.noaa.pmel.tmap.addxml.ADDXMLProcessor;
import gov.noaa.pmel.tmap.addxml.AxisBean;
import gov.noaa.pmel.tmap.cleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.cleaner.jdo.CatalogComment;
import gov.noaa.pmel.tmap.cleaner.jdo.DoubleAttribute;
import gov.noaa.pmel.tmap.cleaner.jdo.FloatAttribute;
import gov.noaa.pmel.tmap.cleaner.jdo.GeoAxis;
import gov.noaa.pmel.tmap.cleaner.jdo.IntAttribute;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafDataset;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.cleaner.jdo.LongAttribute;
import gov.noaa.pmel.tmap.cleaner.jdo.NetCDFVariable;
import gov.noaa.pmel.tmap.cleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.cleaner.jdo.ShortAttribute;
import gov.noaa.pmel.tmap.cleaner.jdo.StringAttribute;
import gov.noaa.pmel.tmap.cleaner.jdo.TimeAxis;
import gov.noaa.pmel.tmap.cleaner.jdo.VerticalAxis;
import gov.noaa.pmel.tmap.cleaner.jdo.LeafNodeReference.DataCrawlStatus;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import javax.jdo.JDOHelper;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.datanucleus.api.jdo.JDOPersistenceManagerFactory;
import org.joda.time.DateTime;

import ucar.ma2.DataType;
import ucar.nc2.Attribute;
import ucar.nc2.constants.FeatureType;
import ucar.nc2.dataset.CoordinateAxis;
import ucar.nc2.dataset.CoordinateAxis1D;
import ucar.nc2.dataset.CoordinateAxis1DTime;
import ucar.nc2.dt.GridDatatype;
import ucar.nc2.dt.grid.GridCoordSys;
import ucar.nc2.dt.grid.GridDataset;
import ucar.nc2.ft.FeatureDatasetFactoryManager;
import ucar.nc2.units.DateRange;
import ucar.nc2.util.CancelTask;
import ucar.unidata.geoloc.LatLonRect;
import ucar.unidata.geoloc.Projection;
import ucar.unidata.geoloc.projection.AlbersEqualArea;
import ucar.unidata.geoloc.projection.FlatEarth;
import ucar.unidata.geoloc.projection.LambertAzimuthalEqualArea;
import ucar.unidata.geoloc.projection.LambertConformal;
import ucar.unidata.geoloc.projection.LatLonProjection;
import ucar.unidata.geoloc.projection.Mercator;
import ucar.unidata.geoloc.projection.Orthographic;
import ucar.unidata.geoloc.projection.ProjectionAdapter;
import ucar.unidata.geoloc.projection.RotatedLatLon;
import ucar.unidata.geoloc.projection.RotatedPole;
import ucar.unidata.geoloc.projection.Stereographic;
import ucar.unidata.geoloc.projection.TransverseMercator;
import ucar.unidata.geoloc.projection.UtmCoordinateConversion;
import ucar.unidata.geoloc.projection.UtmProjection;
import ucar.unidata.geoloc.projection.VerticalPerspectiveView;

public class DataCrawlOne extends DataCrawl implements Callable<String> {
    public DataCrawlOne(Properties properties, String parent, String url, String leafurl) {
        super(properties, parent, url, leafurl, true);
    }
    @Override
    public String call() throws Exception {
        Transaction tx = helper.getTransaction();
        tx.begin();
        Catalog catalog = helper.getCatalog(parent, url);
        tx.commit();
        List<LeafNodeReference> leaves = catalog.getLeafNodes();
        for ( Iterator iterator = leaves.iterator(); iterator.hasNext(); ) {
            LeafNodeReference leafNodeReference = (LeafNodeReference) iterator.next();
            if ( leafNodeReference.getUrl().equals(leafurl)) {
                System.out.println("Crawling "+leafNodeReference.getUrl()+" in thread "+Thread.currentThread().getId());
                tx.begin();
                try {
                    crawlLeafNode(catalog.getUrl(), leafNodeReference.getUrl());
                    leafNodeReference.setDataCrawlStatus(DataCrawlStatus.FINISHED);
                    leafNodeReference.setCrawlDate(DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
                } catch ( Exception e ) {
                    System.err.println("Failed to save "+catalog.getUrl()+"\n\t"+leafNodeReference.getUrl()+"\n\t for "+e.getLocalizedMessage());
                    leafNodeReference.setDataCrawlStatus(DataCrawlStatus.FAILED);
                }
                System.out.println("Commiting changes to leaf node.");
                tx.commit();
                tx.begin();
                LeafDataset leaf = helper.getLeafDataset(url, leafurl);
                List<NetCDFVariable> variables = leaf.getVariables();
                if ( variables != null ) {
                    for ( Iterator varIt = variables.iterator(); varIt.hasNext(); ) {
                        NetCDFVariable netCDFVariable = (NetCDFVariable) varIt.next();
                        System.out.println("\tNetCDFVariable: "+netCDFVariable.getDescription());
                        GeoAxis x = netCDFVariable.getxAxis();
                        System.out.println("\t\tX-Axis: "+x.getName()+" "+x.getMinValue()+" "+x.getMaxValue());
                        GeoAxis y = netCDFVariable.getyAxis();
                        System.out.println("\t\tY-Axis: "+y.getName()+" "+y.getMinValue()+" "+y.getMaxValue());
                        VerticalAxis z = netCDFVariable.getVerticalAxis();
                        if ( z != null ) System.out.println("\t\tZ-Axis: "+z.getName()+" "+z.getMinValue()+" "+z.getMaxValue());
                        TimeAxis t = netCDFVariable.getTimeAxis();
                        if ( t != null ) System.out.println("\t\tT-Axis: "+t.getTimeCoverageStart()+" "+t.getTimeCoverageEnd());
                    }
                }
                tx.commit();
            } 
        }
        return url;
    }
}