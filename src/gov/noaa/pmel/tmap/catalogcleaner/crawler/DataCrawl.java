package gov.noaa.pmel.tmap.catalogcleaner.crawler;

import gov.noaa.pmel.tmap.addxml.ADDXMLProcessor;
import gov.noaa.pmel.tmap.addxml.AxisBean;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogComment;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.GeoAxis;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafDataset;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafNodeReference.DataCrawlStatus;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.NetCDFVariable;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.PersistenceHelper;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.TimeAxis;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.VerticalAxis;

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

import ucar.nc2.Attribute;
import ucar.nc2.constants.FeatureType;
import ucar.nc2.dataset.CoordinateAxis;
import ucar.nc2.dataset.CoordinateAxis1D;
import ucar.nc2.dataset.CoordinateAxis1DTime;
import ucar.nc2.dataset.CoordinateAxis2D;
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

public class DataCrawl implements Callable<String> {
    String parent;
    String url;
    PersistenceHelper helper;
    public DataCrawl(Properties properties, String parent, String url) {
        super();
        this.parent = parent;
        this.url = url;
        JDOPersistenceManagerFactory pmf = (JDOPersistenceManagerFactory) JDOHelper.getPersistenceManagerFactory(properties);
        PersistenceManager persistenceManager = pmf.getPersistenceManager();
        helper = new PersistenceHelper(persistenceManager);
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
            if ( leafNodeReference.getDataCrawlStatus() != DataCrawlStatus.FINISHED ) {
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
                tx.commit();
            }
        }
        return url;
    }
    private void crawlLeafNode(String parent, String url) throws Exception {
        LeafDataset leaf = helper.getLeafDataset(parent, url);
        final CatalogComment cancelMessage = new CatalogComment();
        if ( leaf == null ) {
            leaf = new LeafDataset(parent, url);
            GridDataset gridDs = null;
            try {
                CancelTask cancelTask = new CancelTask() {
                    long starttime = System.currentTimeMillis();
                    public boolean isCancel() {
                        if(System.currentTimeMillis() > starttime + 5*60*1000) // currently set to 5 minutes
                            return true;
                        return false;
                    }
                    public void setError(String err) {
                        cancelMessage.setComment("CANCEL TASK _ taking too long: " + err);
                    }
                };
                gridDs = (GridDataset) FeatureDatasetFactoryManager.open(FeatureType.GRID, url, cancelTask, null);
                if ( gridDs == null ) {
                    gridDs = (GridDataset) FeatureDatasetFactoryManager.open(FeatureType.FMRC, url, cancelTask, null);
                }
                if ( cancelMessage.getComment() != null ) {
                    leaf.setComment(cancelMessage);
                }
                List<GridDatatype> grids = gridDs.getGrids();
                String toEncode = "";
                List<NetCDFVariable> vars = new ArrayList<NetCDFVariable>();
                for(int i = 0; i<grids.size(); i++){
                    NetCDFVariable var = crawlNewVariable(grids.get(i));
                    if ( var != null ) {
                        vars.add(var);
                    }
                }
                leaf.setVariables(vars);
            } catch (Exception e) {
                CatalogComment comment = new CatalogComment();
                comment.setComment(e.getMessage());
                leaf.setComment(comment);
            }
            catch (Error e) {
                CatalogComment comment = new CatalogComment();
                comment.setComment(e.getMessage());
                leaf.setComment(comment);
            }
        }
        helper.save(leaf);
    }
    private NetCDFVariable crawlNewVariable(GridDatatype grid) {
        NetCDFVariable var = new NetCDFVariable();
        var.setDescription(grid.getDescription());
        var.setInfo(grid.getInfo());
        var.setName(grid.getName());
        var.setRank(grid.getRank());
        var.setUnitsString(grid.getUnitsString());
        var.setDataType(grid.getDataType().toString());
        var.setHasMissingData(grid.hasMissingData());
        List<Attribute> attributes = grid.getAttributes();
        
        for(int i=0; i<attributes.size(); i++){
            if(attributes.get(i).getName().toLowerCase().equals("standard_name"))
                var.setStandardName(attributes.get(i).getStringValue());
            else if(attributes.get(i).getName().toLowerCase().equals("long_name"))
                var.setLongName(attributes.get(i).getStringValue());
        }
        
        GridCoordSys gcs = (GridCoordSys) grid.getCoordinateSystem();
        Projection p = gcs.getProjection();
        if ( p instanceof LatLonProjection ) {
            var.setProjection(NetCDFVariable.Projection.LatLonProjection);
        }
        else if(p instanceof AlbersEqualArea ) {
            var.setProjection(NetCDFVariable.Projection.AlbersEqualArea);
        }
        else if(p instanceof FlatEarth ) {
            var.setProjection(NetCDFVariable.Projection.FlatEarth);
        }
        else if(p instanceof LambertAzimuthalEqualArea ) {
            var.setProjection(NetCDFVariable.Projection.LambertAzimuthalEqualArea);
        }
        else if(p instanceof LambertConformal ) {
            var.setProjection(NetCDFVariable.Projection.LambertConformal);
        }
        else if(p instanceof Mercator ) {
            var.setProjection(NetCDFVariable.Projection.Mercator);
        }
        else if(p instanceof Orthographic ) {
            var.setProjection(NetCDFVariable.Projection.Orthographic);
        }
        else if(p instanceof ProjectionAdapter ) {
            var.setProjection(NetCDFVariable.Projection.ProjectionAdapter);
        }
        else if(p instanceof RotatedLatLon ) {
            var.setProjection(NetCDFVariable.Projection.RotatedLatLon);
        }
        else if(p instanceof RotatedPole ) {
            var.setProjection(NetCDFVariable.Projection.RotatedPole);
        }
        else if(p instanceof Stereographic ) {
            var.setProjection(NetCDFVariable.Projection.Stereographic);
        }
        else if(p instanceof TransverseMercator ) {
            var.setProjection(NetCDFVariable.Projection.TransverseMercator);
        }
        else if(p instanceof UtmCoordinateConversion ) {
            var.setProjection(NetCDFVariable.Projection.UtmCoordinateConversion);
        }
        else if(p instanceof UtmProjection ) {
            var.setProjection(NetCDFVariable.Projection.UtmProjection);
        }
        else if(p instanceof VerticalPerspectiveView ) {
            var.setProjection(NetCDFVariable.Projection.VerticalPerspectiveView);
        }
        
        LatLonRect rect = gcs.getLatLonBoundingBox();
        var.setLatMin(rect.getLatMin());
        var.setLatMax(rect.getLatMax());
        var.setLonMin(rect.getLonMin());
        var.setLonMax(rect.getLonMax());
        
        if ( gcs.hasTimeAxis() ) {
            TimeAxis timeAxis = new TimeAxis();
            CoordinateAxis tAxis = gcs.getTimeAxis();
            timeAxis.setBoundaryRef(tAxis.getBoundaryRef());
            double minvalue;
            double maxvalue;
            try{
                maxvalue = tAxis.getMaxValue();
                minvalue = tAxis.getMinValue();
                timeAxis.setMinValue(minvalue);
                timeAxis.setMaxValue(maxvalue);
            } catch ( Exception e) {
                // This is not a double valued axis.  We're skipping it for now.  :-)
                return null;
            }
            timeAxis.setSize(tAxis.getSize());  
            timeAxis.setPositive(tAxis.getPositive());
            timeAxis.setIsContiguous(tAxis.isContiguous());
            timeAxis.setIsNumeric(tAxis.isNumeric());
            timeAxis.setElementSize(tAxis.getElementSize());
            timeAxis.setUnitsString(tAxis.getUnitsString());
            timeAxis.setName(tAxis.getName());
            
            timeAxis.setCalendar(getCalendarIfExists(tAxis.getAttributes()));
            
            if(timeAxis.getCalendar()!=null && timeAxis.getCalendar().toLowerCase().equals("noleap")){
                AxisBean tAxisBean = ADDXMLProcessor.makeTimeAxisStartEnd((CoordinateAxis1DTime) tAxis);
                timeAxis.setTimeCoverageStart(tAxisBean.getArange().getStart());
                timeAxis.setTimeCoverageEnd(tAxisBean.getArange().getEnd());
            } else if (timeAxis.getCalendar()==null || !timeAxis.getCalendar().toLowerCase().equals("noleap")){
                DateRange range = gcs.getDateRange();
                if(range!=null){
                    if(range.getStart() != null){
                        timeAxis.setTimeCoverageStart(range.getStart().getText());
                    }
                    if(range.getEnd() != null){
                        timeAxis.setTimeCoverageEnd(range.getEnd().getText());
                    }
                }
            }           
            var.setTimeAxis(timeAxis);
        }
        
        if ( gcs.hasVerticalAxis() ) {
            VerticalAxis zAxis = new VerticalAxis();
            CoordinateAxis1D coordAxis = gcs.getVerticalAxis();
            zAxis.setSize(coordAxis.getSize());
            zAxis.setPositive(coordAxis.getPositive());
            zAxis.setUnitsString(coordAxis.getUnitsString());
            zAxis.setIsContiguous(coordAxis.isContiguous());
            zAxis.setIsNumeric(coordAxis.isNumeric());
            zAxis.setElementSize(coordAxis.getElementSize());
            zAxis.setName(coordAxis.getName());
            zAxis.setStart(coordAxis.getStart());
            zAxis.setResolution(coordAxis.getIncrement());
            var.setVerticalAxis(zAxis);
        }
        
        CoordinateAxis yCoordAxis = gcs.getYHorizAxis();
        GeoAxis yAxis;
        try {
            yAxis = makeGeoAxis("y", yCoordAxis);
            var.setyAxis(yAxis);
        } catch ( Exception e ) {
            return null;
        }
       
        CoordinateAxis xCoordAxis = gcs.getXHorizAxis();
        GeoAxis xAxis;
        try {
            xAxis = makeGeoAxis("x", xCoordAxis);
            var.setxAxis(xAxis);
        } catch (Exception e) {
            return null;
        }
    
        return var;
        
    }
    private GeoAxis makeGeoAxis (String type, CoordinateAxis axis) throws Exception {
        
        GeoAxis geoAxis = new GeoAxis();
        geoAxis.setType(type);
        geoAxis.setBoundaryRef(axis.getBoundaryRef());
        double minvalue;
        double maxvalue;

        maxvalue = axis.getMaxValue();
        minvalue = axis.getMinValue();
        geoAxis.setMinValue(minvalue);
        geoAxis.setMaxValue(maxvalue);

        geoAxis.setSize(axis.getSize());  
        geoAxis.setUnitsString(axis.getUnitsString());
        geoAxis.setIsContiguous(axis.isContiguous());
        geoAxis.setIsNumeric(axis.isNumeric());
        geoAxis.setElementSize(axis.getElementSize());
        geoAxis.setName(axis.getName());
        
        return geoAxis;

    }
    private String getCalendarIfExists(List<Attribute> attributes){
        for(int i=0; i<attributes.size(); i++){
            if(attributes.get(i).getName() != null && attributes.get(i).getName().toLowerCase().equals("calendar")){
                return attributes.get(i).getStringValue();
            }
        }
        return null;
    }
}
