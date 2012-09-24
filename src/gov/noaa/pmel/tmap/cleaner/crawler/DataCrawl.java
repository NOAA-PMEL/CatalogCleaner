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

public class DataCrawl implements Callable<String> {
    String parent;
    String url;
    PersistenceHelper helper;
    boolean force = false;
    public DataCrawl(Properties properties, String parent, String url, boolean force) {
        super();
        this.parent = parent;
        this.url = url;
        this.force = force;
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
            if ( leafNodeReference.getDataCrawlStatus() != DataCrawlStatus.FINISHED || force ) {
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
            } else {
                System.out.println("Already crawled "+leafNodeReference.getUrl()+" in thread "+Thread.currentThread().getId());
                LeafDataset data = helper.getLeafDataset(catalog.getUrl(), leafNodeReference.getUrl());
                if ( data == null ) {
                    tx.begin();
                    System.out.println("\t Data null, recrawling "+leafNodeReference.getUrl()+" in thread "+Thread.currentThread().getId());
                    crawlLeafNode(catalog.getUrl(), leafNodeReference.getUrl());
                    leafNodeReference.setDataCrawlStatus(DataCrawlStatus.FINISHED);
                    leafNodeReference.setCrawlDate(DateTime.now().toString("yyyy-MM-dd HH:mm:ss"));
                    tx.commit();
                }
            }
        }
        return url;
    }
    private void crawlLeafNode(String parent, String url) throws Exception {
        LeafDataset leaf = helper.getLeafDataset(parent, url);
        final CatalogComment cancelMessage = new CatalogComment();
        if ( leaf == null || force ) {
            if ( leaf == null ) {
                leaf = new LeafDataset(parent, url);
                helper.save(leaf);
            }
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
        // Else already have this saved...
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
        
        List<DoubleAttribute> doubleAttributes = new ArrayList<DoubleAttribute>();
        List<FloatAttribute> floatAttributes = new ArrayList<FloatAttribute>();
        List<ShortAttribute> shortAttributes = new ArrayList<ShortAttribute>();
        List<IntAttribute> intAttributes = new ArrayList<IntAttribute>();
        List<LongAttribute> longAttributes = new ArrayList<LongAttribute>();
        List<StringAttribute> stringAttributes = new ArrayList<StringAttribute>();

        for(int i=0; i<attributes.size(); i++){
            
            Attribute attribute = attributes.get(i);
            // Capture a couple of special attributes
            if(attribute.getName().toLowerCase().equals("standard_name"))
                var.setStandardName(attribute.getStringValue());
            else if(attribute.getName().toLowerCase().equals("long_name"))
                var.setLongName(attribute.getStringValue());
           
            // Capture all attributes...
            if ( attribute.getDataType() == DataType.DOUBLE) {
                List<Double> v = new ArrayList<Double>();
                for (int d = 0; d < attribute.getLength(); d++) {
                    v.add(new Double(attribute.getNumericValue(d).doubleValue()));
                }
                doubleAttributes.add(new DoubleAttribute(attribute.getName(), v));
            } else if ( attribute.getDataType() == DataType.FLOAT ) {
                List<Float> v = new ArrayList<Float>();
                for (int d = 0; d < attribute.getLength(); d++) {
                    v.add(new Float(attribute.getNumericValue(d).floatValue()));
                }
                floatAttributes.add(new FloatAttribute(attribute.getName(), v));
            } else if ( attribute.getDataType() == DataType.SHORT ) {
                List<Short> v = new ArrayList<Short>();
                for (int d = 0; d < attribute.getLength(); d++) {
                    v.add(new Short(attribute.getNumericValue(d).shortValue()));
                }
                shortAttributes.add(new ShortAttribute(attribute.getName(), v));
                
            } else if ( attribute.getDataType() == DataType.INT ) {
                List<Integer> v = new ArrayList<Integer>();
                for (int d = 0; d < attribute.getLength(); d++) {
                    v.add(new Integer(attribute.getNumericValue(d).intValue()));
                }
                intAttributes.add(new IntAttribute(attribute.getName(), v));
            } else if ( attribute.getDataType() == DataType.LONG ) {
                List<Long> v = new ArrayList<Long>();
                for (int d = 0; d < attribute.getLength(); d++) {
                    v.add(new Long(attribute.getNumericValue(d).longValue()));
                }
                longAttributes.add(new LongAttribute(attribute.getName(), v));
            } else if ( attribute.getDataType() == DataType.STRING ) {
                List<String> v = new ArrayList<String>();
                for ( int d = 0; d < attribute.getLength(); d++ ) {
                    v.add(new String(attribute.getStringValue(d)));
                }
                stringAttributes.add(new StringAttribute(attribute.getName(), v));
            }
        }
        
        
        var.setDoubleAttributes(doubleAttributes);
        var.setFloatAttributes(floatAttributes);
        var.setShortAttributes(shortAttributes);
        var.setIntAttributes(intAttributes);
        var.setLongAttributes(longAttributes);
        var.setStringAttributes(stringAttributes);
        
        
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
            zAxis.setMinValue(coordAxis.getMinValue());
            zAxis.setMaxValue(coordAxis.getMaxValue());
            if ( coordAxis.isRegular() ) {
                zAxis.setIsRegular(true);
                zAxis.setResolution(coordAxis.getIncrement());
            } else {
                double v[] = coordAxis.getCoordValues();
                String values = "";
                for ( int i = 0; i < v.length; i++ ) {
                    values = values + v[i] + " ";
                }
                zAxis.setValues(values.trim());
            }
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