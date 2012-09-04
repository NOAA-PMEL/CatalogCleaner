package gov.noaa.pmel.tmap.catalogcleaner.jdo;

import javax.jdo.annotations.Column;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

@PersistenceCapable
public class NetCDFVariable {
    
    @Persistent
    @Column(length=500)
    private String name;
    
    @Persistent
    @Column(length=500)
    private String description;
    
    @Persistent
    private String info;
    
    @Persistent
    int rank;
    
    @Persistent
    private String unitsString;
    
    @Persistent
    private String hasMissingData;
    
    @Persistent
    private String dataType;
    
    @Persistent
    private String standardName;
    
    @Persistent
    private String longName;
    
    @Persistent
    private Projection projection;
    
    @Persistent
    private double lonMin;
    
    @Persistent
    private double lonMax;
    
    @Persistent
    private double latMin;
    
    @Persistent
    private double latMax;
    
    @Persistent
    private TimeAxis timeAxis;
    
    @Persistent
    private GeoAxis xAxis;
    
    @Persistent
    private GeoAxis yAxis;
    
    @Persistent
    private VerticalAxis verticalAxis;

    public double getLonMin() {
        return lonMin;
    }

    public void setLonMin(double lonMin) {
        this.lonMin = lonMin;
    }

    public double getLonMax() {
        return lonMax;
    }

    public void setLonMax(double lonMax) {
        this.lonMax = lonMax;
    }

    public double getLatMin() {
        return latMin;
    }

    public void setLatMin(double latMin) {
        this.latMin = latMin;
    }

    public double getLatMax() {
        return latMax;
    }

    public void setLatMax(double latMax) {
        this.latMax = latMax;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public String getUnitsString() {
        return unitsString;
    }

    public void setUnitsString(String unitsString) {
        this.unitsString = unitsString;
    }

    public boolean isHasMissingData() {
        return Boolean.valueOf(hasMissingData);
    }

    public void setHasMissingData(boolean hasMissingData) {
        this.hasMissingData = String.valueOf(hasMissingData);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    
    public String getStandardName() {
        return standardName;
    }

    public void setStandardName(String standardName) {
        this.standardName = standardName;
    }

    public String getLongName() {
        return longName;
    }

    public void setLongName(String longName) {
        this.longName = longName;
    }

    public TimeAxis getTimeAxis() {
        return timeAxis;
    }

    public void setTimeAxis(TimeAxis timeAxis) {
        this.timeAxis = timeAxis;
    }

    public GeoAxis getxAxis() {
        return xAxis;
    }

    public void setxAxis(GeoAxis xAxis) {
        this.xAxis = xAxis;
    }

    public GeoAxis getyAxis() {
        return yAxis;
    }

    public void setyAxis(GeoAxis yAxis) {
        this.yAxis = yAxis;
    }

    public VerticalAxis getVerticalAxis() {
        return verticalAxis;
    }

    public void setVerticalAxis(VerticalAxis zAxis) {
        this.verticalAxis = zAxis;
    }


    public enum Projection{
        AlbersEqualArea,
        FlatEarth,
        LambertAzimuthalEqualArea,
        LambertConformal,
        LatLonProjection,
        Mercator,
        Orthographic,
        ProjectionAdapter,
        RotatedLatLon,
        RotatedPole,
        Stereographic,
        TransverseMercator,
        UtmCoordinateConversion,
        UtmProjection,
        VerticalPerspectiveView
    }
    public Projection getProjection(){
        return this.projection;
    }
    public void setProjection(Projection projection){
        this.projection = projection;
    }
    public void setProjection(String proj){
        if(proj.equals("AlbersEqualArea"))
            this.projection = Projection.AlbersEqualArea;
        else if(proj.equals("FlatEarth"))
            this.projection = Projection.FlatEarth;
        else if(proj.equals("LambertAzimuthalEqualArea"))
            this.projection = Projection.LambertAzimuthalEqualArea;
        else if(proj.equals("LambertConformal"))
            this.projection = Projection.LambertConformal;
        else if(proj.equals("LatLonProjection"))
            this.projection = Projection.LatLonProjection;
        else if(proj.equals("Mercator"))
            this.projection = Projection.Mercator;
        else if(proj.equals("Orthographic"))
            this.projection = Projection.Orthographic;
        else if(proj.equals("ProjectionAdapter"))
            this.projection = Projection.ProjectionAdapter;
        else if(proj.equals("RotatedLatLon"))
            this.projection = Projection.RotatedLatLon;
        else if(proj.equals("RotatedPole"))
            this.projection = Projection.RotatedPole;
        else if(proj.equals("Stereographic"))
            this.projection = Projection.Stereographic;
        else if(proj.equals("TransverseMercator"))
            this.projection = Projection.TransverseMercator;
        else if(proj.equals("UtmCoordinateConversion"))
            this.projection = Projection.UtmCoordinateConversion;
        else if(proj.equals("UtmProjection"))
            this.projection = Projection.UtmProjection;
        else if(proj.equals("VerticalPerspectiveView"))
            this.projection = Projection.VerticalPerspectiveView;
    }
}
