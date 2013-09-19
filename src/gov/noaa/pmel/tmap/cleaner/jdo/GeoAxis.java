package gov.noaa.pmel.tmap.cleaner.jdo;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

@PersistenceCapable
public class GeoAxis {
    
    @Persistent
    private String type;
    
    @Persistent
    private String name;
    
    @Persistent
    private String boundaryRef;
    
    @Persistent
    private double minValue;
    
    @Persistent
    private double maxValue;
    
    @Persistent
    private long size;
    
    @Persistent
    private String unitsString;
    
    @Persistent
    private String isNumeric;
    
    @Persistent
    private String isContiguous;
    
    public String getIsNumeric() {
        return isNumeric;
    }
    
    @Persistent
    private int elementSize;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBoundaryRef() {
        return boundaryRef;
    }

    public void setBoundaryRef(String boundaryRef) {
        this.boundaryRef = boundaryRef;
    }

    public double getMinValue() {
        return minValue;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public double getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(double maxValue) {
        this.maxValue = maxValue;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getUnitsString() {
        return unitsString;
    }

    public void setUnitsString(String unitsString) {
        this.unitsString = unitsString;
    }

    public boolean isNumeric() {
        return Boolean.valueOf(isNumeric);
    }

    public void setIsNumeric(boolean isNumeric) {
        this.isNumeric = String.valueOf(isNumeric);
    }

    public boolean isContiguous() {
        return Boolean.valueOf(isContiguous);
    }

    public void setIsContiguous(boolean isContiguous) {
        this.isContiguous = String.valueOf(isContiguous);
    }

    public int getElementSize() {
        return elementSize;
    }

    public void setElementSize(int elementSize) {
        this.elementSize = elementSize;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
    @Override
    public String toString() {
        return this.name+this.elementSize+this.minValue+this.maxValue+this.size;
    }
    public String summary() {
        StringBuilder summary = new StringBuilder();
        summary.append("Name = "+name+"\n");
        summary.append("Element size = "+elementSize+"\n");
        summary.append("Min = "+minValue+"\n");
        summary.append("Max = "+maxValue+"\n");
        summary.append("Size = "+size+"\n");
        return summary.toString();
    }
}
