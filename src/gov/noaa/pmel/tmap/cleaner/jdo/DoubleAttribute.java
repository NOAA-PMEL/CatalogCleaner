package gov.noaa.pmel.tmap.cleaner.jdo;

import java.util.List;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

@PersistenceCapable
public class DoubleAttribute {
    
    @Persistent
    String name;
    @Persistent
    List<Double> value;
    
    public DoubleAttribute(String name, List<Double> value) {
        super();
        this.name = name;
        this.value = value;
    }
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }
    public List<Double> getValue() {
        return value;
    }
    public void setValue(List<Double> value) {
        this.value = value;
    }

}
