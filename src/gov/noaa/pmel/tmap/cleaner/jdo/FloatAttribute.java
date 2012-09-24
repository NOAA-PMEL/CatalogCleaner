package gov.noaa.pmel.tmap.cleaner.jdo;

import java.util.List;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

@PersistenceCapable
public class FloatAttribute {
    @Persistent
    String name;
    @Persistent
    List<Float> value;
    public FloatAttribute(String name, List<Float> value) {
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
    public List<Float> getValue() {
        return value;
    }
    public void setValue(List<Float> value) {
        this.value = value;
    }
    

}
