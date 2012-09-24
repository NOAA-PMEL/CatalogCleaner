package gov.noaa.pmel.tmap.cleaner.jdo;

import java.util.List;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

@PersistenceCapable
public class ShortAttribute {
    @Persistent
    String name;
    @Persistent
    List<Short> value;
    public ShortAttribute(String name, List<Short> value) {
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
    public List<Short> getValue() {
        return value;
    }
    public void setValue(List<Short> value) {
        this.value = value;
    }
    

}
