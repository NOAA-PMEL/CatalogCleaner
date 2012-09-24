package gov.noaa.pmel.tmap.cleaner.jdo;

import java.util.List;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
@PersistenceCapable
public class IntAttribute {
    @Persistent
    String name;
    @Persistent
    List<Integer> value;
    public IntAttribute(String name, List<Integer> value) {
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
    public List<Integer> getValue() {
        return value;
    }
    public void setValue(List<Integer> value) {
        this.value = value;
    }
    
    

}
