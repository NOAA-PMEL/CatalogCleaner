package gov.noaa.pmel.tmap.catalogcleaner.jdo;

import java.util.List;

import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;
@PersistenceCapable
public class LongAttribute {
    @Persistent
    String name;
    @Persistent
    List<Long> value;
    public LongAttribute(String name, List<Long> value) {
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
    public List<Long> getValue() {
        return value;
    }
    public void setValue(List<Long> value) {
        this.value = value;
    }
    
}
