package gov.noaa.pmel.tmap.cleaner.xml;

import org.jdom2.Element;
import org.jdom2.filter.AbstractFilter;

public class ElementAttributeFilter extends AbstractFilter {
    String element;
    String attribute;
    String value;
    public ElementAttributeFilter(String element, String attribute, String value) {
        super();
        this.element = element;
        this.attribute = attribute;
        this.value = value;
    }
    @Override
    public Object filter(Object obj) {
        if ( obj instanceof Element ) {
            Element e = (Element) obj;
            if ( e.getName().equals(element) ) {
                if ( e.getAttributeValue(attribute).equals(value) ) return e;
            }          
        }
        return null;
    }

}
