package gov.noaa.pmel.tmap.cleaner.xml;

import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.filter.Filter;

public class CatalogRefFilter implements Filter {
    private static Namespace xlink = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");

    @Override
    public boolean matches(Object obj) {
        if ( obj instanceof Element ) {
            Element element = (Element) obj;
            if ( element.getName().equals("catalogRef")) {
                return true;
            }
        }
        return false;
    }

}
