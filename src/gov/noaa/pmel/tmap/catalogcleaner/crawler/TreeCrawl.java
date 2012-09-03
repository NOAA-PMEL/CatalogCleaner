package gov.noaa.pmel.tmap.catalogcleaner.crawler;

import gov.noaa.pmel.tmap.catalogcleaner.jdo.Catalog;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogReference;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.CatalogXML;
import gov.noaa.pmel.tmap.catalogcleaner.jdo.LeafNodeReference;
import gov.noaa.pmel.tmap.catalogcleaner.util.Util;
import gov.noaa.pmel.tmap.cleaner.http.Proxy;
import gov.noaa.pmel.tmap.cleaner.xml.CatalogRefFilter;
import gov.noaa.pmel.tmap.cleaner.xml.JDOMUtils;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.Namespace;
import org.jdom.Parent;

import thredds.catalog.InvAccess;
import thredds.catalog.InvCatalog;
import thredds.catalog.InvCatalogFactory;
import thredds.catalog.InvDataset;
import thredds.catalog.ServiceType;

public class TreeCrawl implements Callable<TreeCrawlResult> {
    private static Proxy proxy = new Proxy();
    private static Namespace xlink = Namespace.getNamespace("xlink", "http://www.w3.org/1999/xlink");
    private String url;
    private String root;
    private Catalog catalog;
    private CatalogXML catalogXML;
    public TreeCrawl(Catalog catalog, CatalogXML catalogXML, String root, String url) {
        this.catalog = catalog;
        this.catalogXML = catalogXML;
        this.root = root;
        this.url = url;
    }
    @Override
    public TreeCrawlResult call() throws Exception {
        if ( catalog == null ) {
            catalog = new Catalog();
        }
        catalog.setParent(root);
        catalog.setUrl(url);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        proxy.executeGetMethodAndStreamResult(url, stream);
        String xml = stream.toString();
        if ( catalogXML == null ) {
        catalogXML = new CatalogXML();
        } else {
            if ( catalogXML != null ) {
                String oldxml = catalogXML.getXml();
                if ( oldxml.equals(xml) ) {
                    return new TreeCrawlResult(catalog, catalogXML);
                }
            }
        }
        catalogXML.setUrl(url);
        catalogXML.setXml(xml);
        Document doc = new Document();
        JDOMUtils.XML2JDOM(xml, doc);
        Element rootElement = doc.getRootElement();
        String version = rootElement.getAttributeValue("version");
        if(version != null){
            catalog.setVersion(version);
        }
        List<CatalogReference> refs = new ArrayList<CatalogReference>();
        refs = findCatalogRefs(url, rootElement, refs);
        catalog.setCatalogRefs(refs);
        List<LeafNodeReference> accessDatasets = new ArrayList<LeafNodeReference>();
        Iterator removeIt = doc.getDescendants(new CatalogRefFilter());
        Set<Parent> parents = new HashSet<Parent>();
        while ( removeIt.hasNext() ) {
            Element ref = (Element) removeIt.next();
            parents.add(ref.getParent());
        }
        for ( Iterator parentIt = parents.iterator(); parentIt.hasNext(); ) {
            Parent parent = (Parent) parentIt.next();
            parent.removeContent(new CatalogRefFilter());
        }
        accessDatasets = findAccessDatasets(url, doc, accessDatasets);
        catalog.setLeafNodes(accessDatasets);
        return new TreeCrawlResult(catalog, catalogXML);
    }
    private static List<CatalogReference> findCatalogRefs(String originalUrl, Element element, List<CatalogReference> catalogUrls) throws Exception {
        
        if (element.getName().equals("catalogRef")) {
            String suburl = element.getAttributeValue("href", xlink);
            suburl = Util.getUrl(originalUrl, suburl, "thredds");
            catalogUrls.add(new CatalogReference(suburl));
            URL nurl = new URL(suburl);
        }
        List kids = element.getChildren();
        for (Iterator catIt = kids.iterator(); catIt.hasNext();) {
            Element kid = (Element) catIt.next();
            catalogUrls = findCatalogRefs(originalUrl, kid, catalogUrls);
        }
        return catalogUrls;
    }
    private static List<LeafNodeReference> findAccessDatasets(String url, Document doc, List<LeafNodeReference> datasets) throws URISyntaxException {
        InvCatalogFactory catfactory = new InvCatalogFactory("default", false);
        String strippedDoc = JDOMUtils.toString(doc);
        InvCatalog thredds = (InvCatalog) catfactory.readXML(strippedDoc, new URI(url));
        List<InvDataset> rootInvDatasets = thredds.getDatasets();
        return findAccessDatasets(rootInvDatasets, datasets);
    }
    private static List<LeafNodeReference> findAccessDatasets(List<InvDataset> invDatasets, List<LeafNodeReference> datasets) {
        for ( Iterator dsIt = invDatasets.iterator(); dsIt.hasNext(); ) {
            InvDataset ds = (InvDataset) dsIt.next();
            if ( ds.hasAccess() ) {
                InvAccess access = ds.getAccess(ServiceType.OPENDAP);
                if(access!=null){
                    String locationUrl = access.getStandardUrlName();
                    datasets.add(new LeafNodeReference(locationUrl));
                }
                
            } else if ( ds.hasNestedDatasets() ) {
                List<InvDataset> children = ds.getDatasets();
                findAccessDatasets(children, datasets);
            }
        }
        return datasets;
    }
    
}
