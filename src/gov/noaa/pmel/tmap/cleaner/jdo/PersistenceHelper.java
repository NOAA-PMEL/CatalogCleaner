package gov.noaa.pmel.tmap.cleaner.jdo;

import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;

public class PersistenceHelper {
    PersistenceManager persistenceManager;
    public PersistenceHelper(PersistenceManager persistenceManager) {
        super();
        this.persistenceManager = persistenceManager;
    }
    public Catalog getCatalog(String parent, String url) {
        Catalog catalog = null;
        try {  
            Query query = persistenceManager.newQuery("javax.jdo.query.SQL", "SELECT * FROM catalog WHERE url='" + url + "' AND parent='"+parent+"'");
            query.setClass(Catalog.class);
            @SuppressWarnings("unchecked")
            List<Catalog> results = (List<Catalog>) query.execute();
            catalog = results.get(0);
        } catch (Exception e) {
            // S'ok, we'll take the null and carry on.
        }
        return catalog;
    }
    public LeafDataset getLeafDataset(String parent, String url) {
        LeafDataset leaf = null;
        try {
            String statement =  "SELECT * from leafdataset WHERE url='"+url+"' AND parent='"+parent+"'";
            Query query = persistenceManager.newQuery("javax.jdo.query.SQL", statement);
            query.setClass(LeafDataset.class);
            List<LeafDataset> results = (List<LeafDataset>) query.execute();
            leaf = results.get(0);
        } catch ( Exception e ) {
            // S'ok, we'll take the null and carry on.

        } 
        return leaf;
    }
    public CatalogXML getCatalogXML(String url) {
        CatalogXML catalogXML = null;
        try {
            Query query = persistenceManager.newQuery("javax.jdo.query.SQL", "SELECT * from catalogxml WHERE url='"+url+"'");
            query.setClass(CatalogXML.class);
            List<CatalogXML> results = (List<CatalogXML>) query.execute();
            catalogXML = results.get(0);
        } catch (Exception e) {
            
            System.out.println("Error getting XML for "+url+" was "+e);

        }
        return catalogXML;
    }
    public LeafNodeReference getLeafNodeReference(String parent, String leafurl) {
        LeafNodeReference leafRef = null;
        try {
            Query query = persistenceManager.newQuery("javax.jdo.query.SQL", "SELECT * from leafnodereference WHERE url='"+leafurl+"'"+"' AND parent='"+parent+"'");
            query.setClass(LeafNodeReference.class);
            List<LeafNodeReference> results = (List<LeafNodeReference>) query.execute();
            leafRef = results.get(0);
        } catch (Exception e) {
            //S'ok, the null is ok.
        }
        return leafRef;
    }
    public void save(Object object) throws Exception {
        try {
            persistenceManager.makePersistent(object);
        }
        catch(Exception e){
           throw e;
        }
    }
    public void close() {
        persistenceManager.getDataStoreConnection().close();
        persistenceManager.close();
    }
    public Transaction getTransaction() {
        return persistenceManager.currentTransaction();
    }
   
}
