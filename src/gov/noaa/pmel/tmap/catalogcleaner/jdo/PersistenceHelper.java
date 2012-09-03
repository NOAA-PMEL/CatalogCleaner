package gov.noaa.pmel.tmap.catalogcleaner.jdo;

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
        Transaction tx = persistenceManager.currentTransaction();
        try {  
            tx.begin();
            Query query = persistenceManager.newQuery("javax.jdo.query.SQL", "SELECT * FROM catalog WHERE url='" + url + "' AND parent='"+parent+"'");
            query.setClass(Catalog.class);
            @SuppressWarnings("unchecked")
            List<Catalog> results = (List<Catalog>) query.execute();
            catalog = results.get(0);
            tx.commit();
        } catch ( Exception e ) {
            // S'ok, we'll take the null value.
        } finally {
            if ( tx.isActive() ) {
                tx.rollback();
            }
        }
        return catalog;
    }
    public CatalogXML getCatalogXML(String url) {
        CatalogXML catalogXML = null;
        try {
            catalogXML = persistenceManager.getObjectById(CatalogXML.class, url);
        } catch (Exception e) {
            // S'ok, we'll take the null value.
        }
        return catalogXML;
    }
    public void save(Object object) throws Exception {
        Transaction transaction = persistenceManager.currentTransaction();
        try {
            transaction.begin();
            persistenceManager.makePersistent(object);
            transaction.commit();
            persistenceManager.flush();
        }
        catch(Exception e){
           throw e;
        }
        finally
        {
            if (transaction.isActive())
            {
                transaction.rollback();
            }
        }
    }
    public void close() {
       persistenceManager.close();
    }
}
