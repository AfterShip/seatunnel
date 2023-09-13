package org.apache.seatunnel.connectors.seatunnel.spanner.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.*;

import java.util.List;

/**
 * todo implement SpannerCatalog
 *
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/7 10:56
 */
public class SpannerCatalog implements Catalog {


    public SpannerCatalog(
            String catalogName,
            String projectId,
            String instanceId,
            String databaseId,
            String serviceAccount) {
        // todo
    }

    @Override
    public void open() throws CatalogException {

    }

    @Override
    public void close() throws CatalogException {

    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return null;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return false;
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return null;
    }

    @Override
    public List<String> listTables(String databaseName) throws CatalogException, DatabaseNotExistException {
        return null;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        return false;
    }

    @Override
    public CatalogTable getTable(TablePath tablePath) throws CatalogException, TableNotExistException {
        return null;
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {

    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {

    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {

    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {

    }
}
