package simpledb.common;

import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.storage.TupleDesc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 *
 * @Threadsafe
 */
public class Catalog {

    // Maps table names to their corresponding DbFiles.
    private HashMap<String, DbFile> nameToDbFile;
    // Maps table ids to their corresponding DbFiles.
    private HashMap<Integer, DbFile> idToDbFile;
    // Maps table ids to their corresponding primary keys.
    private HashMap<Integer, String> idToPrimaryKey;
    // mapping ID's to nane
    private HashMap<Integer, String> idToName;

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        nameToDbFile = new HashMap<>();
        idToDbFile = new HashMap<>();
        idToPrimaryKey = new HashMap<>();
        idToName= new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     *
     * @param file      the contents of the table to add;  file.getId() is the identfier of
     *                  this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name      the name of the table -- may be an empty string.  May not be null.  If a name
     *                  conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // Store the table's name and DbFile in the nameToDbFile map.
        nameToDbFile.put(name, file);
        // Store the table's id and DbFile in the idToDbFile map.
        int id = file.getId();
        idToDbFile.put(id, file);
        // Store the table's id and primary key in the idToPrimaryKey map.
        idToPrimaryKey.put(id, pkeyField);
        // S
        idToName.put(id,name);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     *
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *             this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     *
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // Lookup the table in the nameToDbFile map and return its ID if found
        if (nameToDbFile.containsKey(name)) {
            DbFile file = nameToDbFile.get(name);
            return file.getId();
        } else {
            throw new NoSuchElementException("Table with name " + name + " not found in catalog.");
        }
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // Retrieve the corresponding DbFile object from the idToDbFile map
        DbFile file = getDatabaseFile(tableid);
        // Check if the table id exists in the idToDbFile map
        if (file == null) {
            throw new NoSuchElementException("Table with id " + tableid + " does not exist in the catalog.");
        }
        // Get the TupleDesc (schema) of the table's DbFile
        TupleDesc schema = file.getTupleDesc();
        return schema;
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        // Lookup the DbFile associated with the provided table id.
        //Do I want to use containsKey instead of get maybe??
        DbFile file = idToDbFile.get(tableid);
        // If no file exists for the provided table id, throw a NoSuchElementException.
        if (file == null) {
            throw new NoSuchElementException("Table with id " + tableid + " not found in catalog.");
        }
        return file;
    }
    //ADDED
    /**
     * Returns the name of the primary key field for the specified table.
     *
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *                function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */

    public String getPrimaryKey(int tableid) {
        // Check if the table exists
        boolean hasTable = idToDbFile.containsKey(tableid);
        if (!hasTable) {
            throw new NoSuchElementException("Table with id " + tableid + " doesn't exist");
        }
        // Look up the primary key for the table
        String primaryKey = idToPrimaryKey.get(tableid);
        if (primaryKey == null) {
            throw new NoSuchElementException("Primary key not found for table with id " + tableid + ".");
        }
        return primaryKey;
    }
    /**
     * Returns an iterator over the table ids.
     */
    public Iterator<Integer> tableIdIterator() {
        // Use keySet() to get a set of all table ids and return an iterator over that set
        Set<Integer> tableIds = idToDbFile.keySet();
        return tableIds.iterator();
    }
    /**
     * Returns the name of the table with the specified id.
     *
     * @throws NoSuchElementException if the table doesn't exist
     */

    // CHECK ON THIS AGAIN!!!

    public String getTableName(int id) throws NoSuchElementException {


            if (idToDbFile.get(id) != null) {
                return idToName.get(id);
            }
        throw new NoSuchElementException("No table with id " + id);
    }

    /**
     * Delete all tables from the catalog
     */
    public void clear() {
        // Clear the maps that hold table information
        idToDbFile.clear();
        nameToDbFile.clear();
    }

    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     *
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder = new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(catalogFile));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                List<String> names = new ArrayList<>();
                List<Type> types = new ArrayList<>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().equalsIgnoreCase("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().equalsIgnoreCase("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder + "/" + name + ".dat"), t);
                addTable(tabHf, name, primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

