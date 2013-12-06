/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2013 Continuent Inc.
 * Contact: tungsten@continuent.org
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of version 2 of the GNU General Public License as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA
 *
 * Initial developer(s): Robert Hodges
 * Contributor(s): Stephane Giron
 */

package com.continuent.tungsten.replicator.catalog;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.continuent.tungsten.common.config.TungstenProperties;
import com.continuent.tungsten.replicator.ReplicatorException;

/**
 * Manages catalogs. A single catalog manager can have any number of uniquely
 * named catalogs.
 */
public class CatalogManager
{
    private static Logger        logger   = Logger.getLogger(CatalogManager.class);

    // Table of currently known catalogs.
    private Map<String, Catalog> catalogs = new TreeMap<String, Catalog>();

    /**
     * Creates a new instance.
     */
    public CatalogManager()
    {
    }

    /**
     * Configures and adds a new catalog. Catalogs are defined by a properties
     * object containing attributes of the following form:
     * 
     * <pre><code>
     * prefix.name=com.continuent.tungsten.replicator.catalog.SomeCatalog
     * prefix.name.attribute1=value1
     * prefix.name.attribute2=value2
     * prefix.name.attribute4=value3
     * ...
     * </code></pre>
     * 
     * @param name Name of the catalog
     * @param catalogProperties TungstenProperties instance containing class
     *            name and attributes of the catalog instance
     * @param prefix The prefix for TungstenProperties when seeking catalog
     *            definitions. The prefix should not have a trailing period.
     */
    public void add(String name, TungstenProperties attributes, String prefix)
            throws ReplicatorException, InterruptedException
    {
        // Check for a duplicate catalog, then find the class name.
        name = name.trim();
        String fullName = prefix.trim() + "." + name;
        String className = attributes.get(fullName);
        if (catalogs.get(name) != null)
        {
            throw new ReplicatorException(
                    "Foiled attempt to load duplicate catalog: name=" + name
                            + " property=" + fullName);
        }
        if (className == null)
        {
            throw new ReplicatorException(
                    "Catalog class name not found in properties: name=" + name
                            + " property=" + fullName);
        }

        // Instantiate the catalog class and apply properties. If successful
        // add result to the catalog table.
        try
        {
            logger.info("Loading catalog: name=" + name + " className="
                    + className);
            Catalog catalog = (Catalog) Class.forName(className).newInstance();
            TungstenProperties subset = attributes.subset(fullName + ".", true);
            subset.applyProperties(catalog);
            catalog.configure();
            catalog.prepare();
            catalogs.put(name, catalog);
        }
        catch (ReplicatorException e)
        {
            // Catalog operations will throw this, so we don't need to wrap it.
            throw e;
        }
        catch (Exception e)
        {
            // Any other exception is bad and must be wrapped.
            throw new ReplicatorException(
                    "Unable to instantiate and configure catalog: name=" + name
                            + " className=" + className + " message="
                            + e.getMessage(), e);
        }
    }

    /**
     * Configures and adds a new catalog. 
     * 
     * @param name Name of the catalog
     * @param className Name of the implementing class. 
     * @param attributes TungstenProperties instance containing values to assign to 
     * catalog instance
     */
    public void add(String name, String className, TungstenProperties attributes)
            throws ReplicatorException, InterruptedException
    {
        // Check for a duplicate catalog, then find the class name.
        if (catalogs.get(name) != null)
        {
            throw new ReplicatorException(
                    "Foiled attempt to load duplicate catalog: name=" + name);
        }

        // Instantiate the catalog class and apply properties. If successful
        // add result to the catalog table.
        try
        {
            logger.info("Loading catalog: name=" + name + " className="
                    + className);
            Catalog catalog = (Catalog) Class.forName(className).newInstance();
            attributes.applyProperties(catalog);
            catalog.configure();
            catalog.prepare();
            catalogs.put(name, catalog);
        }
        catch (ReplicatorException e)
        {
            // Catalog operations will throw this, so we don't need to wrap it.
            throw e;
        }
        catch (Exception e)
        {
            // Any other exception is bad and must be wrapped.
            throw new ReplicatorException(
                    "Unable to instantiate and configure catalog: name=" + name
                            + " className=" + className + " message="
                            + e.getMessage(), e);
        }
    }

    /**
     * Returns the names of currently stored catalogs.
     */
    public List<String> names()
    {
        List<String> names = new ArrayList<String>(catalogs.keySet());
        return names;
    }

    /**
     * Returns the named catalog or null if it does not exist.
     */
    public Catalog find(String name)
    {
        return catalogs.get(name);
    }

    /**
     * Removes and deallocates a catalog.
     * 
     * @param name Name of the catalog to remove
     * @return Return true if the catalog is found and removed
     */
    public boolean remove(String name) throws InterruptedException,
            ReplicatorException
    {
        Catalog catalog = catalogs.remove(name);
        if (catalog == null)
            return false;
        else
        {
            catalog.release();
            return true;
        }
    }

    /**
     * Removes and deallocates all catalogs. This should be called to ensure
     * catalog resources are properly freed.
     */
    public void removeAll() throws InterruptedException, ReplicatorException
    {
        for (String name : names())
        {
            remove(name);
        }
    }
}