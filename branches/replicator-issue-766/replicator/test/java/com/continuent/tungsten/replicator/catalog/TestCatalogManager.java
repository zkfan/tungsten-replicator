/**
 * Tungsten: An Application Server for uni/cluster.
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
 * Contributor(s): 
 */

package com.continuent.tungsten.replicator.catalog;

import junit.framework.Assert;

import org.junit.BeforeClass;
import org.junit.Test;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Runs tests on the catalog manager to ensure we can add, find, and remove
 * catalogs.
 */
public class TestCatalogManager
{
    /**
     * Make sure we have expected test properties.
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
    }

    /**
     * Verify that if you add a catalog it is possible to fetch the catalog back
     * and get the same properties as originally added and also to remove the
     * catalog.
     */
    @Test
    public void testAddRemoveCatalog() throws Exception
    {
        // Create the catalog definition.
        TungstenProperties props = new TungstenProperties();
        props.setString("catalogs.test", SampleCatalog.class.getName());
        props.setString("catalogs.test.serviceName", "mytest");
        props.setLong("catalogs.test.channels", 3);
        props.setString("catalogs.test.myParameter", "some value");

        // Ensure that catalog does not already exist.
        CatalogManager cm = new CatalogManager();
        cm.remove("test");
        Assert.assertNull("Ensuring catalog does not exist prior to test",
                cm.find("test"));

        // Add new catalog, then fetch it back and confirm field values.
        cm.add("test", props, "catalogs");
        SampleCatalog c = (SampleCatalog) cm.find("test");
        Assert.assertNotNull("Catalog should be available", c);
        Assert.assertEquals("Comparing channels", 3, c.getChannels());
        Assert.assertEquals("Comparing service name", "mytest",
                c.getServiceName());

        // Remove the catalog and confirm that it succeeds.
        Assert.assertEquals("Testing catalog removal", true, cm.remove("test"));

        // Confirm that attempts to remove or get the catalog now fail.
        Assert.assertNull("Ensuring catalog does not exist after removal",
                cm.find("test"));
        Assert.assertFalse("Ensuring catalog cannot be removed twice",
                cm.remove("test"));

        // Clean up catalog.
        cm.removeAll();
    }

    /**
     * Verify that we can add two catalogs without errors and then remove them
     * one by one.
     */
    @Test
    public void testAddTwoCatalogs() throws Exception
    {
        // Create the catalog definitions using a single properties file.
        TungstenProperties props = new TungstenProperties();
        props.setString("catalogs.test1", SampleCatalog.class.getName());
        props.setString("catalogs.test1.serviceName", "mytest1");
        props.setString("catalogs.test2", SampleCatalog.class.getName());
        props.setString("catalogs.test2.serviceName", "mytest2");

        // Ensure that catalogs do not already exist.
        CatalogManager cm = new CatalogManager();
        Assert.assertNull("Ensuring catalog does not exist prior to test",
                cm.find("test1"));
        Assert.assertNull("Ensuring catalog does not exist prior to test",
                cm.find("test2"));

        // Add catalogs and confirm that both names are present and that the
        // count of names is 2.
        cm.add("test1", props, "catalogs");
        cm.add("test2", props, "catalogs");
        Assert.assertEquals("Checking number of names", 2, cm.names().size());

        SampleCatalog c1 = (SampleCatalog) cm.find("test1");
        Assert.assertNotNull("Catalog should be available", c1);
        Assert.assertEquals("Comparing service name", "mytest1",
                c1.getServiceName());

        SampleCatalog c2 = (SampleCatalog) cm.find("test2");
        Assert.assertNotNull("Catalog should be available", c2);
        Assert.assertEquals("Comparing service name", "mytest2",
                c2.getServiceName());

        // Remove one catalog and confirm that it succeeds.
        Assert.assertEquals("Testing catalog removal", true, cm.remove("test1"));
        Assert.assertEquals("Checking number of names", 1, cm.names().size());
        Assert.assertNull("Catalog not should be available", cm.find("test1"));
        Assert.assertNotNull("Catalog should be available", cm.find("test2"));

        // Confirm that removeAll removes the remaining catalog.
        cm.removeAll();
        Assert.assertEquals("Checking number of names", 0, cm.names().size());
        Assert.assertNull("Catalog should not be available", cm.find("test2"));
    }
}