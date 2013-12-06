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

import java.io.File;
import java.io.FileInputStream;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.BeforeClass;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Runs tests on the catalog manager to ensure we can add, find, and remove
 * catalogs.
 */
public class SqlCatalogTest extends AbstractCatalogTest
{
    private static Logger      logger         = Logger.getLogger(SqlCatalogTest.class);

    // Properties used in SQL access.
    private static String      driver;
    private static String      url;
    private static String      user;
    private static String      password;
    private static String      schema;

    /**
     * Make sure we have expected test properties.
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        // Set test.properties file name.
        String testPropertiesName = System.getProperty("test.properties");
        if (testPropertiesName == null)
        {
            testPropertiesName = "test.properties";
            logger.info("Setting test.properties file name to default: test.properties");
        }

        // Load properties file.
        TungstenProperties tp = new TungstenProperties();
        File f = new File(testPropertiesName);
        if (f.canRead())
        {
            logger.info("Reading test.properties file: " + testPropertiesName);
            FileInputStream fis = new FileInputStream(f);
            tp.load(fis);
            fis.close();
        }
        else
            logger.warn("Using default values for test");

        // Set values used for test.
        driver = tp.getString("database.driver",
                "org.apache.derby.jdbc.EmbeddedDriver", true);
        url = tp.getString("database.url", "jdbc:derby:testdb;create=true",
                true);
        user = tp.getString("database.user");
        password = tp.getString("database.password");
        schema = tp.getString("database.schema", "testdb", true);

        // Load driver.
        Class.forName(driver);
    }

    /**
     * Set up properties used to configure the catalog.
     */
    @Before
    public void setUp() throws Exception
    {
        // Create the catalog definition.
        catalogProps = new TungstenProperties();
        catalogProps.setString("serviceName", "sqlcatalog");
        catalogProps.setLong("channels", 10);
        catalogProps.setString("url", url);
        catalogProps.setString("user", user);
        catalogProps.setString("password", password);
        catalogProps.setString("schema", schema);

        // Set the catalog class.
        catalogClass = SqlCatalog.class.getName();
    }
}