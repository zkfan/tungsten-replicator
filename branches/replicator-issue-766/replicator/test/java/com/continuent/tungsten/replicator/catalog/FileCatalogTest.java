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

import org.junit.Before;

import com.continuent.tungsten.common.config.TungstenProperties;

/**
 * Runs tests on the catalog manager to ensure we can add, find, and remove
 * catalogs.
 */
public class FileCatalogTest extends AbstractCatalogTest
{
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
        catalogProps.setString("directory", "fileCatalogTest");

        // Set the catalog class.
        catalogClass = FileCatalog.class.getName();
    }
}