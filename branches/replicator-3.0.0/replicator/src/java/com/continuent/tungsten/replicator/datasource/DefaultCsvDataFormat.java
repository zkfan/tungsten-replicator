/**
 * Tungsten Scale-Out Stack
 * Copyright (C) 2014 Continuent Inc.
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

package com.continuent.tungsten.replicator.datasource;

import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import javax.sql.rowset.serial.SerialException;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.extractor.mysql.SerialBlob;

/**
 * Denotes a file that converts Java object instances to strings suitable for
 * most CSV file consumers.
 */
public class DefaultCsvDataFormat implements CsvDataFormat
{
    // Properties.
    protected TimeZone       timezone;

    // Formatting support.
    private SimpleDateFormat dateFormatter;
    private SimpleDateFormat timestampFormatter;
    private SimpleDateFormat timeFormatter;

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CsvDataFormat#setTimeZone(java.util.TimeZone)
     */
    public void setTimeZone(TimeZone timezone)
    {
        this.timezone = timezone;
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CsvDataFormat#prepare()
     */
    public void prepare()
    {
        // Dates are formatted with the date only.
        dateFormatter = new SimpleDateFormat();
        dateFormatter.setTimeZone(timezone);
        dateFormatter.applyPattern("yyyy-MM-dd");

        // Timestamps are formatted to full precision.
        timestampFormatter = new SimpleDateFormat();
        timestampFormatter.setTimeZone(timezone);
        timestampFormatter.applyPattern("yyyy-MM-dd HH:mm:ss.SSS");

        // Time values are formatted to seconds with no date.
        timeFormatter = new SimpleDateFormat();
        timeFormatter.setTimeZone(timezone);
        timeFormatter.applyPattern("HH:mm:ss");
    }

    /**
     * {@inheritDoc}
     * 
     * @see com.continuent.tungsten.replicator.datasource.CsvDataFormat#csvString(java.lang.Object,
     *      int, boolean)
     */
    public String csvString(Object value, int javaType, boolean isBlob)
            throws ReplicatorException
    {
    	// For examples of calling conventions for this method see class 
    	// SimplBatchApplier. 
        if (value == null)
        {
            return null;
        }
        else if (value instanceof Timestamp)
        {
            if (javaType == Types.TIME)
            {
                return timeFormatter.format((Timestamp) value);
            }
            else if (javaType == Types.DATE)
            {
                return dateFormatter.format((Timestamp) value);
            }
            else
            {
                return timestampFormatter.format((Timestamp) value);
            }
        }
        else if (value instanceof java.sql.Date)
        {
            return dateFormatter.format((java.sql.Date) value);
        }
        else if (value instanceof java.sql.Time)
        {
            return timeFormatter.format((java.sql.Time) value);
        }
        else if (javaType == Types.BLOB
                || (javaType == Types.NULL && value instanceof SerialBlob))
        { // ______^______
          // Blob in the incoming event masked as NULL,
          // though this happens with a non-NULL value!
          // Case targeted with this: MySQL.TEXT -> CSV
            SerialBlob blob = (SerialBlob) value;

            if (isBlob)
            {
                // If it's really a blob, the following will not work correctly,
                // but let's not eat the value, if there's a possibility of one.
                return value.toString();
            }
            else
            {
                // Expect a textual field.
                String toString = null;

                if (blob != null)
                {
                    try
                    {
                        toString = new String(blob.getBytes(1,
                                (int) blob.length()));
                    }
                    catch (SerialException e)
                    {
                        throw new ReplicatorException(
                                "Exception while getting blob.getBytes(...)", e);
                    }
                }

                return toString;
            }
        }
        else
            return value.toString();
    }
}