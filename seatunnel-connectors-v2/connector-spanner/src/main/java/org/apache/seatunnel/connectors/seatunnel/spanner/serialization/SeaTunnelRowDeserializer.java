package org.apache.seatunnel.connectors.seatunnel.spanner.serialization;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.cloud.spanner.ResultSet;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 18:32
 */
public interface SeaTunnelRowDeserializer {

    SeaTunnelRow deserialize(ResultSet resultSet);
}
