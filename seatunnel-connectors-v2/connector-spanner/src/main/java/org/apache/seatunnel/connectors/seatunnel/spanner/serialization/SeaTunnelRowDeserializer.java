package org.apache.seatunnel.connectors.seatunnel.spanner.serialization;

import com.google.cloud.spanner.ResultSet;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 18:32
 */
public interface SeaTunnelRowDeserializer {

    SeaTunnelRow deserialize(ResultSet resultSet);

}
