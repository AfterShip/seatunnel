package org.apache.seatunnel.connectors.seatunnel.bigtable.sink;

import com.google.auto.service.AutoService;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

import static org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableConfig.*;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/8/17 14:51
 */
@AutoService(Factory.class)
public class BigtableTableSinkFactory implements TableSinkFactory {

    public static final String IDENTIFIER = "bigtable";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        PROJECT_ID,
                        INSTANCE_ID,
                        TABLE_ID,
                        KEY_ALIAS,
                        COLUMN_MAPPINGS,
                        CONFIG_CENTER_TOKEN,
                        CONFIG_CENTER_URL,
                        CONFIG_CENTER_ENVIRONMENT,
                        CONFIG_CENTER_PROJECT)
                .optional(
                        ENCODING,
                        BIGTABLE_OPTIONS,
                        SINK_BUFFER_FLUSH_MAX_SIZE_IN_BYTES,
                        SINK_BUFFER_FLUSH_MAX_MUTATIONS)
                .build();
    }
}
