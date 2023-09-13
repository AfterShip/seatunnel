package org.apache.seatunnel.connectors.seatunnel.gcs.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;

import com.google.auto.service.AutoService;

static @AutoService(Factory.class) public class GcsSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "GCS";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().required(PROJECT_ID, PATH, FORMAT).optional(SUFFIX).build();
    }
}
