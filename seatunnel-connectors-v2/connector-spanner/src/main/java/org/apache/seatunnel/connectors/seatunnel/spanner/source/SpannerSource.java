package org.apache.seatunnel.connectors.seatunnel.spanner.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerParameters;
import org.apache.seatunnel.connectors.seatunnel.spanner.constants.SpannerConstants;
import org.apache.seatunnel.connectors.seatunnel.spanner.exception.SpannerConnectorException;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_PROJECT;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.common.config.ConfigCenterConfig.CONFIG_CENTER_URL;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.DATABASE_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.INSTANCE_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.PROJECT_ID;
import static org.apache.seatunnel.connectors.seatunnel.spanner.config.SpannerConfig.TABLE_ID;

/**
 * @author: gf.xu
 * @email: gf.xu@aftership.com
 * @date: 2023/9/6 10:56
 */
@AutoService(SeaTunnelSource.class)
public class SpannerSource
        implements SeaTunnelSource<SeaTunnelRow, SpannerSourceSplit, SpannerSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private SpannerParameters spannerParameters;

    private SeaTunnelRowType rowTypeInfo;

    @Override
    public String getPluginName() {
        return SpannerConstants.IDENTIFIER;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        // must have
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        PROJECT_ID.key(),
                        INSTANCE_ID.key(),
                        DATABASE_ID.key(),
                        TABLE_ID.key(),
                        CONFIG_CENTER_TOKEN.key(),
                        CONFIG_CENTER_URL.key(),
                        CONFIG_CENTER_ENVIRONMENT.key(),
                        CONFIG_CENTER_PROJECT.key());
        if (!result.isSuccess()) {
            throw new SpannerConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }

        this.spannerParameters = SpannerParameters.buildWithSourceConfig(pluginConfig);
        this.rowTypeInfo = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, SpannerSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return new SpannerSourceReader(rowTypeInfo, spannerParameters, readerContext);
    }

    @Override
    public SourceSplitEnumerator<SpannerSourceSplit, SpannerSourceState> createEnumerator(
            SourceSplitEnumerator.Context<SpannerSourceSplit> enumeratorContext) throws Exception {
        return new SpannerSourceSplitEnumerator(spannerParameters, enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<SpannerSourceSplit, SpannerSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<SpannerSourceSplit> enumeratorContext,
            SpannerSourceState checkpointState)
            throws Exception {
        return new SpannerSourceSplitEnumerator(
                spannerParameters, enumeratorContext, checkpointState);
    }
}
