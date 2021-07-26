package org.embulk.input.marketo;

import org.apache.bval.jsr303.ApacheValidationProvider;
import org.embulk.base.restclient.RestClientInputPluginBase;
import org.embulk.util.config.ConfigMapperFactory;

import javax.validation.Validation;

/**
 * Created by tai.khuu on 8/22/17.
 */
public class MarketoInputPlugin
        extends RestClientInputPluginBase<MarketoInputPluginDelegate.PluginTask>
{
    public static final ConfigMapperFactory CONFIG_MAPPER_FACTORY = ConfigMapperFactory.builder().addDefaultModules()
            .withValidator(Validation.byProvider(ApacheValidationProvider.class)
                    .configure()
                    .buildValidatorFactory()
                    .getValidator())
            .build();
    public MarketoInputPlugin()
    {
        super(CONFIG_MAPPER_FACTORY, MarketoInputPluginDelegate.PluginTask.class, new MarketoInputPluginDelegate());
    }
}
