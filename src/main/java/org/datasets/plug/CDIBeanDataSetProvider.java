package org.datasets.plug;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

import org.dashbuilder.DataSetCore;
import org.dashbuilder.dataprovider.BeanDataSetProvider;
import org.dashbuilder.dataset.DataSetGenerator;
import org.dashbuilder.dataset.def.BeanDataSetDef;
import org.dashbuilder.dataset.def.DataSetDef;

@ApplicationScoped
public class CDIBeanDataSetProvider extends BeanDataSetProvider {

    @Inject
    Instance<DataSetGenerator> generators;
    
    @PostConstruct
    public void setup() {
        this.staticDataSetProvider = DataSetCore.get().getStaticDataSetProvider();
    }

    @Override
    public DataSetGenerator lookupGenerator(DataSetDef def) {
        BeanDataSetDef beanDef = (BeanDataSetDef) def;
        String generatorClass = beanDef.getGeneratorClass();
        if (generatorClass != null && !generatorClass.isBlank()) {
            return generators.stream()
                             .filter(i -> i.getClass().getSimpleName().equals(generatorClass)).findFirst()
                             .orElseThrow(() -> new IllegalArgumentException("Generator class not found: " + generatorClass));
        }
        throw new IllegalArgumentException("Definition does not contain a valid generator class.");
    }
}