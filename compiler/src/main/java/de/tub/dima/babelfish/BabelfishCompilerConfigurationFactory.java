package de.tub.dima.babelfish;


import de.tub.dima.babelfish.ir.instructiongraph.phases.SelectionRewritePhase;
import de.tub.dima.babelfish.ir.instructiongraph.phases.StateRewritePhase;
import org.graalvm.compiler.core.phases.CommunityCompilerConfiguration;
import org.graalvm.compiler.hotspot.CompilerConfigurationFactory;
import org.graalvm.compiler.hotspot.DefaultInstrumentation;
import org.graalvm.compiler.core.Instrumentation;
import org.graalvm.compiler.loop.phases.*;
import org.graalvm.compiler.options.Option;
import org.graalvm.compiler.options.OptionKey;
import org.graalvm.compiler.options.OptionType;
import org.graalvm.compiler.options.OptionValues;
import org.graalvm.compiler.phases.PhaseSuite;
import org.graalvm.compiler.phases.tiers.CompilerConfiguration;
import org.graalvm.compiler.phases.tiers.HighTierContext;
import org.graalvm.compiler.serviceprovider.ServiceProvider;
import org.graalvm.compiler.virtual.phases.ea.PartialEscapePhase;

/**
 * Babelfish compiler factory, defines custom IR transformation phases.
 */
@ServiceProvider(CompilerConfigurationFactory.class)
public class BabelfishCompilerConfigurationFactory extends CompilerConfigurationFactory {

    public static class Options {

        // @formatter:off
        @Option(help = "Enable inlining", type = OptionType.Expert)
        public static final OptionKey<Boolean> Inline = new OptionKey<>(true);
        // @formatter:on
    }

    public static final String NAME = "luth";
    public static final int AUTO_SELECTION_PRIORITY = 3;

    public BabelfishCompilerConfigurationFactory() {
        super(NAME, AUTO_SELECTION_PRIORITY);
    }

    @Override
    public CompilerConfiguration createCompilerConfiguration() {
        return new CommunityCompilerConfiguration(){
            @Override
            public PhaseSuite<HighTierContext> createHighTier(OptionValues options) {
                PhaseSuite<HighTierContext> defaultHighTier = super.createHighTier(options);
                defaultHighTier.addBeforeLast(new StateRewritePhase());
                defaultHighTier.findPhase(PartialEscapePhase.class).add(new SelectionRewritePhase());
                return defaultHighTier;
            }
        };
    }

    @Override
    public Instrumentation createInstrumentation(OptionValues options) {
        return new DefaultInstrumentation();
    }


    @Override
    public BackendMap createBackendMap() {
        return new DefaultBackendMap("community");
    }
}
