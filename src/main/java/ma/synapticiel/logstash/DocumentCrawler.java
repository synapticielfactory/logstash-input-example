package ma.synapticiel.logstash;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;

/**
 * @author Yassine LASRI <yassine.lasri@synapticiel.co> Example of Java Input
 *         plugin for logstash Target : Read all document from input folder and
 *         get content as Base64
 */

/*
 * Notes about the class declaration:
 * 
 * All Java plugins must be annotated with the @LogstashPlugin annotation.
 * Additionally:
 * 
 * The name property of the annotation must be supplied and defines the name of
 * the plugin as it will be used in the Logstash pipeline definition. For
 * example, this input would be referenced in the input section of the Logstash
 * pipeline defintion as input { java_input_example => { .... } } The value of
 * the name property must match the name of the class excluding casing and
 * underscores. The class must implement the co.elastic.logstash.api.Input
 * interface. Java plugins may not be created in the org.logstash or
 * co.elastic.logstash packages to prevent potential clashes with classes in
 * Logstash itself.
 * 
 * input {
  document_crawler {
	count => 3
	prefix => "This is an example of prefix variable contant"
  }
	}
 */
@LogstashPlugin(name = "document_crawler")
public class DocumentCrawler implements Input {

	/*
	 * The PluginConfigSpec class allows developers to specify the settings that a
	 * plugin supports complete with setting name, data type, deprecation status,
	 * required status, and default value. In this example, the count setting
	 * defines the number of events that will be generated and the prefix setting
	 * defines an optional prefix to include in the event field. Neither setting is
	 * required and if it is not explicitly set, the settings default to 3 and
	 * message, respectively.
	 */
	public static final PluginConfigSpec<Long> EVENT_COUNT_CONFIG = PluginConfigSpec.numSetting("count", 3);

	public static final PluginConfigSpec<String> PREFIX_CONFIG = PluginConfigSpec.stringSetting("prefix",
			"This is an example of prefix variable contant");

	private String id;
	private long count;
	private String prefix;
	private final CountDownLatch done = new CountDownLatch(1);
	private volatile boolean stopped;

	public DocumentCrawler(String id, Configuration config, Context context) {
		this.id = id;
		count = config.get(EVENT_COUNT_CONFIG);
		prefix = config.get(PREFIX_CONFIG);
	}

	@Override
	public void start(Consumer<Map<String, Object>> consumer) {

		// The start method should push Map<String, Object> instances to the supplied
		// QueueWriter
		// instance. Those will be converted to Event instances later in the Logstash
		// event
		// processing pipeline.
		//
		// Inputs that operate on unbounded streams of data or that poll indefinitely
		// for new
		// events should loop indefinitely until they receive a stop request. Inputs
		// that produce
		// a finite sequence of events should loop until that sequence is exhausted or
		// until they
		// receive a stop request, whichever comes first.

		int eventCount = 0;
		try {
			while (!stopped && eventCount < count) {
				eventCount++;
				consumer.accept(Collections.singletonMap("message",
						prefix + " " + StringUtils.center(eventCount + " of " + count, 20)));
			}
		} finally {
			stopped = true;
			done.countDown();
		}
	}

	@Override
	public void stop() {
		stopped = true; // set flag to request cooperative stop of input
	}

	@Override
	public void awaitStop() throws InterruptedException {
		done.await(); // blocks until input has stopped
	}

	/*
	 * The configSchema method must return a list of all settings that the plugin
	 * supports. In a future phase of the Java plugin project, the Logstash
	 * execution engine will validate that all required settings are present and
	 * that no unsupported settings are present.
	 */
	@Override
	public Collection<PluginConfigSpec<?>> configSchema() {
		return Arrays.asList(EVENT_COUNT_CONFIG, PREFIX_CONFIG);
	}

	@Override
	public String getId() {
		return this.id;
	}
}