package ma.synapticiel.logstash;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.Watchable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import ma.synapticiel.model.MyEvent;

@LogstashPlugin(name = "fsmonitor")
public class FsMonitor implements Input {

	private static final List<Object> DEFAULT_PATHS = Arrays.asList(".");
	public static final PluginConfigSpec<List<Object>> INPUT_FOLDER_CONFIG = PluginConfigSpec.arraySetting("paths",
			DEFAULT_PATHS, false, true);

	private String id;
	private List<Object> paths;
	private final CountDownLatch done = new CountDownLatch(1);
	private volatile boolean stopped;

	public FsMonitor(String id, Configuration config, Context context) {
		this.id = id;
		paths = config.get(INPUT_FOLDER_CONFIG);
	}

	@Override
	public void start(Consumer<Map<String, Object>> consumer) {

		try {

			while (!stopped) {
				try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
					for (int i = 0; i < paths.size(); i++) {
						Path p = Paths.get(paths.get(i).toString());
						p.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
								StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE,
								StandardWatchEventKinds.OVERFLOW);
					}

					while (true) {
						WatchKey key = watchService.take();
						final Watchable watchable = key.watchable();
						final Path directory = (Path) watchable;

						for (WatchEvent<?> watchEvent : key.pollEvents()) {
							final Kind<?> kind = watchEvent.kind();
							final Path context = (Path) watchEvent.context();

							MyEvent event = new MyEvent();
							event.setFileId(
									UUID.nameUUIDFromBytes((directory.toString() + context.toString()).getBytes())
											.toString());
							event.setFileName(context.toString());
							event.setFilePath(directory.toString());
							event.setEventType(kind.toString());

							consumer.accept(Collections.singletonMap("message", event.toString()));
						}

						boolean valid = key.reset();
						if (!valid) {
							break;
						}
					}
				} catch (IOException | InterruptedException e) {
					e.printStackTrace();
				}
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

	@Override
	public Collection<PluginConfigSpec<?>> configSchema() {
		return Arrays.asList(INPUT_FOLDER_CONFIG);
	}

	@Override
	public String getId() {
		return this.id;
	}
}