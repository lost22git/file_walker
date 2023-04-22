///usr/bin/env jbang "$0" "$@" ; exit $?

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

import javax.naming.directory.BasicAttributes;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

//NATIVE_OPTIONS --no-fallback -H:+ReportExceptionStackTraces --enable-preview --enable-monitoring
//COMPILE_OPTIONS --enable-preview --release 19
//RUNTIME_OPTIONS --enable-preview -XX:+UseZGC -XX:NativeMemoryTracking=summary -XX:+HeapDumpOnOutOfMemoryError -XX:StartFlightRecording=filename=jfr/,path-to-gc-roots=true,jdk.ObjectCount#enabled=true

//DEPS info.picocli:picocli:4.7.2
//DEPS info.picocli:picocli-codegen:4.7.2
@Command(name = "file_walker", mixinStandardHelpOptions = true, version = "file_walker 0.1", description = "file walker")
public class file_walker implements Callable<Integer> {

    @Option(names = { "--path", "-p" }, required = true, description = "path")
    Path path;

    @Option(names = { "--thread",
            "-t" }, defaultValue = "16", fallbackValue = "16", description = "thread count, default: ${DEFAULT-VALUE}")
    int thread;

    @Option(names = { "--queue-length",
            "-q" }, defaultValue = "200", fallbackValue = "200", description = "queue length for the method `fix_queue_pool`. default:${DEFAULT-VALUE}")
    int queue_length;

    static enum Method {
        single_thread, single_thread_v2, virtual_thread, fix_thread_pool, fix_queue_pool, work_stealing_pool
    }

    @Option(names = { "--method",
            "-m" }, required = true, description = """
                    method: [
                        single_thread
                        single_thread_v2
                        virtual_thread
                        fix_thread_pool
                        fix_queue_pool
                        work_stealing_pool
                    ]
                    """)
    Method method;

    final Consumer<Path> consumer = path -> {
        // System.out.println(path);
    };

    public static void main(String... args) {
        var cmd = new CommandLine(new file_walker());
        var exit_code = cmd.execute(args);
        System.exit(exit_code);
    }

    @Override
    public Integer call() throws Exception {
        switch (this.method) {
            case single_thread -> single_thread(this.path);
            case single_thread_v2 -> single_thread_v2(this.path);
            case virtual_thread -> virtual_thread(this.path);
            case fix_thread_pool -> fix_thread_pool(this.path, this.thread);
            case fix_queue_pool -> fix_queue_pool(this.path, this.queue_length, this.thread);
            case work_stealing_pool -> work_stealing_pool(this.path, this.thread);
        }
        return 0;
    }

    final void single_thread(Path root) throws Exception {
        var start = System.nanoTime();

        // not be recommended: walk(...) without error flow
        var file_count = new int[] { 0 };
        try (var paths = Files.walk(root)) {
            paths.filter(p -> Files.isRegularFile(p))
                    .forEach(p -> {
                        file_count[0] += 1;
                        this.consumer.accept(p);
                    });
        }

        System.out.format(
                "---------------------------- single_thread(%d): time used: %d ms, filec count:%d\n",
                1,
                (System.nanoTime() - start) / 1_000_000,
                file_count[0]);
    }

    final void single_thread_v2(Path root) throws Exception {
        var start = System.nanoTime();

        var file_count = new int[] { 0 };
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                if (allow_file(file)) {
                    file_count[0] += 1;
                    file_walker.this.consumer.accept(file);
                    return FileVisitResult.CONTINUE;
                } else {
                    return FileVisitResult.SKIP_SUBTREE;
                }
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                return allow_dir(dir) ? FileVisitResult.CONTINUE : FileVisitResult.SKIP_SUBTREE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                exc.printStackTrace();
                return FileVisitResult.CONTINUE;
            }
        });

        System.out.format(
                "---------------------------- single_thread_v2(%d): time used: %d ms, filec count:%d\n",
                1,
                (System.nanoTime() - start) / 1_000_000,
                file_count[0]);
    }

    final void fix_thread_pool(Path root, int thread_count) throws Exception {
        var start = System.nanoTime();
        try (var executor_service = Executors.newFixedThreadPool(thread_count)) {
            var action = new Action(null, root, executor_service);
            executor_service.execute(() -> action.invoke(this.consumer));
            var file_count = (int) action.join().join();

            System.out.format(
                    "---------------------------- fix_thread_pool(%d): time used: %d ms, filec count:%d\n",
                    thread_count,
                    (System.nanoTime() - start) / 1_000_000,
                    file_count);
        }
    }

    final void fix_queue_pool(Path root, int queue_length, int thread_count) throws Exception {
        var start = System.nanoTime();
        try (var executor_service = new ThreadPoolExecutor(
                thread_count,
                thread_count,
                0, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queue_length),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.CallerRunsPolicy())) {
            var action = new Action(null, root, executor_service);
            executor_service.execute(() -> action.invoke(this.consumer));
            var file_count = (int) action.join().join();

            System.out.format(
                    "---------------------------- fix_queue_pool(%d,queue:%d): time used: %d ms, filec count:%d\n",
                    thread_count,
                    queue_length,
                    (System.nanoTime() - start) / 1_000_000,
                    file_count);
        }
    }

    final void work_stealing_pool(Path root, int thread_count) throws Exception {
        var start = System.nanoTime();
        try (var executor_service = Executors.newWorkStealingPool(thread_count)) {
            var action = new Action(null, root, executor_service);
            executor_service.execute(() -> action.invoke(this.consumer));
            var file_count = (int) action.join().join();

            System.out.format(
                    "---------------------------- work_stealing_pool(%d): time used: %d ms, filec count:%d\n",
                    thread_count,
                    (System.nanoTime() - start) / 1_000_000,
                    file_count);
        }
    }

    final void virtual_thread(Path root) throws Exception {
        var start = System.nanoTime();
        try (var executor_service = Executors.newVirtualThreadPerTaskExecutor()) {
            var action = new Action(null, root, executor_service);
            executor_service.execute(() -> action.invoke(this.consumer));
            var file_count = (int) action.join().join();

            System.out.format(
                    "---------------------------- virtual_thread: time used: %d ms, filec count:%d\n",
                    (System.nanoTime() - start) / 1_000_000,
                    file_count);
        }
    }

    static final boolean allow_dir(Path p) {
        // allow: exist dir
        // deny: not-exist/dead link + symbolic link dir + windows junction
        try {
            return Files.isDirectory(p, LinkOption.NOFOLLOW_LINKS) && p.equals(p.toRealPath());
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    static final boolean allow_file(Path p) {
        // allow: exist file + dead link/symbolic link file
        try {
            return Files.isRegularFile(p);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    static class Action {
        private volatile int undone_fork_count = 0;
        private int join_data = 0;
        private final Lock join_lock = new ReentrantLock();

        private final CompletableFuture<Integer> join_data_fut = new CompletableFuture<>();

        final Action parent;
        final Path path;
        final ExecutorService executor_service;

        Action(Action parent, Path path, ExecutorService executor_service) {
            this.parent = parent;
            this.path = path;
            this.executor_service = executor_service;
        }

        final CompletableFuture<Integer> join() {
            return this.join_data_fut;
        }

        final void invoke(Consumer<Path> consumer) {
            List<Path> files = new ArrayList<>();
            List<Path> dirs = new ArrayList<>();
            try (var paths = Files.list(this.path)) {
                paths.forEach(p -> {
                    if (allow_file(p)) {
                        files.add(p);
                    } else if (allow_dir(p)) {
                        dirs.add(p);
                    } else {
                        System.err.format("not-allow file type, file=%s\n", p);
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }

            files.forEach(consumer);
            this.join_data += files.size();

            if (dirs.size() == 0) {
                this.join_data_fut.complete(this.join_data);
                if (this.parent != null) {
                    this.parent.do_join(this.join_data);
                }
            } else {
                this.undone_fork_count = dirs.size();
                for (var p : dirs) {
                    var fork_action = new Action(this, p, this.executor_service);
                    try {
                        this.executor_service.execute(() -> fork_action.invoke(consumer));
                    } catch (Exception e) {
                        e.printStackTrace();
                        do_join(0);
                    }
                }
            }

        }

        final void do_join(int fork_data) {
            this.join_lock.lock();
            this.join_data += fork_data;
            try {
                if ((--this.undone_fork_count) == 0 && !this.join_data_fut.isDone()) {
                    this.join_data_fut.complete(this.join_data);
                    if (this.parent != null) {
                        this.parent.do_join(this.join_data);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                this.join_lock.unlock();
            }
        }
    }

}
