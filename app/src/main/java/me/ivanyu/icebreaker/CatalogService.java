package me.ivanyu.icebreaker;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CatalogService {
    private final RESTCatalog restCatalog;

    CatalogService(final String url) {
        Objects.requireNonNull(url);
        this.restCatalog = new RESTCatalog(conf -> HTTPClient.builder(conf).uri(url).build());
        restCatalog.initialize("rest", Map.of(
            "io-impl", InMemoryFileIO.class.getName()
        ));
    }

    public FetchResult listChild(final Namespace namespace) {
        // TODO parallelize
        final List<Namespace> namespaces = restCatalog.listNamespaces(namespace);
        final List<TableIdentifier> tables = namespace.equals(Namespace.empty())
            ? List.of()
            : restCatalog.listTables(namespace);
        final List<TableIdentifier> views = namespace.equals(Namespace.empty())
            ? List.of()
            : restCatalog.listViews(namespace);

        final List<TableOrView> tablesAndViews = new ArrayList<>();
        tablesAndViews.addAll(
            tables.stream().map(t -> new TableOrView(t, true)).toList()
        );
        tablesAndViews.addAll(
            views.stream().map(v -> new TableOrView(v, false)).toList()
        );

        return new FetchResult(namespaces, tablesAndViews);
    }

    public record TableOrView(TableIdentifier identifier,
                              boolean isTable) {
    }

    public record FetchResult(Collection<Namespace> namespaces,
                              Collection<TableOrView> tablesAndViews) {
    }

    public DescribeTableResult describeTable(final TableIdentifier tableId) {
        final Table table = restCatalog.loadTable(tableId);
        final Map<Long, Snapshot> snapshots = new HashMap<>();
        for (final Snapshot snapshot : table.snapshots()) {
            snapshots.put(snapshot.snapshotId(), snapshot);
        }
        return new DescribeTableResult(table.schema(), snapshots, table.refs());
    }

    public record DescribeTableResult(Schema schema,
                                      Map<Long, Snapshot> snapshots,
                                      Map<String, SnapshotRef> refs) {
    }
}
