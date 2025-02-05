package me.ivanyu.icebreaker.main_window.details;

import me.ivanyu.icebreaker.CatalogService;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import javax.swing.*;
import java.awt.*;
import java.util.Objects;

public class DetailsControl extends JPanel {
    private final CatalogService catalog;
    private final JTabbedPane tabbedPane;
    private final SchemaTable schemaTable;
    private final SnapshotsControl snapshotsControl;
    private TableIdentifier selectedTableId = null;

    public DetailsControl(final CatalogService catalog) {
        this.catalog = Objects.requireNonNull(catalog);
        this.setLayout(new BorderLayout());

        schemaTable = new SchemaTable();
        snapshotsControl = new SnapshotsControl();

        tabbedPane = new JTabbedPane();
        tabbedPane.addTab("Schema", schemaTable);
        tabbedPane.addTab("Snapshots", snapshotsControl);
        this.add(tabbedPane, BorderLayout.CENTER);
        tabbedPane.setEnabled(false);
    }

    public void selectTable(final TableIdentifier tableId) {
        this.selectedTableId = tableId;

        tabbedPane.setEnabled(false);
        schemaTable.beginLoading();
        snapshotsControl.beginLoading();
        this.setCursor(new Cursor(Cursor.WAIT_CURSOR));

        Thread.ofVirtual().start(() -> {
            // TODO handle errors
            final var describeTableResult = catalog.describeTable(tableId);

            final SchemaTableModel model = new SchemaTableModel();
            for (final Types.NestedField column : describeTableResult.schema().columns()) {
                model.addRow(new Object[]{
                    column.fieldId(),
                    column.name(),
                    column.isOptional(),
                    column.type(),
                    column.doc()
                });
            }

            SwingUtilities.invokeLater(() -> {
                schemaTable.endLoading(model);
                snapshotsControl.endLoading(describeTableResult.snapshots(), describeTableResult.refs());
                tabbedPane.setEnabled(true);
                this.setCursor(Cursor.getDefaultCursor());
            });
        });

    }
}
