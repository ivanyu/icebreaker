package me.ivanyu.icebreaker.main_window.details;

import org.apache.iceberg.Snapshot;

import javax.swing.table.DefaultTableModel;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;

class SnapshotTableModel extends DefaultTableModel {
    static final int TAGS_COLUMN_NUMBER = 0;
    static final int TIME_LINE_COLUMN_NUMBER = 1;

    public interface TimelineNode {
    }

    public record TimelineNodeEnd() implements TimelineNode {
    }

    public record TimelineNodeMiddle() implements TimelineNode {
    }

    public record TimelineNodeStart() implements TimelineNode {
    }

    private SnapshotTableModel() {
        addColumn("Tags");
        addColumn("");
        addColumn("Sequence #");
        addColumn("Timestamp");
        addColumn("Operation");
        addColumn("Summary");
        addColumn("ID");
    }

    @Override
    public Class<?> getColumnClass(final int columnIndex) {
        return switch (columnIndex) {
            case TAGS_COLUMN_NUMBER -> TagsCell.class;
            case TIME_LINE_COLUMN_NUMBER -> TimelineNode.class;
            default -> super.getColumnClass(columnIndex);
        };
    }

    static SnapshotTableModel create(final java.util.List<Snapshot> snapshots,
                                     final Map<Long, Collection<String>> tags) {
        final SnapshotTableModel model = new SnapshotTableModel();
        for (int i = 0; i < snapshots.size(); i++) {
            final TimelineNode node;
            if (i == 0) {
                node = new TimelineNodeEnd();
            } else if (i == snapshots.size() - 1) {
                node = new TimelineNodeStart();
            } else {
                node = new TimelineNodeMiddle();
            }
            final Snapshot snapshot = snapshots.get(i);

            model.addRow(new Object[]{
                createTagsCell(tags.get(snapshot.snapshotId())),
                node,
                snapshot.sequenceNumber(),
                Instant.ofEpochMilli(snapshot.timestampMillis()),
                snapshot.operation(),
                snapshot.summary(),
                snapshot.snapshotId()
            });
        }
        return model;
    }
    
    private static TagsCell createTagsCell(final Collection<String> tags) {
        return tags == null ? null : new TagsCell(tags);
    }

    @Override
    public boolean isCellEditable(final int row, final int column) {
        return false;
    }
}
