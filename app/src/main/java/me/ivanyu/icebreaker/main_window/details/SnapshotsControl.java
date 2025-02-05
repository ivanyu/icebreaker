package me.ivanyu.icebreaker.main_window.details;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;

import javax.swing.*;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import java.awt.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class SnapshotsControl extends JPanel {
    private final JComboBox<String> branchComboBox = new JComboBox<>();
    private final SnapshotTable snapshotTable;
    private Map<Long, Snapshot> snapshots = null;
    private Map<String, SnapshotRef> refs = null;
    private Map<Long, Collection<String>> tags = null;

    SnapshotsControl() {
        this.setLayout(new BoxLayout(this, BoxLayout.PAGE_AXIS));

        final JPanel branchComboBoxWithLabel = new JPanel();
        branchComboBoxWithLabel.setLayout(new BoxLayout(branchComboBoxWithLabel, BoxLayout.LINE_AXIS));
        branchComboBoxWithLabel.setAlignmentX(LEFT_ALIGNMENT);

        final JLabel branchComboBoxLabel = new JLabel("Branch:");
        branchComboBoxLabel.setAlignmentX(CENTER_ALIGNMENT);

        branchComboBox.setAlignmentX(CENTER_ALIGNMENT);
        branchComboBox.addActionListener(e -> {
            if (e.getActionCommand().equals("comboBoxChanged")) {
                branchSelected((String) branchComboBox.getSelectedItem());
            }
        });

        branchComboBoxWithLabel.add(branchComboBoxLabel);
        branchComboBoxWithLabel.add(Box.createRigidArea(new Dimension(10, 1)));
        branchComboBoxWithLabel.add(branchComboBox);
        branchComboBoxWithLabel.add(Box.createGlue());

        snapshotTable = new SnapshotTable();
        snapshotTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);

        final JPanel tablePanel = new JPanel(new BorderLayout());
        tablePanel.setAlignmentX(LEFT_ALIGNMENT);
        tablePanel.add(new JScrollPane(snapshotTable), BorderLayout.CENTER);

        this.add(Box.createRigidArea(new Dimension(1, 10)));
        this.add(branchComboBoxWithLabel);
        this.add(Box.createRigidArea(new Dimension(1, 10)));
        this.add(tablePanel);
        this.add(Box.createGlue());
    }

    void beginLoading() {
        this.setEnabled(false);
    }

    void endLoading(Map<Long, Snapshot> snapshots, final Map<String, SnapshotRef> refs) {
        this.snapshots = Objects.requireNonNull(snapshots);
        this.refs = Objects.requireNonNull(refs);
        final var comboBoxModel = new DefaultComboBoxModel<String>();
        this.tags = new HashMap<>();
        // TODO sort main first
        for (final var entry : refs.entrySet()) {
            final SnapshotRef snapshotRef = entry.getValue();
            final String refName = entry.getKey();
            if (snapshotRef.isBranch()) {
                comboBoxModel.addElement(refName);
            } else if (snapshotRef.isTag()) {
                tags.computeIfAbsent(snapshotRef.snapshotId(), ignore -> new ArrayList<>())
                    .add(refName);
            }
        }
        branchComboBox.setModel(comboBoxModel);
        branchComboBox.setMaximumSize(branchComboBox.getPreferredSize());
        branchSelected((String) branchComboBox.getSelectedItem());

        this.setEnabled(true);
    }

    private void branchSelected(final String branch) {
        if (branch == null) {
            snapshotTable.setVisible(false);
        } else {
            snapshotTable.setVisible(true);
            Long snapshotId = refs.get(branch).snapshotId();
            final java.util.List<Snapshot> snapshotList = new ArrayList<>();
            while (snapshotId != null) {
                final Snapshot snapshot = snapshots.get(snapshotId);
                snapshotList.add(snapshot);
                snapshotId = snapshot.parentId();
            }
            snapshotTable.setData(snapshotList, this.tags);
        }
    }
}
