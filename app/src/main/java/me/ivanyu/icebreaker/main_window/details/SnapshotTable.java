package me.ivanyu.icebreaker.main_window.details;

import org.apache.iceberg.Snapshot;

import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableColumnModel;
import java.awt.*;
import java.util.Collection;
import java.util.Map;

import static me.ivanyu.icebreaker.main_window.details.SnapshotTableModel.TIME_LINE_COLUMN_NUMBER;

class SnapshotTable extends JTable {
    private static final int ROW_HEIGHT = 40;
    private static final int CIRCLE_SIZE = ROW_HEIGHT / 2;

    SnapshotTable() {
        setShowGrid(true);
        setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        setRowHeight(ROW_HEIGHT);
        setDefaultRenderer(SnapshotTableModel.TimelineNode.class, new CustomRenderer());
    }

    void setData(final java.util.List<Snapshot> snapshotList,
                 final Map<Long, Collection<String>> tags) {
        final SnapshotTableModel snapshotTableModel = SnapshotTableModel.create(snapshotList, tags);
        setModel(snapshotTableModel);

        final TableColumnModel columnModel = this.getColumnModel();
        for (int column = 0; column < this.getColumnCount(); column++) {
            int width = getHeaderPreferredSize(column);
            for (int row = 0; row < this.getRowCount(); row++) {
                final TableCellRenderer renderer = this.getCellRenderer(row, column);
                final Component comp = this.prepareRenderer(renderer, row, column);
                width = Math.max(comp.getPreferredSize().width + 1, width);
            }
            columnModel.getColumn(column).setPreferredWidth(width);
        }
        this.getColumn(this.getColumnName(TIME_LINE_COLUMN_NUMBER)).setPreferredWidth(40);
        this.getColumn(this.getColumnName(TIME_LINE_COLUMN_NUMBER)).setMaxWidth(40);
    }

    private int getHeaderPreferredSize(final int column) {
        final TableColumn tableColumn = this.getColumnModel().getColumn(column);
        final Object value = tableColumn.getHeaderValue();
        TableCellRenderer renderer = tableColumn.getHeaderRenderer();
        if (renderer == null) {
            renderer = this.getTableHeader().getDefaultRenderer();
        }
        final Component c = renderer.getTableCellRendererComponent(this, value, false, false, -1, column);
        return c.getPreferredSize().width;
    }

    private static class CustomRenderer extends DefaultTableCellRenderer {
        private SnapshotTableModel.TimelineNode value;

        @Override
        public Component getTableCellRendererComponent(JTable table,
                                                       Object value,
                                                       boolean isSelected,
                                                       boolean hasFocus,
                                                       int row,
                                                       int column) {
            super.getTableCellRendererComponent(table, null, isSelected, hasFocus, row, column);
            setForeground(Color.PINK);
            this.value = (SnapshotTableModel.TimelineNode) value;
            return this;
        }

        @Override
        protected void paintComponent(final Graphics g) {
            super.paintComponent(g);

            final int size = CIRCLE_SIZE;
            final int centerX = getSize().width / 2;
            final int centerY = getSize().height / 2;

            final Graphics2D g2d = (Graphics2D) g;
            g2d.setStroke(new BasicStroke(3));
            g2d.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
            g2d.fillOval(centerX - size / 2, centerY - size / 2, size, size);
            if (value instanceof SnapshotTableModel.TimelineNodeStart) {
                g2d.drawLine(centerX, 0, centerX, centerY);
            } else {
                g2d.drawLine(centerX, 0, centerX, getSize().height);
            }
        }
    }
}
