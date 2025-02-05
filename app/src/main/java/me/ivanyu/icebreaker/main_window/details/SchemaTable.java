package me.ivanyu.icebreaker.main_window.details;

import javax.swing.*;

class SchemaTable extends JTable {
    SchemaTable() {
        setShowGrid(true);
        setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    }

    void beginLoading() {
        this.setEnabled(false);
    }

    void endLoading(final SchemaTableModel model) {
        this.setModel(model);
        this.setEnabled(true);
        this.revalidate();
        this.repaint();
    }
}
