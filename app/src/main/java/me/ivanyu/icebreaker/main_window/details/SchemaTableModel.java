package me.ivanyu.icebreaker.main_window.details;

import javax.swing.table.DefaultTableModel;

class SchemaTableModel extends DefaultTableModel {
    SchemaTableModel() {
        addColumn("ID");
        addColumn("Name");
        addColumn("Optional");
        addColumn("Type");
        addColumn("Doc");
    }

    @Override
    public boolean isCellEditable(final int row, final int column) {
        return false;
    }
}
