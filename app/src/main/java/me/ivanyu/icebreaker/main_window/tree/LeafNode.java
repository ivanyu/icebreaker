package me.ivanyu.icebreaker.main_window.tree;

import org.apache.iceberg.catalog.TableIdentifier;

import javax.swing.tree.DefaultMutableTreeNode;

class LeafNode extends DefaultMutableTreeNode {
    final TableIdentifier tableId;

    LeafNode(final TableIdentifier tableId) {
        super(tableId.name());
        this.tableId = tableId;
    }
}
