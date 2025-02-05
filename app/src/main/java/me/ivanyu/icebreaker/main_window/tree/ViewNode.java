package me.ivanyu.icebreaker.main_window.tree;

import org.apache.iceberg.catalog.TableIdentifier;

class ViewNode extends LeafNode {
    ViewNode(final TableIdentifier tableId) {
        super(tableId);
    }
}
