package me.ivanyu.icebreaker.main_window.tree;

import org.apache.iceberg.catalog.TableIdentifier;

@FunctionalInterface
public interface TableSelectedCallback {
    void call(TableIdentifier tableId);
}
