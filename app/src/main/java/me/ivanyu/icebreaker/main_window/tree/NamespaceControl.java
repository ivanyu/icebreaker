package me.ivanyu.icebreaker.main_window.tree;

import me.ivanyu.icebreaker.CatalogService;

import javax.swing.*;
import java.awt.*;

public class NamespaceControl extends JPanel {
    private final CatalogService catalog;
    private final TableSelectedCallback tableSelectedCallback;
    private final NamespaceTree tree;

    public NamespaceControl(final CatalogService catalog,
                            final TableSelectedCallback tableSelectedCallback) {
        super(new BorderLayout());
        this.catalog = catalog;
        this.tableSelectedCallback = tableSelectedCallback;

        this.tree = new NamespaceTree(catalog, tableSelectedCallback);
        final JScrollPane scrollPane = new JScrollPane(tree);
        this.add(scrollPane, BorderLayout.CENTER);
    }
}
