package me.ivanyu.icebreaker.main_window.tree;

import me.ivanyu.icebreaker.CatalogService;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import javax.swing.*;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.event.TreeWillExpandListener;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.ExpandVetoException;
import javax.swing.tree.TreePath;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

class NamespaceTree extends JTree {
    private final CatalogService catalog;
    private final DefaultTreeModel model;

    NamespaceTree(final CatalogService catalog,
                  final TableSelectedCallback tableSelectedCallback) {
        super(new RootNode());
        this.catalog = Objects.requireNonNull(catalog);
        this.model = (DefaultTreeModel) getModel();

        this.setCellRenderer(new TreeCellRenderer());
        this.addTreeWillExpandListener(new LazyLoadTreeWilLExpandListener());

        this.addTreeSelectionListener(new TreeSelectionListener() {
            @Override
            public void valueChanged(final TreeSelectionEvent e) {
                if (e.getPath() != null) {
                    if (e.getPath().getLastPathComponent() instanceof TableNode) {
                        final TableIdentifier tableId = ((TableNode) e.getPath().getLastPathComponent()).tableId;
                        tableSelectedCallback.call(tableId);
                    }
                }
            }
        });

        final RootNode root = (RootNode) this.model.getRoot();
        root.loadingPlaceholder = new LoadingPlaceholder();
        model.insertNodeInto(root.loadingPlaceholder, root, 0);
        expandPath(new TreePath(root.getPath()));
    }

    private class LazyLoadTreeWilLExpandListener implements TreeWillExpandListener {
        @Override
        public void treeWillExpand(final TreeExpansionEvent event) throws ExpandVetoException {
            if (event.getPath().getLastPathComponent() instanceof ExpandableNode) {
                loadExpandableNode((ExpandableNode) event.getPath().getLastPathComponent());
            }
        }

        @Override
        public void treeWillCollapse(final TreeExpansionEvent event) throws ExpandVetoException {
        }
    }

    private void loadExpandableNode(final ExpandableNode node) {
        if (node.loadingState() != LoadingState.NOT_LOADED) {
            return;
        }

        node.setLoadingStateLoading();
        Thread.ofVirtual().start(() -> {
            // TODO handle not found
            // TODO properly report errors
            final CatalogService.FetchResult fetchResult = catalog.listChild(node.namespace);
            SwingUtilities.invokeLater(() -> {
                for (final Namespace namespace : fetchResult.namespaces()) {
                    final NamespaceNode namespaceNode = new NamespaceNode(namespace);
                    model.insertNodeInto(namespaceNode, node, model.getChildCount(node));

                    namespaceNode.loadingPlaceholder = new LoadingPlaceholder();
                    model.insertNodeInto(namespaceNode.loadingPlaceholder, namespaceNode, model.getChildCount(namespaceNode));
                }

                final List<CatalogService.TableOrView> sortedTablesAndViews = fetchResult.tablesAndViews().stream()
                    .sorted(Comparator.comparing(tv -> tv.identifier().name()))
                    .toList();
                for (final CatalogService.TableOrView tableOrView : sortedTablesAndViews) {
                    final LeafNode tableOrViewNode = tableOrView.isTable()
                        ? new TableNode(tableOrView.identifier())
                        : new ViewNode(tableOrView.identifier());
                    model.insertNodeInto(tableOrViewNode, node, model.getChildCount(node));
                }
                model.removeNodeFromParent(node.loadingPlaceholder);
                node.setLoadingStateLoaded();
            });
        });
    }
}
