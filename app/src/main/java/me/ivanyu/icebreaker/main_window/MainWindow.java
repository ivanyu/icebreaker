package me.ivanyu.icebreaker.main_window;

import me.ivanyu.icebreaker.CatalogService;
import me.ivanyu.icebreaker.main_window.details.DetailsControl;
import me.ivanyu.icebreaker.main_window.tree.NamespaceControl;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.util.Objects;

public class MainWindow extends JFrame {
    private final CatalogService catalogService;

    public MainWindow(final CatalogService catalogService) {
        this.catalogService = Objects.requireNonNull(catalogService);
        this.setTitle("Icebreaker");

        final DetailsControl detailsControl = new DetailsControl(catalogService);
        final NamespaceControl namespaceControl = new NamespaceControl(catalogService, detailsControl::selectTable);
        final JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, namespaceControl, detailsControl);
        this.add(splitPane);

        namespaceControl.addComponentListener(new ComponentAdapter() {
            @Override
            public void componentResized(final ComponentEvent e) {
                splitPane.setDividerLocation(400);
                namespaceControl.removeComponentListener(this);
            }
        });
    }

    public void run() {
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
//        this.setExtendedState(JFrame.MAXIMIZED_BOTH);
        this.pack();
        final GraphicsDevice device = this.getGraphicsConfiguration().getDevice();
        this.setSize(new Dimension(
            (int) (device.getDisplayMode().getWidth() * 0.85),
            (int) (device.getDisplayMode().getHeight() * 0.85)
        ));
        this.setVisible(true);
        this.setLocationRelativeTo(null);
    }
}
