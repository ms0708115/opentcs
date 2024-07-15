/**
 * Copyright (c) The openTCS Authors.
 *
 * This program is free software and subject to the MIT license. (For details,
 * see the licensing information (LICENSE.txt) you should have received with
 * this copy of the software.)
 */
package org.opentcs.guing.common.components.properties.panel;

import static java.util.Objects.requireNonNull;

import jakarta.annotation.Nonnull;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import javax.swing.ImageIcon;
import javax.swing.JPanel;
import org.opentcs.components.plantoverview.LocationTheme;
import org.opentcs.data.model.visualization.LocationRepresentation;
import org.opentcs.guing.base.components.properties.type.Property;
import org.opentcs.guing.base.components.properties.type.SymbolProperty;
import org.opentcs.guing.common.components.dialogs.DetailsDialogContent;
import org.opentcs.guing.common.util.I18nPlantOverview;
import org.opentcs.thirdparty.guing.common.jhotdraw.util.ResourceBundleUtil;

/**
 * User interface to edit a symbol property.
 */
public class SymbolPropertyEditorPanel
    extends
      JPanel
    implements
      DetailsDialogContent {

  /**
   * The possible symbols.
   */
  private final List<LocationRepresentation> fRepresentations = new ArrayList<>();
  /**
   * The symbol.
   */
  private final List<ImageIcon> fSymbols = new ArrayList<>();
  /**
   * The location theme to be used.
   */
  private final LocationTheme locationTheme;
  /**
   * The index of the selected symbols.
   */
  private int fIndex;
  /**
   * The property.
   */
  private SymbolProperty fProperty;

  /**
   * Creates new instance.
   *
   * @param locationTheme The location theme to be used.
   */
  @Inject
  @SuppressWarnings("this-escape")
  public SymbolPropertyEditorPanel(
      @Nonnull
      LocationTheme locationTheme
  ) {
    this.locationTheme = requireNonNull(locationTheme, "locationTheme");

    initComponents();
    init();
  }

  @Override // DetailsDialogContent
  public void setProperty(Property property) {
    fProperty = (SymbolProperty) property;
    fIndex = fRepresentations.indexOf(fProperty.getLocationRepresentation());

    if (fIndex == -1) {
      fIndex = 0;
    }

    updateView();
  }

  @Override // DetailsDialogContent
  public void updateValues() {
    if (fIndex < 0) {
      fProperty.setLocationRepresentation(LocationRepresentation.DEFAULT);
    }
    else {
      fProperty.setLocationRepresentation(fRepresentations.get(fIndex));
    }
  }

  @Override // DetailsDialogContent
  public String getTitle() {
    return ResourceBundleUtil.getBundle(I18nPlantOverview.PROPERTIES_PATH)
        .getString("symbolPropertyEditorPanel.title");
  }

  @Override // DetailsDialogContent
  public Property getProperty() {
    return fProperty;
  }

  private void init() {
    for (LocationRepresentation cur : LocationRepresentation.values()) {
      fRepresentations.add(cur);
      fSymbols.add(new ImageIcon(locationTheme.getImageFor(cur)));
    }
  }

  /**
   * Updates the view.
   */
  private void updateView() {
    fSymbols.clear();
    fRepresentations.clear();
    init();
    labelSymbol.setIcon(fSymbols.get(fIndex));
    labelSymbolName.setText(fRepresentations.get(fIndex).name());
  }

  // FORMATTER:OFF
  // CHECKSTYLE:OFF
  /**
   * This method is called from within the constructor to initialize the form.
   * WARNING: Do NOT modify this code. The content of this method is always
   * regenerated by the Form Editor.
   */
  // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
  private void initComponents() {
    java.awt.GridBagConstraints gridBagConstraints;

    previousSymbolButton = new javax.swing.JButton();
    nextSymbolButton = new javax.swing.JButton();
    labelSymbol = new javax.swing.JLabel();
    labelSymbolName = new javax.swing.JLabel();
    removeButton = new javax.swing.JButton();

    setLayout(new java.awt.GridBagLayout());

    previousSymbolButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/org/opentcs/guing/res/symbols/panel/back.24x24.gif"))); // NOI18N
    previousSymbolButton.setMargin(new java.awt.Insets(2, 2, 2, 2));
    previousSymbolButton.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(java.awt.event.ActionEvent evt) {
        previousSymbolButtonActionPerformed(evt);
      }
    });
    gridBagConstraints = new java.awt.GridBagConstraints();
    gridBagConstraints.gridx = 0;
    gridBagConstraints.gridy = 1;
    gridBagConstraints.fill = java.awt.GridBagConstraints.VERTICAL;
    gridBagConstraints.insets = new java.awt.Insets(4, 4, 4, 4);
    add(previousSymbolButton, gridBagConstraints);

    nextSymbolButton.setIcon(new javax.swing.ImageIcon(getClass().getResource("/org/opentcs/guing/res/symbols/panel/forward.24x24.gif"))); // NOI18N
    nextSymbolButton.setMargin(new java.awt.Insets(2, 2, 2, 2));
    nextSymbolButton.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(java.awt.event.ActionEvent evt) {
        nextSymbolButtonActionPerformed(evt);
      }
    });
    gridBagConstraints = new java.awt.GridBagConstraints();
    gridBagConstraints.gridx = 2;
    gridBagConstraints.gridy = 1;
    gridBagConstraints.fill = java.awt.GridBagConstraints.VERTICAL;
    gridBagConstraints.insets = new java.awt.Insets(4, 4, 4, 4);
    add(nextSymbolButton, gridBagConstraints);

    labelSymbol.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
    labelSymbol.setBorder(javax.swing.BorderFactory.createCompoundBorder(javax.swing.BorderFactory.createLineBorder(new java.awt.Color(0, 0, 0)), javax.swing.BorderFactory.createEmptyBorder(10, 10, 10, 10)));
    labelSymbol.setMaximumSize(new java.awt.Dimension(200, 100));
    labelSymbol.setMinimumSize(new java.awt.Dimension(100, 60));
    labelSymbol.setPreferredSize(new java.awt.Dimension(100, 60));
    gridBagConstraints = new java.awt.GridBagConstraints();
    gridBagConstraints.gridx = 1;
    gridBagConstraints.gridy = 1;
    gridBagConstraints.fill = java.awt.GridBagConstraints.BOTH;
    gridBagConstraints.weightx = 0.5;
    gridBagConstraints.weighty = 0.5;
    gridBagConstraints.insets = new java.awt.Insets(4, 4, 4, 4);
    add(labelSymbol, gridBagConstraints);

    labelSymbolName.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
    labelSymbolName.setText("Name");
    gridBagConstraints = new java.awt.GridBagConstraints();
    gridBagConstraints.gridx = 0;
    gridBagConstraints.gridy = 2;
    gridBagConstraints.gridwidth = 3;
    gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
    gridBagConstraints.insets = new java.awt.Insets(0, 6, 4, 6);
    add(labelSymbolName, gridBagConstraints);

    java.util.ResourceBundle bundle = java.util.ResourceBundle.getBundle("i18n/org/opentcs/plantoverview/panels/propertyEditing"); // NOI18N
    removeButton.setText(bundle.getString("symbolPropertyEditorPanel.button_removeSymbol.text")); // NOI18N
    removeButton.addActionListener(new java.awt.event.ActionListener() {
      public void actionPerformed(java.awt.event.ActionEvent evt) {
        removeButtonActionPerformed(evt);
      }
    });
    gridBagConstraints = new java.awt.GridBagConstraints();
    gridBagConstraints.gridx = 1;
    gridBagConstraints.gridy = 3;
    gridBagConstraints.fill = java.awt.GridBagConstraints.HORIZONTAL;
    gridBagConstraints.insets = new java.awt.Insets(4, 4, 4, 4);
    add(removeButton, gridBagConstraints);
  }// </editor-fold>//GEN-END:initComponents
  // CHECKSTYLE:ON
  // FORMATTER:ON

  private void nextSymbolButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_nextSymbolButtonActionPerformed
    if (fIndex >= fSymbols.size() - 1 || fIndex < 0) {
      fIndex = 0;
    }
    else {
      fIndex++;
    }

    updateView();
  }//GEN-LAST:event_nextSymbolButtonActionPerformed

  private void previousSymbolButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_previousSymbolButtonActionPerformed
    if (fIndex <= 0 || fIndex >= fSymbols.size()) {
      fIndex = fSymbols.size() - 1;
    }
    else {
      fIndex--;
    }

    updateView();
  }//GEN-LAST:event_previousSymbolButtonActionPerformed

  private void removeButtonActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_removeButtonActionPerformed
    labelSymbol.setIcon(null);
    labelSymbolName.setText(LocationRepresentation.DEFAULT.name());
    fIndex = -2;  // Invalid index, so in updateValues() no Icon will be loaded
  }//GEN-LAST:event_removeButtonActionPerformed

  // FORMATTER:OFF
  // CHECKSTYLE:OFF
  // Variables declaration - do not modify//GEN-BEGIN:variables
  private javax.swing.JLabel labelSymbol;
  private javax.swing.JLabel labelSymbolName;
  private javax.swing.JButton nextSymbolButton;
  private javax.swing.JButton previousSymbolButton;
  private javax.swing.JButton removeButton;
  // End of variables declaration//GEN-END:variables
  // CHECKSTYLE:ON
  // FORMATTER:ON
}
