/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.centralenantes.bdonntp2;

import java.sql.*;
import java.lang.*;
import java.util.logging.*;

/**
 *
 * @author muruowang
 */
public class BordereauDeLivraison {

    public static void main(String[] argv) {
        try {
            Class.forName("org.postgresql.Driver");

        } catch (java.lang.ClassNotFoundException e) {
            System.err.println("ClassNotFoundException :" + e.getMessage());

        }

        // connexion à la base 
        try {
            Connection connect = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5432/TP1", "postgres", "yyn941201");

            int id_ordre = 1;
            // écrire des requêtes  1
            String query = "select * from ordre_mission join quai_chargement using(id_quai) join chauffeur using(id_chauffeur) where id_ordre = ?";
            PreparedStatement stmt = connect.prepareStatement(query);
            stmt.setInt(1, id_ordre);
            ResultSet res = stmt.executeQuery();
            while (res.next()) {
                System.out.println("Nom de chauffeur     :    " + res.getString("nom_chauffeur"));
                System.out.println("Prenom de chauffeur  :    " + res.getString("prenom_chauffeur"));
                System.out.println("Quai de chargement   :    " + res.getString("adresse_quai"));
                System.out.println("Date de chargement   :    " + res.getString("date_heure_chargement"));
            }
            stmt.close();

            // écrire des requêtes  2
            String query1 = "select nom_entreprise,  adresse_depot , nom_produit, quantite, date_livraison from liste_commande \n"
                    + "join commande using(id_commande)\n"
                    + "join produit using(id_produit)\n"
                    + "join entreprise using(siret)\n"
                    + "join depot using(id_depot)\n"
                    + "where id_ordre = ? ";
            PreparedStatement stmt1 = connect.prepareStatement(query1);
            stmt1.setInt(1, id_ordre);
            ResultSet res1 = stmt1.executeQuery();
            while (res1.next()) {
                System.out.println();
                System.out.println("Nom de l'entreprise    :    " + res1.getString("nom_entreprise"));
                System.out.println("Adresse de depôt       :    " + res1.getString("adresse_depot"));
                System.out.println("date livraison         :    " + res1.getString("date_livraison"));
                System.out.println("Nom de produit         :    " + res1.getString("nom_produit"));
                System.out.println("Quantité de produit    :    " + res1.getString("quantite"));
            }
            stmt.close();

            connect.close();
            // cloture de Driver
            Driver theDriver = DriverManager.getDriver("jdbc:postgresql://127.0.0.1:5432/TP1");
            DriverManager.deregisterDriver(theDriver);

        } catch (SQLException ex) {
            System.err.println("SQLException :  " + ex.getMessage());
        }

    }
}
