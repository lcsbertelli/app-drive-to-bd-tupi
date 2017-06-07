package com.google.api.services.samples.drive.cmdline;

import java.sql.ResultSet;

/**
 *
 * @author lucas
 */
public class Insert {

    public Insert() {
    }
    public static void main(String[] args) {
            Conexao c = new Conexao("PostgreSql","localhost","5432","tupi","tupi","123456");
            c.conect();
            //String query = "INSERT INTO tupi.CONTROLE_CARGA (id, data_ultima_carga) VALUES (3, '1900-01-01T01:02:03.001Z');";
            String query = "SELECT data_ultima_carga AT TIME ZONE 'UTC' FROM tupi.CONTROLE_CARGA where id=1;";
            ResultSet result = c.query(query);
            try{    
                if (result.next()){  
                    String dt_ult_carga_drive = result.getString(1);

                    System.out.println("Dt ult carga bd: "+dt_ult_carga_drive);
                }
            }catch(Exception e){
                System.out.println(" "+e.getMessage());
            }
                
            c.disconect();
        }        
}
