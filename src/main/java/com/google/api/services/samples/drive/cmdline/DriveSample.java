package com.google.api.services.samples.drive.cmdline;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
import com.google.api.services.drive.model.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Map;

import java.time.*;


public class DriveSample {

//<editor-fold defaultstate="collapsed" desc="DEFINICAO DAS CONSTANTES GLOBAIS DA APP">
    
    private static final String APPLICATION_NAME = "UFF-AppCargaTupi/1.1";
    
    //<editor-fold defaultstate="collapsed" desc="ALTERAR PARA CADA MAQUINA DIFERENTE">
    private static final String DIR_USUARIO_HOME = System.getProperty("user.home");
    
    private static final String DIR_FOR_DOWNLOADS = "C:\\Users\\lucas\\download_tupi";
    
    /** Directory to store user credentials. */
    private static final java.io.File DATA_STORE_DIR =
            new java.io.File(System.getProperty("user.home"), ".store/drive_tupi");
    
    //Conexao da classe Generecia Conexao
    private static final Conexao CONEXAO = new Conexao("PostgreSql","localhost","5432","tupi","tupi","123456");
//</editor-fold>
 
    
    /**
     * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
     * globally shared instance across your application.
     */
    private static FileDataStoreFactory dataStoreFactory;
    
    /** Global instance of the HTTP transport. */
    private static HttpTransport httpTransport;
    
    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
    
    /** Global Drive API client. */
    private static Drive drive;
//</editor-fold>

  //<editor-fold defaultstate="collapsed" desc="AUTENTICACAO GOOGLE DRIVE">
  /** Authorizes the installed application to access user's protected data. */
  private static Credential authorize() throws Exception {
      // load client secrets
      GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY,
              new InputStreamReader(DriveSample.class.getResourceAsStream("/client_secrets.json")));
      if (clientSecrets.getDetails().getClientId().startsWith("Enter")
              || clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
          System.out.println(
                  "Enter Client ID and Secret from https://code.google.com/apis/console/?api=drive "
                          + "into drive-cmdline-sample/src/main/resources/client_secrets.json");
          System.exit(1);
      }
      // set up authorization code flow
      GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
              httpTransport, JSON_FACTORY, clientSecrets,
              Collections.singleton(DriveScopes.DRIVE)).setDataStoreFactory(dataStoreFactory)
              .build();
      // authorize
      return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
  }
//</editor-fold>

  public static void main(String[] args) {    
    
 // ## Var de LOGICA     
    String dt_ult_carga_formata_drive = null; //DATA DA ULTIMA CARGA NO FORMATO DO DRIVE RFC 3339 ISO 8601
    LocalDate date_name; // Data obtida pelo nome do arquivo
    
 // ## Var JDBC    
    String query;
    ResultSet resultSet, resultSet2;
    resultSet = null; 
    resultSet2 = null;    
    
      
    Preconditions.checkArgument(
        !DIR_FOR_DOWNLOADS.startsWith("Enter "),
        "Please enter download directory in %s", DriveSample.class);

    try {      
        //<editor-fold defaultstate="collapsed" desc="SETA VARIAVEIS DO GOOGLE DRIVE e chama AUTENTICACAO">
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);
        // authorization
        Credential credential = authorize();
        // set up the global Drive instance
        drive = new Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(
                APPLICATION_NAME).build();
//</editor-fold>
    

    try{
        //<editor-fold defaultstate="collapsed" desc="OBTER DATA da ULTIMA CARGA DADOS">
        
        
        CONEXAO.conect();
        // Data da Ultima Carga Realizada em UTC.
        query = "SELECT data_ultima_carga AT TIME ZONE 'UTC' FROM tupi.CONTROLE_CARGA where id IN (SELECT MAX(id) FROM tupi.CONTROLE_CARGA);";
        resultSet = CONEXAO.query(query);
        CONEXAO.disconect();// já salvei no resultSet já posso fechar a conexao.
        if (resultSet.next()){  // vai para primeira linha do resultSet
            String data_completa_utc = resultSet.getString(1);
            System.out.println("Dt ult carga bd: "+data_completa_utc);
            dt_ult_carga_formata_drive = formatToDrive(data_completa_utc); // add T e Z
            System.out.println("Dt formato drive:  "+dt_ult_carga_formata_drive);
        }
        }catch(SQLException e){
            System.out.println(" "+e.getMessage());
        }finally{
            if (resultSet != null) {
                resultSet.close();
            }
            if (CONEXAO.getStatement() != null) {
                CONEXAO.getStatement().close();
            }
            if (CONEXAO != null) {
                CONEXAO.disconect();
            }
        }
//</editor-fold>
      
      
// cuidado: Identacao errada 
if(dt_ult_carga_formata_drive != null){
      String pageToken = null;
      List<File> files = new LinkedList<File>();
      
      do {   // Token para paginas -- tenta sem token, pq no files so fica a ultima pagina, o ultimo token desse jeito.       
        FileList result = drive.files().list()
                .setCorpora("user") // Abrange o MyDrive e Shared With Me                
//<editor-fold defaultstate="collapsed" desc="Comentarios do Filtro">
                // (Somente os com tipo/MIME .dat) and (ID da Pasta Novo monitor banco de dados in Coleção de Pastas do Arquivo(Parents)
                // and ((dt_Create > dat_ultima_carga) or (dt_ModifiedTime > dat_ultima_carga))
                // and nome começa com prefixo: DB_Tupi_
                // id do mydrive: 0AMfZkpEPzZbRUk9PVA
                // id Alvo do monitor de bd: 0Bx9yd3l3M5AaZzlZNXJjRE9Wemc
//</editor-fold>
                .setQ("(mimeType='application/octet-stream')"  // tipo .dat
                        + "and ('0Bx9yd3l3M5AaZzlZNXJjRE9Wemc' in parents)" // id da pasta: Novo monitor banco de dados
                        + "and ((createdTime > '" + dt_ult_carga_formata_drive + "') or (modifiedTime > '" + dt_ult_carga_formata_drive + "'))"                 
                        + "and (name contains 'DB_Tupi_')") // name começa com prefixo , pois para a propriedade name o contains é sempre por prefixo.
                .setFields("nextPageToken, files(id, name, parents, createdTime, modifiedTime, mimeType)") //Nao existe mas Title na v3, agora é name.
                .setOrderBy("modifiedTime")// ordena pela dt_modificacao CRESCENTE, de forma que os arquivos MAIS NOVOS FICAM POR ULTIMO na list e sobrescrevem suas duplicatas ao serem baixados.              
                .setPageToken(pageToken)                
                .execute();            
        
        files.addAll(result.getFiles());        
        
        pageToken = result.getNextPageToken();
      } while (pageToken != null);
      System.out.println("qtde arquivos: "+files.size());
      
//### Printa Metadados dos arquivos        
      //printFiles(files);

//### fim Printa Metadados dos arquivos
        
// #################### DESCOMENTE PARA ACHAR DUPLICADAS NA CARGA INICIAL e Comente a parte do DOWNLOAD Abaixo ################################    
// #################### e Comente a parte do DOWNLOAD Abaixo!!  - até resolver como baixar em Gzip   ###############################
    //  System.out.println(contaArquivosNomeRepetidoDrive(files));
    //  return;

// #################### FIM SOMENTE PARA CARGA INICIAL ################################   


//### Fazer Download dos Arquivos Novos - os inseridos em Files
//## Observe que a ordenação faz com que os mais recentes sobrecrevam suas duplicatas mais antigas.
        for (File file : files) {
            downloadFile(file);
        }
    
//### fim Download
        
//###  Leitura de Arquivos ##############
        
        for (File file : files) {
            try{
                
                Integer id_tempo = null;
      //###### por agora só funciona para o TUPI, logo o ID forçado para 1.          
                Integer id_telescopio = 1; 
                String tu_str, valor_vertical, valor_escaler;
                Double tu_double;
                ZonedDateTime tu; // TU como DateTime e UTC setado.
                // split datas do getName                
                String ano, mes, dia;
                String query2;
                
                
                java.io.File parentDir = new java.io.File(DIR_FOR_DOWNLOADS);            
                FileInputStream inputStream = new FileInputStream(new java.io.File(parentDir, file.getName()));                
                
                //InputStream is = new FileInputStream("arquivo.txt");
                InputStreamReader isr = new InputStreamReader(inputStream);
                BufferedReader br = new BufferedReader(isr);
                
                //splitando data do Name
                //Name: DB_Tupi_2014_12_17.dat
                String[] name_splitada = file.getName().split("_");
                ano = name_splitada[2]; //pq começa em 0.
                mes = name_splitada[3];
                dia = name_splitada[4];
                dia = dia.substring(0,(dia.length()-4)); // retirar .dat da String.
                
                date_name = LocalDate.of(Integer.parseInt(ano), Integer.parseInt(mes), Integer.parseInt(dia));
                
                System.out.printf("ano: %s\n",  ano);
                System.out.printf("mês: %s\n",  mes);
                System.out.printf("dia: %s\n",  dia);
                
 //###  LOOP DE PULAR LINHA READLINE INSERIR AQUI          
                String linha_completa = br.readLine();
                //linha completa
                System.out.printf("resp: %s \n", linha_completa);
                String[] linha_splitada = linha_completa.split("	"); // tab caracter, não são espaços.
                //System.out.printf("%s \n", linha_splitada[2]);
                
                tu_str = linha_splitada[0];
                tu_double = Double.valueOf(tu_str).doubleValue();
                tu = ConvertTUtoTime.tuToDateTime(tu_double, date_name);
                
                valor_vertical = linha_splitada[1];
                valor_vertical = valor_vertical.substring(0,(valor_vertical.length()-1)); // tirando o caracter ponto no final.
                valor_escaler = linha_splitada[2];
                valor_escaler = valor_escaler.substring(0,(valor_escaler.length()-1));
                System.out.printf("tempo universal em ZonedDateTime: %s\n",  tu);
                System.out.printf("v. vertical: %s\n",  valor_vertical);
                System.out.printf("v. escaler: %s\n",  valor_escaler);
                
            // Já foi feito a  carga?        
            try{         
                //#### Já existe carga desse Dia and Mes and Ano ? Se sim, DELETA todos os registros e insere novamente.
                //### Pois nao temos como saber se um campo foi excluido. E afirmar com certeza que o arquivo continua na mesma ordem, q nenhum reg foi add.
                

  // !!!!!!!! ########## TEM QUE SER NULL OUTROS CAMPO DO DIM_TEMPO NA QUERY ABAIXO ??????????????????????????????????????????????????????????????????            
                CONEXAO.conect();
                query = "SELECT id_tempo FROM DIM_TEMPO WHERE num_ano = " + ano + " and num_mes = " + mes + " and num_dia = " + dia + ";";
                CONEXAO.query(query);
                resultSet2 = CONEXAO.query(query); // Posso usar o mesmo resultSet ? Nao sei pq tem q dar close depois de usar o RS                
                CONEXAO.disconect();
                if(resultSet2.next()){
                    id_tempo = resultSet2.getInt(1);
                    System.out.println("id tempo já existe, é: "+id_tempo);            
                

  // !!!!!!!!!!!! AQUI TEM Q CHAMAR UM PROCEDURE Q DELE OS AGREGADOS TBM ############################################
                    // ##### 1 - FAzer Delete dos Registros 
                    // AINDA NAO TEM REGISTROS LA, Q QUERY É EMPTY
                    // coloquei 1 registro e nao excluiu, ANALISAR.
                    
                    //Verifica se tem fatos registrados para esse Id tempo
                    CONEXAO.conect();
                    // id_telescopio = 1 é o TUPI
                    query = "SELECT id_tempo, id_telescopio FROM FAT_SINAIS WHERE id_tempo = "+ id_tempo +" AND id_telescopio = " + id_telescopio + ";";
                    CONEXAO.query(query);
                    resultSet = CONEXAO.query(query); // Posso usar o mesmo resultSet ? Nao sei pq tem q dar close depois de usar o RS                
                    CONEXAO.disconect();
                    
                    //Se tem registros, deleta.
                    if(resultSet.next()){
                        // DELETAR REGISTROS
                        CONEXAO.conect();                        
                        query = "DELETE FROM FAT_SINAIS WHERE id_tempo =" + id_tempo + " AND id_telescopio =" + id_telescopio + ";";                                                          
                        CONEXAO.runStatementDDL(query);                        
                        CONEXAO.disconect();
                    }                            
                }                                            
                // Se é uma nova carga. Um arquivo novo.
                if(id_tempo==null){
                    //######## INSERE O NOVO DIM_TEMPO ###############    
                     CONEXAO.conect();
                     query = "INSERT INTO DIM_TEMPO (id_tempo, dt_data_completa, num_ano, num_mes, num_dia, num_trimestre, num_semestre, num_hora, num_minuto, num_segundo)" 
                            + "VALUES (nextval('seq_id_tempo'),null," + ano + ", " + mes + ", " + dia + ", null, null, null, null, null);"; 
                     CONEXAO.runStatementDDL(query);                    
                     CONEXAO.disconect();                
                }
                
                // INSERE FAT_SINAIS ######################## id_telescopio = 1 fixo tupi
                CONEXAO.conect();
                query = "INSERT INTO FAT_SINAIS (id_tempo, id_telescopio, valor_vertical, valor_escaler) "
                        + "VALUES ("+ id_tempo + ", "+ id_telescopio + " ," + valor_vertical + " , "+ valor_escaler + ");";                        
                CONEXAO.runStatementDDL(query);                    
                CONEXAO.disconect(); 
                
                
     //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! nova feature !!!!!!!!!!!!!!!!!!!!!!!
                //##### usa o id_tempo para carga dos fatos. Em fat_sinais.    
                
                
                 // #### TEM QUE FAZER UM LOOP AQUI PULANDO AS LINHAS DO ARQUIVO E DANDO INSER NA FAT.SINAIS COM O ID ATUAL DO TEMPO E DAR INSERT EM TELESCOPIOS TBM, NAO INSERT MAS PEGAR O ID DO TELESCOPIO, COMO ESSA CARGA SO PARA O TUPI POSSO SETAR NA MAO FORÇADO O ID NEH?
                    
            // try verifica se ja teve carga e insere        
            }catch(SQLException e){
                System.out.println(" "+e.getMessage());
            }  
            
        // try de manipular arquivo
        }catch(IOException e){
            System.out.println("Erro ao Manipular o Arquivo: " + file.getName());
            System.out.println("msg: "+e.getMessage());
        }    
    } // Fim loop file in files   ##############        
    
        
//    // INSERE DATETIME em UTC ATUAL PARA UMA NOVA DATA_ULT_CARGA ##################################################
//    try{
//        ZonedDateTime agora = ZonedDateTime.now(ZoneId.of("Z"));  
//        CONEXAO.conect();        
//        query = "INSERT INTO tupi.CONTROLE_CARGA (id, data_ultima_carga) VALUES (nextval('seq_id_controle_carga'), '"+ agora +"');";
//        
//        CONEXAO.runStatementDDL(query);                        
//        CONEXAO.disconect();
//    }catch(Exception e){
//                System.out.println(" "+e.getMessage());
//    }finally{
//                if (resultSet != null) {
//                    resultSet.close();
//                }
//                if (CONEXAO.getStatement() != null) {
//                    CONEXAO.getStatement().close();
//                }
//                if (CONEXAO != null) {
//                    CONEXAO.disconect();
//                }
//    } 
      
      
 } // if dt_ult_carga != null

   
//<editor-fold defaultstate="collapsed" desc="LIBERAR RECURSOS JDBC">
//###    liberar recursos

try {
    if (resultSet != null) {
        resultSet.close();
    }
    if (resultSet2 != null) {
        resultSet2.close();
    }
    if (CONEXAO.getStatement() != null) {
        CONEXAO.getStatement().close();
    }
    if (CONEXAO != null) {
        CONEXAO.disconect();
    }
    
} catch (SQLException ex) {
    System.err.println(ex.getMessage());
}
//</editor-fold>
    
} catch (IOException e) {
  System.err.println(e.getMessage());
} catch (Throwable t) {
  t.printStackTrace();
}

System.exit(1);
} // main

  // Metodo para adicionar o T na separacao de Data e Time, e o Z no final. Aceita datas no Padrao yyyy-mm-dd hh:mm:ss 
  // Ou seja, a data de entrada já está em UTC. Se não tem que converter antes.
  private static String formatToDrive(String data_completa_utc){
        String[] data_splitada = data_completa_utc.split(" ");
        String data = data_splitada[0];
        String time = data_splitada[1];
        //System.out.println("data:  "+data);
        //System.out.println("time:  "+time);
        String dt_formato_drive = data+"T"+time+"Z"; // Esse formato Usado para Buscar no Drive e Insert no postgres      
        return dt_formato_drive; 
  } 
  
//### PRINTAS PROPRIEDADES PARA TESTE ####################
  private static void printFiles(List<File> files) {   
    if (files == null || files.size() == 0) {
        System.out.println("No files found.");
    } else {
        System.out.println("Files:");
        for (File file : files) {
            System.out.printf("Name: %s ID: (%s) Parents: (%s) DataTime Criação: %s DataTime Modificacao: %s MIME Type: %s\n", 
                    file.getName(), file.getId(), file.getParents(), file.getCreatedTime(), file.getModifiedTime(), file.getMimeType());                

        }
    }
  }  
//####################################################

//#############################   BAIXAR 1 ARQUIVO    #######################  
    private static void downloadFile(File file) throws IOException {
        String fileId = file.getId();
        // create parent directory (if necessary)
        java.io.File parentDir = new java.io.File(DIR_FOR_DOWNLOADS);
        if (!parentDir.exists() && !parentDir.mkdirs()) {
          throw new IOException("Unable to create parent directory");
        }
        OutputStream outputStream = new FileOutputStream(new java.io.File(parentDir, file.getName()));               
        drive.files().get(fileId)
                    .executeMediaAndDownloadTo(outputStream);        
            
    }
    
    private static Map<String, Integer> contaArquivosNomeRepetidoDrive(List<File> files){        
        
        if (files == null || files.size() == 0) {
            System.out.println("No files found.");
            return null;
        } else {
            Map<String, Integer> nomesRepetidos = new HashMap<String, Integer>();
            for (File file : files) {
                if (nomesRepetidos.containsKey(file.getName())){    
                    nomesRepetidos.put (file.getName(), nomesRepetidos.get(file.getName()) + 1);
                } else {
                    nomesRepetidos.put (file.getName(), 1);
                }   
            }
            //excluir os q nao repetem
            for (File file : files) {
                if (nomesRepetidos.get(file.getName()) == 1){
                   nomesRepetidos.remove(file.getName());                    
                }
            }        
            return nomesRepetidos;
        } 	
    } 
    
}//class
