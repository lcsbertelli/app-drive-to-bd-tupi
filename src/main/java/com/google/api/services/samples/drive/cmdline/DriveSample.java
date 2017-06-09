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

import java.time.*;


public class DriveSample {

  /**
   * Be sure to specify the name of your application. If the application name is {@code null} or
   * blank, the application will log a warning. Suggested format is "MyCompany-ProductName/1.0".
   */
  private static final String APPLICATION_NAME = "UFF-AppCargaTupi/1.1";  

  private static final String DIR_USUARIO_HOME = System.getProperty("user.home");
  
  private static final String DIR_FOR_DOWNLOADS = "C:\\Users\\lucas\\download_tupi";

  /** Directory to store user credentials. */
  private static final java.io.File DATA_STORE_DIR =
      new java.io.File(System.getProperty("user.home"), ".store/drive_tupi");

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

  public static void main(String[] args) {    
    
    String dt_ult_carga_formata_drive = null;
    Conexao c; //da classe Generecia Conexao
    ResultSet resultSet, resultSet2;
    resultSet = null; 
    resultSet2 = null;
    LocalDate date_name; // somente a data pelo norme do arquivo
    
      
    Preconditions.checkArgument(
        !DIR_FOR_DOWNLOADS.startsWith("Enter "),
        "Please enter download directory in %s", DriveSample.class);

    try {
      //Teste verificiar conteudo:  System.out.println(CAMINHO_UPLOAD);
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);
      // authorization
      Credential credential = authorize();
      // set up the global Drive instance
      drive = new Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(
          APPLICATION_NAME).build();        

//####### CONEXAO COM BANCO ##########################################################
    
  
      c = new Conexao("PostgreSql","localhost","5432","tupi","tupi","123456");
      c.conect();
      // Data da Ultima Carga Realizada em UTC - Assumindo q sempre sera a de ID 1, atualizando ela ao final dessa carga.
      String query = "SELECT data_ultima_carga AT TIME ZONE 'UTC' FROM tupi.CONTROLE_CARGA where id=1;";
      resultSet = c.query(query);      
      c.disconect();// já salvei no resultSet já posso fechar a conexao.
      try{    
        if (resultSet.next()){  
            String data_completa_utc = resultSet.getString(1);
            System.out.println("Dt ult carga bd: "+data_completa_utc);
            dt_ult_carga_formata_drive = formatToDrive(data_completa_utc); // add T e Z
            System.out.println("Dt formato drive:  "+dt_ult_carga_formata_drive);
            
        
        }
      }catch(Exception e){
            System.out.println(" "+e.getMessage());
      }      
      
//####### fim AQUI IRIA NO BANCO E PEGA A DATA DE LÁ   
if(dt_ult_carga_formata_drive != null){
      String pageToken = null;
      do {          
        FileList result = drive.files().list()
                .setCorpora("user") // Abrange o MyDrive e Shared With Me                
                // (Somente os com tipo/MIME .dat) and (ID da Pasta Novo monitor banco de dados in Coleção de Pastas do Arquivo(Parents)
                // and ((dt_Create > dat_ultima_carga) or (dt_ModifiedTime > dat_ultima_carga))
                // id do mydrive: 0AMfZkpEPzZbRUk9PVA
                // id Alvo do monitor de bd: 0Bx9yd3l3M5AaZzlZNXJjRE9Wemc
                .setQ("(mimeType='application/octet-stream') and ('0Bx9yd3l3M5AaZzlZNXJjRE9Wemc' in parents) and "
                        + "((createdTime > '" + dt_ult_carga_formata_drive + "') or (modifiedTime > '" + dt_ult_carga_formata_drive + "'))") // TESTE: ID Pasta MyDrive Raiz                 
                
                .setFields("nextPageToken, files(id, name, parents, createdTime, modifiedTime, mimeType)") //Nao existe mas Title na v3, agora é name.
                .setPageToken(pageToken)                
                .execute();    
        
        List<File> files = result.getFiles();      
        //List<File> files = result.getItems(); //metodo defasado, usado na API v2.
        System.out.println(files.size());
        
          printFiles(files);
        
//### Fazer Download dos Arquivos Novos - os inseridos em Files
        //### Nao precisa deletar todos arquivos da basta download_tupi. Eu so abro os arquivos com o name contido na busca dos novos ou alterados.              
        for (File file : files) {
            downloadFile(file);
        }   
    
//### fim Download
        
//###  Leitura de Arquivos ##############
        
        for (File file : files) {
            try{
                //tem colocar ID para as linhas e ID_TEMPO, ID_TELESCOPIO
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
                
                
                
                //#### Já existe carga desse Dia and Mes and Ano ? Se sim, DELETA todos os registros e insere novamente.
                //### Pois nao temos como saber se um campo foi excluido. E afirmar com certeza que o arquivo continua na mesma ordem, q nenhum reg foi add.
                c.conect();

  // !!!!!!!! ########## TEM QUE SER NULL OUTROS CAMPO DO DIM_TEMPO NA QUERY ABAIXO ??????????????????????????????????????????????????????????????????            
                query = "SELECT id_tempo FROM DIM_TEMPO WHERE num_ano = " + ano + " and num_mes = " + mes + " and num_dia = " + dia + ";";
                c.query(query);
                resultSet2 = c.query(query); // Posso usar o mesmo resultSet ? Nao sei pq tem q dar close depois de usar o RS                
                c.disconect();// já salvei no resultSet já posso fechar a conexao.
                try{    
                    if(resultSet2.next()){  // Se tem registro, é pq esse mes ja foi carregado no BD e foi modificado. Ou deletado e inserido novamente.
                        c.conect(); 
                        // ##### 1 - FAzer Delete dos Registros 
                        // AINDA NAO TEM REGISTROS LA, Q QUERY É EMPTY
                        // coloquei 1 registro e nao excluiu, ANALISAR.
                        query2 = "DELETE FROM FAT_SINAIS WHERE id_tempo =" + resultSet2.getString(1) + ";";                                                          
                        c.query(query);                        
                        
                        
                        //#### 2 - Delete o registro com o id_tempo.
                        query = "DELETE FROM DIM_TEMPO WHERE id_tempo =" + resultSet2.getString(1) + ";";
                        c.query(query);                        
                        c.disconect();
                    }                    
                    // Aqui é  Igual sempre da Insert, após DELETE ou direto, se for um arquivo novo.
                     c.conect();
                     query = "INSERT INTO DIM_TEMPO (id_tempo, dt_data_completa, num_ano, num_mes, num_dia, num_trimestre, num_semestre, num_hora, num_minuto, num_segundo)" 
                            + "VALUES (nextval('seq_id_tempo'),null," + ano + ", " + mes + ", " + dia + ", CAST(null as integer), CAST(null as integer), CAST(null as integer), CAST(null as integer), CAST(null as integer));"; 
                     c.query(query);                    
                     c.disconect();
                     // #### TEM QUE FAZER UM LOOP AQUI PULANDO AS LINHAS DO ARQUIVO E DANDO INSER NA FAT.SINAIS COM O ID ATUAL DO TEMPO E DAR INSERT EM TELESCOPIOS TBM, NAO INSERT MAS PEGAR O ID DO TELESCOPIO, COMO ESSA CARGA SO PARA O TUPI POSSO SETAR NA MAO FORÇADO O ID NEH?
                    
                    
                }catch(Exception e){
                    System.out.println(" "+e.getMessage());
                }  
            
            
            }catch(IOException e){
                System.out.println("Erro ao Manipular o Arquivo: " + file.getName());
                System.out.println("msg: "+e.getMessage());
            }    
        }
        
      

  
//### Fim leitura arquivos ##############        
        pageToken = result.getNextPageToken();
      } while (pageToken != null); 
      
      // ##### TEM Q DA UPDATE NA data_ultima_carga
      // UPDATE tupi.controle_carga SET data_ultima_carga = '1500-01-02T01:04:03Z' WHERE id = 1;
      View.header1("Success!"); // do codigo original excluir depois se nao for usar.
      return;
     } // if dt_ult_carga != null

   
//###    liberar recursos
    
    try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (resultSet2 != null) {
                    resultSet2.close();
                }
                if (c.getStatment() != null) {
                    c.getStatment().close();
                }
                if (c != null) {
                    c.disconect();
                }

    } catch (SQLException ex) {
        System.err.println(ex.getMessage());
    }
    
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

}
