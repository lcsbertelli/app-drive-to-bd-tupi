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
import excecoes.TUmaiorQMeiaNoiteException;

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
import java.time.temporal.IsoFields;
import java.util.Calendar;


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
    ResultSet resultSet;
    resultSet = null;      
    
      
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
    

//###        OBTER DATA da ULTIMA CARGA DADOS ##########
        dt_ult_carga_formata_drive = getDataUltimaCarga();
        System.out.println("dt_ult_carga_formata_drive= "+dt_ult_carga_formata_drive);
         
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
                    Integer hora, min, seg;
                    Integer trimestre, semestre;

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
               
                    // Deleta Carga Anteriores Relacionadas a Esse Dia, se houverem. + agregados + dim_tempo        
                    deletaCargaAnterior(ano, mes, dia, id_telescopio, date_name);
// #### #########!!!!!!!!!!!!!!!!!! INSERIR CHAMADA PROCEDURE DELETA AGREGADOS !!!!!!!!!!!!####################

                    //Insere um novo Dim_tempo para cada TU e depois preenche o fato desse dim grao tu.
                    //chamar procedure calcula_agregados
                    //###  LOOP DE PULAR LINHA READLINE INSERIR AQUI
                    while( br.ready() ){
                        //lê a proxima linha
                        String linha_completa = br.readLine();
                                            
                        String[] linha_splitada = linha_completa.split("	"); // tab caracter, não são espaços.                        

                        tu_str = linha_splitada[0];
                        tu_double = Double.valueOf(tu_str).doubleValue();                        
                        try {
                            tu = ConvertTUtoTime.tuToDateTime(tu_double, date_name);
                        } catch (TUmaiorQMeiaNoiteException ex) {
                            continue; // Excecao para descartar o Registro atualmente sendo lido.
                        }
                        hora = tu.getHour();
                        min = tu.getMinute();
                        seg = tu.getSecond();
                        trimestre = YearMonth.from(tu).get(IsoFields.QUARTER_OF_YEAR);
                        if(tu.getMonthValue()<7)
                           semestre = 1;
                        else
                           semestre = 2;     
                        valor_vertical = linha_splitada[1];
                        valor_vertical = valor_vertical.substring(0,(valor_vertical.length()-1)); // tirando o caracter ponto no final.
                        valor_escaler = linha_splitada[2];
                        valor_escaler = valor_escaler.substring(0,(valor_escaler.length()-1));                        
                        
                        id_tempo = insereDimTempo(tu, ano, mes, dia, trimestre, semestre, hora, min, seg);
                        insereFatSinais(id_tempo, id_telescopio, valor_vertical, valor_escaler);
// #### #########!!!!!!!!!!!!!!!!!!! INSERIR CHAMADA PROCEDURE CALCULA AGREGADOS !!!!!!!!!!!!####################
                       
                    } //loop prox linha arquivo                    
            // try do Loop files       
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
   
//<editor-fold defaultstate="collapsed" desc="LIBERAR RECURSOS JDBC">
//###    liberar recursos

            try {
                if (resultSet != null) {
                    resultSet.close();
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
     } // if data_formata     
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

    private static Integer insereDimTempo (ZonedDateTime tu, String ano, String mes, String dia, Integer num_trimestre, Integer num_semestre, Integer num_hora, Integer num_minuto, Integer num_segundo){
        //######## INSERE O NOVO DIM_TEMPO ###############    
            int id_tempo;
            ResultSet resultSet;
            CONEXAO.conect();
            String query;
            query = "INSERT INTO DIM_TEMPO (id_tempo, dt_data_completa, num_ano, num_mes, num_dia, num_trimestre, num_semestre, num_hora, num_minuto, num_segundo)" 
                   + "VALUES (nextval('seq_id_tempo'),'"+ tu +"'," + ano + ", " + mes + ", " + dia + ", "+num_trimestre+", "+num_semestre+", "+num_hora+", "+num_minuto+","+num_segundo+");"; 
            CONEXAO.runStatementDDL(query);
            
        // ### set esse id_tempo criado na variavel - posso usar currval, ainda estou na msm sessão nao fechei a conexao
            query = "select currval('seq_id_tempo');";
            resultSet = CONEXAO.query(query);
            try{
                if(resultSet.next()){
                    id_tempo = resultSet.getInt(1);
                    //System.out.println("id_tempo currval: "+id_tempo);                     
                    CONEXAO.disconect();
                    return id_tempo;
                }else
                    return null;
            }catch(SQLException e){
                System.out.println(" "+e.getMessage());
                return null;
            }finally{
                try{
                    if (resultSet != null) {
                        resultSet.close();
                    }            
                    if (CONEXAO.getStatement() != null) {
                        CONEXAO.getStatement().close();
                    }
                }catch(SQLException e){
                    System.out.println(" "+e.getMessage());
                }  
                if (CONEXAO != null) {
                    CONEXAO.disconect();
                }
            }                   
    }   

    //### Deleto Todos os Fatos (fat_sinais) com essa Data. LocalDate.
    // + deleto todos os dim_tempo que envolvem essa Data.
    // ou seja, deleto os graos minimos dt_data_completa e seus agregados superiores
    // FALTA CHAMAR PROCEDURE AGREGADOS DEL
    private static void deletaCargaAnterior(String ano, String mes, String dia, int id_telescopio, LocalDate date_name){
            ResultSet resultSet = null;
            ResultSet resultSetIdsTempo = null;
            try{
                int id_tempo;
                String query;                
                List<Integer> ids_tempo = new LinkedList<Integer>();
                LocalDate dia_seguinte = date_name.plusDays(1);
                
                delReg00h00m00sProxDia(dia_seguinte, id_telescopio); // deleta o reg do dia SEGUINTE 00:00:00, pois esse arquivo pode duplicalo ao final
                
                //Não deleto os 00:00:00, pois eles estao sempre unitarios e atualizados. E o arquivo deste Dia só insere valores a partir disso, nunca tem valores de 00:00:00 que o duplicariam
                CONEXAO.conect();
                query = "SELECT id_tempo FROM DIM_TEMPO WHERE num_ano = " + ano + " and num_mes = " + mes + " and num_dia = " + dia + ""
                        + "and dt_data_completa AT TIME ZONE 'UTC' > '"+ano+"-"+mes+"-"+dia+" 00:00:00';";                                       
                resultSetIdsTempo = CONEXAO.query(query);              
                CONEXAO.disconect();
                while(resultSetIdsTempo.next()){ // Enquanto tiver id_tempo de cargas anteriores para esse dia  LocalDate
                    ids_tempo.add(resultSetIdsTempo.getInt(1));
                    id_tempo = resultSetIdsTempo.getInt(1);            
                    CONEXAO.conect();
                    // id_telescopio = 1 é o TUPI
                    query = "SELECT id_tempo, id_telescopio FROM FAT_SINAIS WHERE id_tempo = "+ id_tempo +" AND id_telescopio = " + id_telescopio + ";";
                    CONEXAO.query(query);
                    resultSet = CONEXAO.query(query); // Posso usar o mesmo resultSet ? Nao sei pq tem q dar close depois de usar o RS                
                    CONEXAO.disconect();
                    
                    //Se tem registros na FAT_SINAIS p/ esse id, deleta.
                    if(resultSet.next()){
                        // DELETAR REGISTROS
                        CONEXAO.conect();                        
                        query = "DELETE FROM FAT_SINAIS WHERE id_tempo =" + id_tempo + " AND id_telescopio =" + id_telescopio + ";";                                                          
                        CONEXAO.runStatementDDL(query);                        
                        CONEXAO.disconect();
                    }
                }    
                // Deleta os dim_tempo com esses ids.
                if(!ids_tempo.isEmpty()){
                    delDimTempoByIDs(ids_tempo);
                }
            }catch(SQLException e){
                System.out.println(" "+e.getMessage());
            }finally{
                try{
                    if (resultSet != null) {
                        resultSet.close();
                    }
                    if (resultSetIdsTempo != null) {
                        resultSetIdsTempo.close();
                    }
                    if (CONEXAO.getStatement() != null) {
                        CONEXAO.getStatement().close();
                    }
                }catch(SQLException e){
                    System.out.println(" "+e.getMessage());
                }  
                if (CONEXAO != null) {
                    CONEXAO.disconect();
                }
            }               
    }
    
    //Deleta os Registros de 00:00:00 do dia seguinte, pois ele pode ser inserido novamente no final desse arquivo.
    private static void delReg00h00m00sProxDia(LocalDate dia_seguinte, int id_telescopio){
        
        int ano, mes, dia;
        ano = dia_seguinte.getYear();
        mes = dia_seguinte.getMonthValue();
        dia = dia_seguinte.getDayOfMonth();
        
        ResultSet resultSet = null;
        ResultSet resultSetIdsTempo = null;        
        List<Integer> ids_tempo = new LinkedList<Integer>();
            try{
                int id_tempo;
                String query;
                                
                CONEXAO.conect();
                query = "select id_tempo from dim_tempo WHERE dt_data_completa AT TIME ZONE 'UTC' = '"+ano+"-"+mes+"-"+dia+" 00:00:00';";
                resultSetIdsTempo = CONEXAO.query(query);              
                CONEXAO.disconect();
                while(resultSetIdsTempo.next()){ // Enquanto tiver id_tempo de cargas anteriores para esse dia  LocalDate
                    ids_tempo.add(resultSetIdsTempo.getInt(1));
                    id_tempo = resultSetIdsTempo.getInt(1);            
                    CONEXAO.conect();
                    // id_telescopio = 1 é o TUPI
                    query = "SELECT id_tempo, id_telescopio FROM FAT_SINAIS WHERE id_tempo = "+ id_tempo +" AND id_telescopio = " + id_telescopio + ";";
                    CONEXAO.query(query);
                    resultSet = CONEXAO.query(query); // Posso usar o mesmo resultSet ? Nao sei pq tem q dar close depois de usar o RS                
                    CONEXAO.disconect();
                    
                    //Se tem registros na FAT_SINAIS p/ esse id, deleta.
                    if(resultSet.next()){
                        // DELETAR REGISTROS
                        CONEXAO.conect();                        
                        query = "DELETE FROM FAT_SINAIS WHERE id_tempo =" + id_tempo + " AND id_telescopio =" + id_telescopio + ";";                                                          
                        CONEXAO.runStatementDDL(query);                        
                        CONEXAO.disconect();
                    }                            
                }
                // Deleta o dim_tempo com esses ids.
                if(!ids_tempo.isEmpty()){
                    delDimTempoByIDs(ids_tempo);
                }
            }catch(SQLException e){
                System.out.println(" "+e.getMessage());
            }finally{
                try{
                    if (resultSet != null) {
                        resultSet.close();
                    }
                    if (resultSetIdsTempo != null) {
                        resultSetIdsTempo.close();
                    }
                    if (CONEXAO.getStatement() != null) {
                        CONEXAO.getStatement().close();
                    }
                }catch(SQLException e){
                    System.out.println(" "+e.getMessage());
                }  
                if (CONEXAO != null) {
                    CONEXAO.disconect();
                }
            }
    }
    
    private static void delDimTempoByIDs(List<Integer> ids_tempo){                    
                    
            String query;           
            for(Integer id_tempo : ids_tempo){ // Enquanto tiver id_tempo de cargas anteriores para esse dia  LocalDate                                     
                // DELETAR REGISTROS dim_tempo cargas anteriores com esses IDS
                CONEXAO.conect();                        
                query = "DELETE FROM DIM_TEMPO WHERE id_tempo =" + id_tempo + ";";                                                          
                CONEXAO.runStatementDDL(query);                        
                CONEXAO.disconect();                
            }       
    }
    
    private static String getDataUltimaCarga(){
        String dt_ult_carga_formata_drive = null;
        String query;
        ResultSet resultSet = null;
        try{
            
            CONEXAO.conect();
            // Data da Ultima Carga Realizada em UTC.
            query = "SELECT data_ultima_carga AT TIME ZONE 'UTC' FROM tupi.CONTROLE_CARGA where id IN (SELECT MAX(id) FROM tupi.CONTROLE_CARGA);";
            resultSet = CONEXAO.query(query);
            CONEXAO.disconect();// já salvei no resultSet já posso fechar a conexao.
            if (resultSet.next()){  // vai para primeira linha do resultSet
                String data_completa_utc = resultSet.getString(1);
                //System.out.println("Dt ult carga bd: "+data_completa_utc);
                dt_ult_carga_formata_drive = formatToDrive(data_completa_utc); // add T e Z
                //System.out.println("Dt formato drive:  "+dt_ult_carga_formata_drive);
            }
            return dt_ult_carga_formata_drive;
        }catch(SQLException e){
            System.out.println(" "+e.getMessage());
            return null;
        }finally{
            try{
                if (resultSet != null) {
                    resultSet.close();
                }            
                if (CONEXAO.getStatement() != null) {
                    CONEXAO.getStatement().close();
                }
            }catch(SQLException e){
                System.out.println(" "+e.getMessage());
            }  
            if (CONEXAO != null) {
                CONEXAO.disconect();
            }
        }        
    }
    
    //id_telescopio = 1 fixo tupi
    private static void insereFatSinais(int id_tempo, int id_telescopio, String valor_vertical, String valor_escaler){    
        String query;
        
        CONEXAO.conect();
        query = "INSERT INTO FAT_SINAIS (id_tempo, id_telescopio, valor_vertical, valor_escaler) "
                + "VALUES ("+ id_tempo + ", "+ id_telescopio + " ," + valor_vertical + " , "+ valor_escaler + ");";                        
        CONEXAO.runStatementDDL(query);                    
        CONEXAO.disconect();        
        
        try{
            if (CONEXAO.getStatement() != null) {
                CONEXAO.getStatement().close();
            }
        }catch(SQLException e){
            System.out.println(" "+e.getMessage());
        }    
        if (CONEXAO != null) {
            CONEXAO.disconect();
        }
        
    }   
    
    
}//class
