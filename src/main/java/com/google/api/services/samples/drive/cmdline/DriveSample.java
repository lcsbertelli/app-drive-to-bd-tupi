/*
 * Copyright (c) 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.api.services.samples.drive.cmdline;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.media.MediaHttpDownloader;
import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.Preconditions;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.drive.Drive;
import com.google.api.services.drive.DriveScopes;
//import com.google.api.services.drive.model.File;
import com.google.api.services.drive.model.*;
import com.google.api.client.http.HttpResponse;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import java.util.Date;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;



/**
 * A sample application that runs multiple requests against the Drive API. The requests this sample
 * makes are:
 * <ul>
 * <li>Does a resumable media upload</li>
 * <li>Updates the uploaded file by renaming it</li>
 * <li>Does a resumable media download</li>
 * <li>Does a direct media upload</li>
 * <li>Does a direct media download</li>
 * </ul>
 *
 * @author rmistry@google.com (Ravi Mistry)
 */
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

//####### AQUI IRIA NO BANCO E PEGA A DATA DE LÁ
      String data_completa = "2017-04-03 12:00:00"; 
      String[] data_splitada = data_completa.split(" ");
      String data = data_splitada[0];
      String time = data_splitada[1];
      System.out.println("data:  "+data);
      System.out.println("time:  "+time);
      String data_formato_drive = data+"T"+time;
      System.out.println("Dt formato drive:  "+data_formato_drive);
      
      //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");  
      //SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
      //Date data_ultima_carga = new Date();
      //String stringDate = "2017-04-03 12:00:00";
      //String stringDate = "2014-03-31T14:11:29+02:00";
      //data_ultima_carga = sdf.format(stringDate); // se for Date já Format, se não .parse
      //data_ultima_carga = sdf.parse(stringDate); // se for Date já Format, se não .parse     
      //System.out.println("Data formata: "+data_ultima_carga); 
      //System.out.println("Data formatada: "+sdf.format(data_ultima_carga));
      
      
      
      
//####### fim AQUI IRIA NO BANCO E PEGA A DATA DE LÁ   

      String pageToken = null;
      do {          
        FileList result = drive.files().list()
                .setCorpora("user") // Abrange o MyDrive e Shared With Me                
                //.setQ("(mimeType='application/octet-stream') and ('0Bx9yd3l3M5AaZzlZNXJjRE9Wemc' in parents) and (createdTime > '2017-04-03T12:00:00')") // (Somente os com tipo/MIME .dat) and (ID da Pasta Novo monitor banco de dados in Coleção de Pastas do Arquivo(Parents)
                //.setQ("(mimeType='application/octet-stream') and ('0AMfZkpEPzZbRUk9PVA' in parents) and (createdTime > '2017-06-02T18:02:11.412Z')") // TESTE: ID Pasta MyDrive Raiz                
                .setQ("(mimeType='application/octet-stream') and ('0AMfZkpEPzZbRUk9PVA' in parents) and (createdTime > '" + data_formato_drive + "')") // TESTE: ID Pasta MyDrive Raiz                 
                
                .setFields("nextPageToken, files(id, name, parents, createdTime, modifiedTime, mimeType)") //Nao existe mas Title na v3, agora é name.
                .setPageToken(pageToken)                
                .execute();    
        
        List<File> files = result.getFiles();      
        //List<File> files = result.getItems(); //metodo defasado, usado na API v2.
        System.out.println(files.size()); // so retorna 18, mas tem 25 itens lá. nao retornas os arquivos novos add via browser. 
        
        
    //### PRINTAS PROPRIEDADES PARA TESTE ####################
    
        if (files == null || files.size() == 0) {
            System.out.println("No files found.");
        } else {
            System.out.println("Files:");
            for (File file : files) {
                System.out.printf("Name: %s ID: (%s) Parents: (%s) DataTime Criação: %s DataTime Modificacao: %s MIME Type: %s\n", 
                        file.getName(), file.getId(), file.getParents(), file.getCreatedTime(), file.getModifiedTime(), file.getMimeType());
                //System.out.printf("Title: %s ID:(%s) Data Criação: %s Data Modificacao: %s MIME Type: %s\n", file.getTitle(), file.getId(), file.getCreatedDate(), file.getModifiedDate(), file.getMimeType());
            
            }
        }
    //####################################################    
        
        pageToken = result.getNextPageToken();
      } while (pageToken != null); 
      
 
        
 // ####### Baixando somentes os novos #####################################################
     
//        //Supondo que a Cns trouxe do Banco a Data da Ultima Carga a setada abaixo    
//        Date data_ultima_carga = "2017-06-01T18:08:00.220Z";
//        
//        // ### 1º MIMETYPE = .DAT Se for Buscar Novos Arquivos ou Atualizados após a última carga dada no SGBD.
//        
//        if (files == null || files.size() == 0) {
//            System.out.println("No files found.");
//        } else {
//            System.out.println("Files Atualizados:");
//            for (File file : files) {
//                if(file.getCreatedDate() > data_ultima_carga)    
//                    System.out.printf("Nome: %s ID:(%s) Data Criação: %s Data Modificacao: %s MIME Type: %s\n", file.getTitle(), file.getId(), file.getCreatedDate(), file.getModifiedDate(), file.getMimeType());
//            }
//        }
// ####### fim Baixando somentes os novos #####################################################   
        
        
      
      
      
      //####################################################
      
      View.header1("Success!");
      return;
    } catch (IOException e) {
      System.err.println(e.getMessage());
    } catch (Throwable t) {
      t.printStackTrace();
    }
    System.exit(1);
  }

  /**
   * Print a file's metadata.
   *
   * @param service Drive API service instance.
   * @param fileId ID of the file to print metadata for.
   */
  private static void printFile(Drive service, String fileId) {

    try {
      File file = service.files().get(fileId).execute();

      //System.out.println("Title: " + file.getTitle());
      System.out.println("Description: " + file.getDescription());
      System.out.println("MIME type: " + file.getMimeType());
    } catch (IOException e) {
      System.out.println("An error occurred: " + e);
    }
  }

  /**
   * Download a file's content.
   *
   * @param service Drive API service instance.
   * @param file Drive File instance.
   * @return InputStream containing the file's content if successful,
   *         {@code null} otherwise.
   */
  // METODOS DEFASADOS PARA A API V3. REVER
//  private static InputStream downloadFile(Drive service, File file) {
//    if (file.getDownloadUrl() != null && file.getDownloadUrl().length() > 0) {
//      try {
//        HttpResponse resp =
//            service.getRequestFactory().buildGetRequest(new GenericUrl(file.getDownloadUrl()))
//                .execute();
//        return resp.getContent();
//      } catch (IOException e) {
//        // An error occurred.
//        e.printStackTrace();
//        return null;
//      }
//    } else {
//      // The file doesn't have any content stored on Drive.
//      return null;
//    }
//  }

  
//// ############## ACHO Q N  VAI USAR
//  /** Downloads a file using either resumable or direct media download. */
//  private static void downloadFile(boolean useDirectDownload, File uploadedFile)
//      throws IOException {
//    // create parent directory (if necessary)
//    java.io.File parentDir = new java.io.File(DIR_FOR_DOWNLOADS);
//    if (!parentDir.exists() && !parentDir.mkdirs()) {
//      throw new IOException("Unable to create parent directory");
//    }
//    OutputStream out = new FileOutputStream(new java.io.File(parentDir, uploadedFile.getTitle()));
//    //OutputStream out = new FileOutputStream(new java.io.File(parentDir, "teste123"));
//
//    MediaHttpDownloader downloader =
//        new MediaHttpDownloader(httpTransport, drive.getRequestFactory().getInitializer());
//    downloader.setDirectDownloadEnabled(useDirectDownload);
//    downloader.setProgressListener(new FileDownloadProgressListener());
//    downloader.download(new GenericUrl(uploadedFile.getDownloadUrl()), out);
//    
//  }
//// ### ACHO Q VOU DELETAR ESSE METODO ACIMA !


}
