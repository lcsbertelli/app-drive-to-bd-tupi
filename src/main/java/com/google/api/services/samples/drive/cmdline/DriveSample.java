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
import java.util.Date;





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
        Collections.singleton(DriveScopes.DRIVE_FILE)).setDataStoreFactory(dataStoreFactory)
        .build();
    // authorize
    return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
  }

  public static void main(String[] args) {
    Preconditions.checkArgument(
        !DIR_FOR_DOWNLOADS.startsWith("Enter "),
        "Please enter download directory in %s", DriveSample.class);

    try {
      //Teste verficiar conteudo  System.out.println(CAMINHO_UPLOAD);
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR);
      // authorization
      Credential credential = authorize();
      // set up the global Drive instance
      drive = new Drive.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName(
          APPLICATION_NAME).build();

      // run commands
      //####################################################
// Build a new authorized API client service.
        //Drive service = getDriveService();

        // Print the names and IDs for up to 10 files.
        FileList result = drive.files().list()
             //.setPageSize(10)
             //.setFields("nextPageToken, files(id, name)") metodo parece esta depreciado
             //.setFields("nextPageToken, files(id, title)")
             //.setKind(".JPEG"); tentar filtras o tipo dos arquivos e os campos retornados. é sempre o msm tipo #drive
             //ver se vai precisar de token e paginar, no drive ofical q tem mts paginas   
             .execute();
        //List<File> files = result.getFiles(); metodo depreciado
        
        List<File> files = result.getItems();
        System.out.println(files.size()); // so retorna 12, nao retornas os arquivos novos add. 
        
        
 //### PRINTAS PROPRIEDADES PARA TESTE ####################
        	
        if (files == null || files.size() == 0) {
            System.out.println("No files found.");
        } else {
            System.out.println("Files:");
            for (File file : files) {
//                System.out.printf("%s (%s)\n", file.getName(), file.getId());
                System.out.printf("Nome: %s ID:(%s) Data Criação: %s Data Modificacao: %s MIME Type: %s\n", file.getTitle(), file.getId(), file.getCreatedDate(), file.getModifiedDate(), file.getMimeType());
            }
        }
 //####################################################
        
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

      System.out.println("Title: " + file.getTitle());
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
  private static InputStream downloadFile(Drive service, File file) {
    if (file.getDownloadUrl() != null && file.getDownloadUrl().length() > 0) {
      try {
        HttpResponse resp =
            service.getRequestFactory().buildGetRequest(new GenericUrl(file.getDownloadUrl()))
                .execute();
        return resp.getContent();
      } catch (IOException e) {
        // An error occurred.
        e.printStackTrace();
        return null;
      }
    } else {
      // The file doesn't have any content stored on Drive.
      return null;
    }
  }

  
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
