package com.google.api.services.samples.drive.cmdline;

import java.time.*;
import java.time.temporal.ChronoUnit;

/**
 *
 * @author lucas
 */
public abstract class ConvertTUtoTime {
    
    private static final LocalDate BASE_TU = LocalDate.of(1900, Month.JANUARY, 1);
    
    public static ZonedDateTime tuToDateTime(double tu, LocalDate date_name){
        
        ZonedDateTime resp;
        LocalTime time;        
        
        double tempo;
                
        long p2 = ChronoUnit.DAYS.between(BASE_TU, date_name);
        long dif_seg = p2*86400; //transforma em segundos
        //System.out.println("dif em segundos: "+dif_seg);
        
        tempo = tu - dif_seg;        
        
        //System.out.println("time em segundos: "+tempo);
        
        time = LocalTime.ofSecondOfDay((long)(tempo));     
        
            
        //System.out.println(""+time); 
        
        LocalDateTime datetime_linha = LocalDateTime.of(date_name, time);       
        //System.out.println("datetime: "+datetime_linha);
        resp = datetime_linha.atZone(ZoneId.of("Z"));
        return resp;
    }
    
    /* MAIN USADO NO TESTE - PODE EXCLUIR DEPOIS
    public static void main(String[] args) {
        LocalTime time;
        LocalDate BASE_TU = LocalDate.of(1900, Month.JANUARY, 1);
        LocalDate date_name = LocalDate.of(2014, Month.SEPTEMBER, 5);
        //double tu = 3.618864002e9; // 00:00:02
        double tu = 3.618950398e9; // 23:59:58
        double long_tu1 = 3.618950398 * Math.pow(10, 9);
        long tu_long = (long)long_tu1;
        double tempo;

        Period p = Period.between(BASE_TU, date_name);        
        long p2 = ChronoUnit.DAYS.between(BASE_TU, date_name);
        long dif_seg = p2*86400;
        System.out.println("dif em segundos: "+dif_seg);
        
        tempo = tu - dif_seg;
        
        
        System.out.println("time em segundos: "+tempo);
        time = LocalTime.ofSecondOfDay((long)tempo);        
        System.out.println(""+time); 
        
        LocalDateTime datetime_linha = LocalDateTime.of(date_name, time);       
        System.out.println("datetime: "+datetime_linha);        
    }
    */
}
