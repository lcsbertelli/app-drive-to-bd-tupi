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
        LocalDateTime datetime_linha;        
        int qtde_seg_dia = 86400; //quantos seg tem 1 dia.
        
        double tempo;
                
        long p2 = ChronoUnit.DAYS.between(BASE_TU, date_name);
        long dif_seg = p2*qtde_seg_dia; //transforma em segundos
        //System.out.println("dif em segundos: "+dif_seg);
        
        tempo = tu - dif_seg;
        
        //Tratar registros que extrapolam o dia do arquivo - passaram das 23:59:59
        //E já são contados para o próximo dia. Só serão mantidos os reg 00:00:00 o resto é deletado, não deve ser inserido por esse arquivo de carga. 
        if(tempo==qtde_seg_dia){
            date_name.plusDays(1);
            time = LocalTime.ofSecondOfDay(0);
            datetime_linha = LocalDateTime.of(date_name, time);
            resp = datetime_linha.atZone(ZoneId.of("Z"));
            return resp;
        }else if(tempo<qtde_seg_dia){  //registros daquele mesmo dia do arquivo       
        
            time = LocalTime.ofSecondOfDay((long)(tempo));                              
            datetime_linha = LocalDateTime.of(date_name, time);       
            resp = datetime_linha.atZone(ZoneId.of("Z"));
            return resp;
        
        }else //descartar registro, maior que 00:00:00 do dia seguinte
            return null;
    }
    
    // MAIN USADO NO TESTE 
    public static void main(String[] args) {        
        
        LocalDate date_name = LocalDate.of(2014, Month.DECEMBER, 17);        
        double tu = 3.6278496e9; // Reg. que extrapolou o dia.
        
        System.out.println(""+tuToDateTime(tu, date_name));
        
               
    }
    //
}
