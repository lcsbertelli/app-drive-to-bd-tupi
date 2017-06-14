package testes;
import excecoes.TUmaiorQMeiaNoiteException;
import static com.google.api.services.samples.drive.cmdline.ConvertTUtoTime.tuToDateTime;
import java.time.LocalDate;
import java.time.Month;
/**
 *
 * @author lucas
 */
public class TestTUmaiorQMeiaNoiteEx {
        public static void main(String[] args) {               
            
        LocalDate date_name = LocalDate.of(2014, Month.DECEMBER, 17); // do q passa da 00h
        //LocalDate date_name = LocalDate.of(2014, Month.SEPTEMBER, 14); // do primeiro reg
        //double tu = 3.6278496e9; // Reg. que extrapolou o dia, mas ainda é 00:00:00
        //double tu = 3.619641602e9; // reg. primeiro sempre inserido nos teste com dt_ult_carga = 2017-04-28 16:17:56
        double tu = 3.627849602e9 ; //gerar excecao 
        try {
            System.out.println(""+tuToDateTime(tu, date_name));
        } catch (TUmaiorQMeiaNoiteException ex) {
            System.out.println(ex.getMessage());
            System.out.println("Obrigatorio excluir esse registro, trate a excecao assim");
        }              
    }
}
