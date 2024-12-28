package org.george0st.processor;

import com.opencsv.exceptions.CsvValidationException;
import org.george0st.CqlAccess;
import org.george0st.helper.Setup;
import javax.management.InvalidAttributeValueException;
import java.io.IOException;


abstract class CqlProcessor extends CqlAccess {

    public CqlProcessor(Setup setup) {
        super(setup);
    }

    public CqlProcessor(CqlAccess access) {
        super(access);
    }

    protected String prepareHeaders(String[] headers){
        return String.join(", ",headers);
    }

    protected String prepareItems(String[] headers){
        StringBuilder prepareItems= new StringBuilder();

        for (int i=0;i<headers.length;i++)
            prepareItems.append("?, ");
        return prepareItems.deleteCharAt(prepareItems.length() - 2).toString();
    }

    protected String whereItems(String[] whereItems){
        StringBuilder prepareItems= new StringBuilder();

        for (int i=0;i<whereItems.length;i++){
            if (prepareItems.length()>0)
                prepareItems.append(" AND ");
            prepareItems.append(String.format("%s = ?", whereItems[i]));
        }
        return prepareItems.toString();
    }

    abstract void execute(String fileName) throws CsvValidationException, IOException, InvalidAttributeValueException;
}
