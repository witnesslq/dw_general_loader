package com.angejia.dw.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class UnclosableOutputStream extends FilterOutputStream {

    public UnclosableOutputStream(OutputStream out) {
        super(out);
    }
    
    @Override
    public void close() throws IOException {
        
    }
}
