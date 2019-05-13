package edu.uci.ics.cs221.positional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implement this compressor with Delta Encoding and Variable-Length Encoding.
 * See Project 3 description for details.
 */
public class DeltaVarLenCompressor implements Compressor {

    @Override
    public byte[] encode(List<Integer> integers) {
        if(integers == null || integers.size() == 0)
            return null;

        // Process gaps
        int[] gaps = new int[integers.size()];
        gaps[0] = integers.get(0);
        for(int i = integers.size() - 1; i > 1; i-- ) {
            gaps[i] = integers.get(i) - integers.get(i-1);
        }

        //Process bytes:
        List<Byte> codes = new ArrayList<>();
        for( int i :gaps){
            List<Byte> encodeParts = new ArrayList<>();
            int num = i;
            int part = 0;
            while(num > 0){
                // Part is going to exceed 7 bit
                if( 64 <= part && part < 128 ) { //put it in the byte
                    // Not the lowest bytes: add '1' at 1st bit
                    if (encodeParts.size() > 0)
                        part += (1 << 7);
                    // Add higher byte to the tail
                    encodeParts.add(0, (byte)part);
                    part = 0;
                }

                // Increment this part's value
                part = (part << 1) + num % 2;
                num >>= 1; //shift right
            }
            // Add the last one part
            part += (1 << 7);
            encodeParts.add((byte)part);

            Collections.reverse(encodeParts);
            codes.addAll(encodeParts);
        }

        byte[] result = new byte[codes.size()];
        for(int i = 0 ; i < codes.size(); i++){
            result[i] = codes.get(i);
        }

        return result;
    }

    @Override
    public List<Integer> decode(byte[] bytes, int start, int length) {
        if(start + length < bytes.length)
            return null;    //todo null or others?

        // Read from variable-length bytes
        List<Integer> result = new ArrayList<>();
        int num = 0;
        for(int i = start; i < start + length; i++){
            int newInt = bytes[i] & 0xFF;  // Get unsigned number
            if(newInt > 127)  // has 1 on higest bit todo get bit from byte directly
                num = (num << 8) + (newInt & 0x7F); // delete 1
            else {           // no 1 on highest bit
                num += newInt;
                result.add(num);
                num = 0;
            }
        }

        // Fill the gap
        for(int i = 1 ; i < result.size(); i++ ){
            result.set(i, result.get(i) + result.get(i -1));
        }

        return result;


        //throw new UnsupportedOperationException();
    }
}
