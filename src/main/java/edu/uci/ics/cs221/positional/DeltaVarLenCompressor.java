package edu.uci.ics.cs221.positional;

import java.util.ArrayList;
import java.util.Arrays;
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
            return new byte[0];

        // Process gaps
        int[] gaps = new int[integers.size()];
        gaps[0] = integers.get(0);
        for(int i = integers.size() - 1; i >= 1; i-- ) {
            gaps[i] = integers.get(i) - integers.get(i-1);
        }

        //Process bytes:
        List<Byte> codes = new ArrayList<>();
        for( int i :gaps){
            List<Byte> encodeParts = new ArrayList<>();
            int num = i;
            int slice = 0;
            while(num > 0){
                // Get lowest 7 bit & shift num
                slice = num & 0x7F;
                num >>= 7;

                // Put slice(7 bit) in encode array
                if (encodeParts.size() > 0)
                    slice += (1 << 7);
                encodeParts.add((byte)slice);
                slice = 0;
            }

            // Add Num 0: haven't been processed
            if (encodeParts.size() == 0)
                encodeParts.add((byte)slice);

            // Reverse list and add this number's code to bytes.
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
        if(length < 0 || start + length > bytes.length)
            return null;    //todo null or others?

        // Read from variable-length bytes
        List<Integer> result = new ArrayList<>();
        int num = 0;
        for(int i = start; i < start + length; i++){
            // Get unsigned number
            int newInt = bytes[i] & 0xFF;
            // 1 on higest bit : still need to go
            if(newInt > 127) {
                // Delete 1 & concatenate last 7 bit
                num = (num << 7) + (newInt & 0x7F);
            }
            // 0 on highest bit : a number ends
            else {
                // Directly concatenate last 7 bit & push back decoded number & reset
                num = (num << 7) + newInt;
                result.add(num);
                num = 0;
            }
        }

        // Fill the gap
        for(int i = 1 ; i < result.size(); i++ ){
            result.set(i, result.get(i) + result.get(i -1));
        }

        return result;
    }
}
